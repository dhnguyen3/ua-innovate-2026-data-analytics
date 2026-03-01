"""
UA Innovate 2026 - Southern Company Data Pipeline
Processes Excel workbook into device lifecycle CSVs for dashboard consumption.

Data Sources (per prompt):
- NA (Network Automation): switches, routers, voice gateways
- CatCtr (Catalyst Center) / PrimeAP / PrimeWLC: source of truth for APs and WLCs (override NA duplicates)
- SOLID + SOLID-Loc: site info, geo (lat/long), affiliate (Call Group), county
- ModelData: EoS, EoL, replacement device, costs
- Decom: decommissioned sites (exceptions)

Assumptions:
- Only active/reachable devices
- NA Device Type → Switch, Router, or Voice Gateway
- Host name: first 2 chars = State, chars 3-5 = site code

Usage:
  py ua-innovate-backend.py [excel_path] [output_dir]
  Default: UAInnovateDataset-SoCo.xlsx, data/outputs
"""

import argparse
import os
import pandas as pd
from datetime import datetime

def run_pipeline(excel_path: str, output_dir: str) -> None:
    """Process Excel and write CSV outputs."""
    xls = pd.ExcelFile(excel_path)

    # Load sheets (handle missing sheets gracefully)
    sheet_names = xls.sheet_names
    solid = pd.read_excel(xls, sheet_name='SOLID') if 'SOLID' in sheet_names else pd.DataFrame()
    solid_loc = pd.read_excel(xls, sheet_name='SOLID-Loc') if 'SOLID-Loc' in sheet_names else pd.DataFrame()
    na = pd.read_excel(xls, sheet_name='NA') if 'NA' in sheet_names else pd.DataFrame()
    decom = pd.read_excel(xls, sheet_name='Decom') if 'Decom' in sheet_names else pd.DataFrame()
    modeldata = pd.read_excel(xls, sheet_name='ModelData') if 'ModelData' in sheet_names else pd.DataFrame()
    prime_ap = pd.read_excel(xls, sheet_name='PrimeAP') if 'PrimeAP' in sheet_names else pd.DataFrame()
    prime_wlc = pd.read_excel(xls, sheet_name='PrimeWLC') if 'PrimeWLC' in sheet_names else pd.DataFrame()
    catctr = pd.read_excel(xls, sheet_name='CatCtr') if 'CatCtr' in sheet_names else pd.DataFrame()

    # ModelData columns for costs and replacement
    model_cols = ['Model', 'EoS', 'EoL', 'In Scope?']
    cost_cols = ['Repl Device', 'ReplaceNow', 'PrepareToReplace', 'Material Cost', 'Labor Cost', 'Device Cost']
    for c in cost_cols:
        if c in modeldata.columns:
            model_cols.append(c)
    modeldata_sub = modeldata[[c for c in model_cols if c in modeldata.columns]]

    # Ensure NA has required columns
    if na.empty:
        devices = pd.DataFrame()
    else:
        # 3. Filter NA → only active devices
        active_na = na[na['Device Status'].astype(str).str.lower() == 'active'].copy()

        # 4. Standardize Device Type (Switch, Router, Voice Gateway)
        type_map = {
            'Switch': 'Switch', 'L3Switch': 'Switch',
            'Router': 'Router',
            'Voice Gateway': 'Voice Gateway',
        }
        active_na['Device Type Standard'] = active_na['Device Type'].map(type_map).fillna(active_na['Device Type'])

        # 5. CatCtr/Prime override NA for duplicates
        na_hosts_to_remove = set()
        if 'hostname' in catctr.columns:
            na_hosts_to_remove.update(catctr['hostname'].dropna().astype(str).str.strip())
        if 'name' in prime_ap.columns:
            na_hosts_to_remove.update(prime_ap['name'].dropna().astype(str).str.strip())
        if 'deviceName' in prime_wlc.columns:
            na_hosts_to_remove.update(prime_wlc['deviceName'].dropna().astype(str).str.strip())
        active_na = active_na[~active_na['Host Name'].astype(str).str.strip().isin(na_hosts_to_remove)]

        # Combine all sources (CatCtr/Prime override NA for APs/WLCs)
        to_concat = [active_na]
        if not catctr.empty and 'hostname' in catctr.columns:
            to_concat.append(catctr.rename(columns={'hostname': 'Host Name'}))
        if not prime_ap.empty and 'name' in prime_ap.columns:
            to_concat.append(prime_ap.rename(columns={'name': 'Host Name'}))
        if not prime_wlc.empty and 'deviceName' in prime_wlc.columns:
            to_concat.append(prime_wlc.rename(columns={'deviceName': 'Host Name'}))
        devices = pd.concat(to_concat, ignore_index=True, sort=False)

    if devices.empty:
        # Write empty CSVs
        os.makedirs(output_dir, exist_ok=True)
        base_cols = ['Host Name', 'Device Model', 'EoS', 'EoL', 'State', 'Site Code Extracted', 'Exception_Flag', 'Exception_Reason', 'EoL_Date', 'Days_to_EoL']
        empty_df = pd.DataFrame(columns=base_cols)
        empty_df.to_csv(os.path.join(output_dir, "core_device_table.csv"), index=False)
        for name in ["devices_approaching_eol.csv", "devices_past_eol.csv", "devices_exceptions.csv", "devices_unknown_lifecycle.csv"]:
            empty_df.to_csv(os.path.join(output_dir, name), index=False)
        return

    # 6. Extract State and Site Code from host name
    devices['State'] = devices['Host Name'].astype(str).str[:2]
    devices['Site Code Extracted'] = devices['Host Name'].astype(str).str[2:5]

    # 7. Merge with ModelData (lifecycle + costs + replacement)
    devices = devices.merge(
        modeldata_sub,
        left_on='Device Model',
        right_on='Model',
        how='left',
        suffixes=('', '_model')
    )
    if 'Model_model' in devices.columns:
        devices = devices.drop(columns=['Model_model'], errors='ignore')

    # 8. Exceptions (Decom sites)
    devices['Exception_Flag'] = False
    devices['Exception_Reason'] = None
    if not decom.empty and 'Site Cd' in decom.columns:
        decom_sites = set(decom['Site Cd'].dropna().astype(str).str.strip())
        mask = devices['Site Code Extracted'].astype(str).str.strip().isin(decom_sites)
        devices.loc[mask, 'Exception_Flag'] = True
        devices.loc[mask, 'Exception_Reason'] = 'Decommissioned site'

    # 9. Days to EoL
    today = pd.Timestamp.today()
    devices['EoL_Date'] = pd.to_datetime(devices['EoL'], errors='coerce')
    devices['Days_to_EoL'] = (devices['EoL_Date'] - today).dt.days

    # 10. Lifecycle subsets
    approaching_eol = devices[(devices['Days_to_EoL'].notna()) & (devices['Days_to_EoL'] >= 0) & (devices['Days_to_EoL'] <= 365)].copy()
    past_eol = devices[(devices['Days_to_EoL'].notna()) & (devices['Days_to_EoL'] < 0)].copy()
    exceptions_df = devices[devices['Exception_Flag'] == True].copy()
    unknown_lifecycle = devices[devices['EoL'].isna()].copy()

    # 11. Save
    os.makedirs(output_dir, exist_ok=True)
    devices.to_csv(os.path.join(output_dir, "core_device_table.csv"), index=False)
    approaching_eol.to_csv(os.path.join(output_dir, "devices_approaching_eol.csv"), index=False)
    past_eol.to_csv(os.path.join(output_dir, "devices_past_eol.csv"), index=False)
    exceptions_df.to_csv(os.path.join(output_dir, "devices_exceptions.csv"), index=False)
    unknown_lifecycle.to_csv(os.path.join(output_dir, "devices_unknown_lifecycle.csv"), index=False)

    print(f"Processed {len(devices):,} devices. Output written to {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UA Innovate SoCo - Process Excel to CSV")
    parser.add_argument("excel_path", nargs="?", default="UAInnovateDataset-SoCo.xlsx",
                        help="Path to UAInnovateDataset Excel file")
    parser.add_argument("output_dir", nargs="?", default=os.path.join("data", "outputs"),
                        help="Output directory for CSVs")
    args = parser.parse_args()

    if not os.path.exists(args.excel_path):
        print(f"Error: Excel file not found: {args.excel_path}")
        exit(1)

    run_pipeline(args.excel_path, args.output_dir)
