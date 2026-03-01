"""
UA Innovate 2026 - Southern Company Data Pipeline
Processes Excel workbook into device lifecycle CSVs for dashboard consumption.

Usage:
  py ua-innovate-backend.py [excel_path] [output_dir]
  Default: UAInnovateDataset-SoCo.xlsx, data/outputs
"""

import argparse
import os
import numpy as np
import pandas as pd

def run_pipeline(excel_path: str, output_dir: str) -> None:
    """Process Excel and write CSV outputs."""
    xls = pd.ExcelFile(excel_path)
    sheet_names = xls.sheet_names

    solid = pd.read_excel(xls, sheet_name='SOLID') if 'SOLID' in sheet_names else pd.DataFrame()
    solid_loc = pd.read_excel(xls, sheet_name='SOLID-Loc') if 'SOLID-Loc' in sheet_names else pd.DataFrame()
    na = pd.read_excel(xls, sheet_name='NA') if 'NA' in sheet_names else pd.DataFrame()
    decom = pd.read_excel(xls, sheet_name='Decom') if 'Decom' in sheet_names else pd.DataFrame()
    modeldata = pd.read_excel(xls, sheet_name='ModelData') if 'ModelData' in sheet_names else pd.DataFrame()
    prime_ap = pd.read_excel(xls, sheet_name='PrimeAP') if 'PrimeAP' in sheet_names else pd.DataFrame()
    prime_wlc = pd.read_excel(xls, sheet_name='PrimeWLC') if 'PrimeWLC' in sheet_names else pd.DataFrame()
    catctr = pd.read_excel(xls, sheet_name='CatCtr') if 'CatCtr' in sheet_names else pd.DataFrame()

    model_cols = ['Model', 'EoS', 'EoL', 'In Scope?']
    for c in ['Repl Device', 'ReplaceNow', 'PrepareToReplace', 'Material Cost', 'Labor Cost', 'Device Cost']:
        if c in modeldata.columns:
            model_cols.append(c)
    modeldata_sub = modeldata[[c for c in model_cols if c in modeldata.columns]]

    sites = pd.DataFrame()
    if not solid.empty and not solid_loc.empty and 'Site Code' in solid.columns and 'Site Code' in solid_loc.columns:
        loc_cols = [c for c in ['Site Code', 'Latitude', 'Longitude', 'PhysicalAddressCounty', 'Call Group', 'Owner'] if c in solid_loc.columns]
        sites = solid.merge(solid_loc[loc_cols], on='Site Code', how='left')

    if na.empty:
        devices = pd.DataFrame()
    else:
        active_na = na[na['Device Status'].astype(str).str.lower() == 'active'].copy()
        type_map = {'Switch': 'Switch', 'L3Switch': 'Switch', 'Router': 'Router', 'Voice Gateway': 'Voice Gateway'}
        active_na['Device Type Standard'] = active_na['Device Type'].map(type_map).fillna(active_na['Device Type'])

        na_hosts_to_remove = set()
        if 'hostname' in catctr.columns:
            na_hosts_to_remove.update(catctr['hostname'].dropna().astype(str).str.strip())
        if 'name' in prime_ap.columns:
            na_hosts_to_remove.update(prime_ap['name'].dropna().astype(str).str.strip())
        if 'deviceName' in prime_wlc.columns:
            na_hosts_to_remove.update(prime_wlc['deviceName'].dropna().astype(str).str.strip())
        active_na = active_na[~active_na['Host Name'].astype(str).str.strip().isin(na_hosts_to_remove)]

        to_concat = [active_na]
        if not catctr.empty and 'hostname' in catctr.columns:
            to_concat.append(catctr.rename(columns={'hostname': 'Host Name'}))
        if not prime_ap.empty and 'name' in prime_ap.columns:
            to_concat.append(prime_ap.rename(columns={'name': 'Host Name'}))
        if not prime_wlc.empty and 'deviceName' in prime_wlc.columns:
            to_concat.append(prime_wlc.rename(columns={'deviceName': 'Host Name'}))
        devices = pd.concat(to_concat, ignore_index=True, sort=False)

    if devices.empty:
        os.makedirs(output_dir, exist_ok=True)
        empty_df = pd.DataFrame(columns=['Host Name', 'Device Model', 'EoS', 'EoL', 'State', 'Site Code Extracted', 'Exception_Flag', 'Exception_Reason', 'EoL_Date', 'Days_to_EoL', 'EoS_Date', 'Days_to_EoS'])
        for name in ["core_device_table.csv", "devices_approaching_eol.csv", "devices_past_eol.csv", "devices_approaching_eos.csv", "devices_past_eos.csv", "devices_exceptions.csv", "devices_unknown_lifecycle.csv"]:
            empty_df.to_csv(os.path.join(output_dir, name), index=False)
        print(f"No devices. Empty CSVs written to {output_dir}")
        return

    devices['Site Code Extracted'] = devices['Host Name'].astype(str).str[2:5]
    devices['State'] = devices['Host Name'].astype(str).str[:2]

    devices = devices.merge(modeldata_sub, left_on='Device Model', right_on='Model', how='left', suffixes=('', '_m'))
    devices = devices.drop(columns=['Model_m'], errors='ignore')

    if not sites.empty:
        sites_renamed = sites.rename(columns={'Site Code': 'Site Code Extracted'})
        geo_cols = [c for c in ['Site Code Extracted', 'State', 'Latitude', 'Longitude', 'PhysicalAddressCounty', 'Call Group', 'Owner'] if c in sites_renamed.columns]
        devices = devices.merge(sites_renamed[geo_cols], on='Site Code Extracted', how='left', suffixes=('', '_site'))
        if 'State_site' in devices.columns:
            devices['State'] = devices['State_site'].fillna(devices['State'])
            devices = devices.drop(columns=['State_site'], errors='ignore')
        for k, v in [('Latitude', 'latitude'), ('Longitude', 'longitude'), ('PhysicalAddressCounty', 'county'), ('Call Group', 'affiliate')]:
            if k in devices.columns:
                devices = devices.rename(columns={k: v})
        for col in ['latitude', 'longitude']:
            if col not in devices.columns:
                devices[col] = np.nan
        for col in ['county', 'affiliate', 'State']:
            if col in devices.columns:
                devices[col] = devices[col].fillna('Unknown')
            else:
                devices[col] = 'Unknown'
    else:
        devices['latitude'] = np.nan
        devices['longitude'] = np.nan
        devices['county'] = 'Unknown'
        devices['affiliate'] = 'Unknown'

    devices['Exception_Flag'] = False
    devices['Exception_Reason'] = None
    if not decom.empty and 'Site Cd' in decom.columns:
        decom_sites = set(decom['Site Cd'].dropna().astype(str).str.strip())
        mask = devices['Site Code Extracted'].astype(str).str.strip().isin(decom_sites)
        devices.loc[mask, ['Exception_Flag', 'Exception_Reason']] = [True, 'Decommissioned site']

    today = pd.Timestamp.today()
    devices['EoL_Date'] = pd.to_datetime(devices['EoL'], errors='coerce')
    devices['Days_to_EoL'] = (devices['EoL_Date'] - today).dt.days
    devices['EoS_Date'] = pd.to_datetime(devices['EoS'], errors='coerce')
    devices['Days_to_EoS'] = (devices['EoS_Date'] - today).dt.days

    approaching_eol = devices[(devices['Days_to_EoL'].notna()) & (devices['Days_to_EoL'] >= 0) & (devices['Days_to_EoL'] <= 365)]
    past_eol = devices[(devices['Days_to_EoL'].notna()) & (devices['Days_to_EoL'] < 0)]
    approaching_eos = devices[(devices['Days_to_EoS'].notna()) & (devices['Days_to_EoS'] >= 0) & (devices['Days_to_EoS'] <= 365)]
    past_eos = devices[(devices['Days_to_EoS'].notna()) & (devices['Days_to_EoS'] < 0)]
    exceptions_df = devices[devices['Exception_Flag'] == True]
    unknown_lifecycle = devices[devices['EoL'].isna()]

    os.makedirs(output_dir, exist_ok=True)
    devices.to_csv(os.path.join(output_dir, "core_device_table.csv"), index=False)
    approaching_eol.to_csv(os.path.join(output_dir, "devices_approaching_eol.csv"), index=False)
    past_eol.to_csv(os.path.join(output_dir, "devices_past_eol.csv"), index=False)
    approaching_eos.to_csv(os.path.join(output_dir, "devices_approaching_eos.csv"), index=False)
    past_eos.to_csv(os.path.join(output_dir, "devices_past_eos.csv"), index=False)
    exceptions_df.to_csv(os.path.join(output_dir, "devices_exceptions.csv"), index=False)
    unknown_lifecycle.to_csv(os.path.join(output_dir, "devices_unknown_lifecycle.csv"), index=False)

    print(f"Processed {len(devices):,} devices. Output written to {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("excel_path", nargs="?", default="UAInnovateDataset-SoCo.xlsx")
    parser.add_argument("output_dir", nargs="?", default=os.path.join("data", "outputs"))
    args = parser.parse_args()

    _base = os.path.dirname(os.path.abspath(__file__))
    excel_path = args.excel_path if os.path.isabs(args.excel_path) else os.path.normpath(os.path.join(_base, args.excel_path))
    output_dir = args.output_dir if os.path.isabs(args.output_dir) else os.path.normpath(os.path.join(_base, args.output_dir))

    if not os.path.exists(excel_path):
        print(f"Error: Excel not found: {excel_path}")
        exit(1)

    run_pipeline(excel_path, output_dir)
