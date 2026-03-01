import pandas as pd
from datetime import datetime

# 1. Load Excel workbook
file_name = "UAInnovateDataset-SoCo.xlsx"
xls = pd.ExcelFile(file_name)

# Load sheets
solid = pd.read_excel(xls, sheet_name='SOLID')
solid_loc = pd.read_excel(xls, sheet_name='SOLID-Loc')
na = pd.read_excel(xls, sheet_name='NA')
decom = pd.read_excel(xls, sheet_name='Decom')
modeldata = pd.read_excel(xls, sheet_name='ModelData')
prime_ap = pd.read_excel(xls, sheet_name='PrimeAP')
prime_wlc = pd.read_excel(xls, sheet_name='PrimeWLC')
catctr = pd.read_excel(xls, sheet_name='CatCtr')

# 2. Merge SOLID + SOLID-Loc → site info
sites = solid.merge(
    solid_loc[['Site Code', 'Latitude', 'Longitude', 'PhysicalAddressCounty', 'Call Group', 'Owner']],
    on='Site Code',
    how='left'
)

# 3. Filter NA → only active/reachable devices
active_na = na[na['Device Status'].str.lower() == 'active'].copy()

# 4. Standardize Device Type
type_map = {
    'Switch': 'Switch',
    'Router': 'Router',
    'Voice Gateway': 'Voice Gateway',
}
active_na['Device Type Standard'] = active_na['Device Type'].map(type_map).fillna(active_na['Device Type'])

# 5. Override NA with CatCtr / PrimeAP / PrimeWLC for duplicates
na_hosts_to_remove = set(catctr['hostname']).union(set(prime_ap['name'].fillna(''))).union(set(prime_wlc['deviceName'].fillna('')))
active_na = active_na[~active_na['Host Name'].isin(na_hosts_to_remove)]

# 6. Combine all sources
devices = pd.concat([
    active_na,
    catctr.rename(columns={'hostname':'Host Name'}),
    prime_ap.rename(columns={'name':'Host Name'}),
    prime_wlc.rename(columns={'deviceName':'Host Name'})
], ignore_index=True, sort=False)

# 7. Extract Site Code from host name (for mapping)
devices['Site Code Extracted'] = devices['Host Name'].str[2:5]

# 8. Merge with ModelData for lifecycle info
devices = devices.merge(
    modeldata[['Model','EoS','EoL','In Scope?']],
    left_on='Device Model',
    right_on='Model',
    how='left'
)

# 9. Merge geographic/site info using extracted Site Code
devices = devices.merge(
    sites.rename(columns={'Site Code':'Site Code Extracted'}),
    on='Site Code Extracted',
    how='left'
)

# 10. Fill missing columns with 'Unknown' for consistency
for col in ['State','Owner','PhysicalAddressCounty','Latitude','Longitude']:
    if col not in devices.columns:
        devices[col] = 'Unknown'
    devices[col] = devices[col].fillna('Unknown')

# 11. Flag exceptions using Decom
devices['Exception_Flag'] = False
devices['Exception_Reason'] = None
devices.loc[devices['Site Code Extracted'].isin(decom['Site Cd']), ['Exception_Flag','Exception_Reason']] = [True, 'Decommissioned site']

# 12. Calculate Days to EoL
today = pd.Timestamp.today()
devices['EoL_Date'] = pd.to_datetime(devices['EoL'], errors='coerce')
devices['Days_to_EoL'] = (devices['EoL_Date'] - today).dt.days

# 13. Flag concern devices
approaching_eol = devices[(devices['Days_to_EoL'] >= 0) & (devices['Days_to_EoL'] <= 365)]
past_eol = devices[devices['Days_to_EoL'] < 0]
exceptions = devices[devices['Exception_Flag'] == True]
unknown_lifecycle = devices[devices['EoL'].isna()]

# 14. Columns to include in print/output
geo_columns = ['State','Owner','PhysicalAddressCounty','Latitude','Longitude']

# 15. Print concern rows with geographic info
print("\n=== Devices Approaching EoL (within 1 year) ===")
print(approaching_eol[['Host Name','Device Model','EoS','EoL','Days_to_EoL'] + geo_columns])

print("\n=== Devices Past EoL ===")
print(past_eol[['Host Name','Device Model','EoS','EoL','Days_to_EoL'] + geo_columns])

print("\n=== Devices Flagged as Exceptions ===")
print(exceptions[['Host Name','Device Model','Exception_Reason'] + geo_columns])

print("\n=== Devices with Unknown Lifecycle ===")
print(unknown_lifecycle[['Host Name','Device Model','EoS','EoL'] + geo_columns])

# 16. Save full dataset and concern subsets
devices.to_csv("core_device_table.csv", index=False)
approaching_eol.to_csv("devices_approaching_eol.csv", index=False)
past_eol.to_csv("devices_past_eol.csv", index=False)
exceptions.to_csv("devices_exceptions.csv", index=False)
unknown_lifecycle.to_csv("devices_unknown_lifecycle.csv", index=False)