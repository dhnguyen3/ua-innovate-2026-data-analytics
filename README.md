# Southern Company ? UA Innovate 2026 Lifecycle Dashboard

Evaluate network equipment costs and identify opportunities for improvement/optimization. Provides leadership with clear insights to support decision-making.

## Quick Start

```bash
py -m pip install -r requirements.txt
py -m streamlit run app.py
```

## Data Loading Process

### Option A: Load from Excel (recommended)

1. Place your Excel file in `data/raw/` (e.g. `data/raw/UAInnovateDataset-SoCo.xlsx`) or any known location.
2. In the dashboard sidebar, set **Excel Path** to the file (e.g. `data/raw/UAInnovateDataset-SoCo.xlsx` or full path like `C:\Users\...\UAInnovateDataset-SoCo.xlsx`). If the path is incomplete (e.g. missing `.xlsx`), the app will try appending it automatically.
3. Click **Run Pipeline from Excel** to process raw data into CSVs.
4. Click **Reload Data**.

### Option B: Use pre-processed CSVs

1. Run the backend manually:
   ```bash
   py ua-innovate-backend.py "path/to/UAInnovateDataset-SoCo.xlsx" data/outputs
   ```
2. Set **CSV Output Directory** in the sidebar (default: `data/outputs`).
3. Click **Reload Data**.

## Input Data (Excel sheets)

| Sheet | Description |
|-------|-------------|
| NA | Network Automation: switches, routers, voice gateways |
| CatCtr | Catalyst Center (source of truth for APs/WLCs) |
| PrimeAP, PrimeWLC | Cisco Prime device data |
| SOLID, SOLID-Loc | Site info, lat/long, affiliate (Call Group), county |
| ModelData | EoS, EoL, replacement device, costs |
| Decom | Decommissioned sites (exceptions) |

## Dashboard Features

- **Overview:** KPIs, metrics by state/affiliate/county/model/device type, top priority devices
- **Lifecycle:** Approaching EoL, past EoL, unknown lifecycle drilldown
- **Cost & Risk:** Estimated replacement costs, prioritization for refresh/investment
- **Geo Bundling:** Sites within radius (1/5/10 mi) for lifecycle bundling
- **Exceptions:** Decommissioned or omitted devices
- **Data Pipeline:** Documentation of the loading process

## Assumptions (per prompt)

- Only active/reachable devices
- NA Device Type ? Switch, Router, or Voice Gateway
- CatCtr/Prime override NA for duplicate APs/WLCs
- Host name: first 2 chars = State, chars 3?5 = site code
