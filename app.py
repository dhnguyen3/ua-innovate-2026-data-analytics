# app.py
# Southern Company — UA Innovate 2026 Lifecycle Dashboard
#
# Answers: What equipment is approaching EoS/EoL? Where is risk highest?
# Sites within radius to bundle lifecycle? Past-EoL devices? Exceptions? Cost/risk correlation?
#
# Run:
#   py -m pip install -r requirements.txt
#   py -m streamlit run app.py

import os
import subprocess
import sys
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

# -----------------------------
# Chart styling helper
# -----------------------------
CHART_THEME = {
    "template": "plotly_white",
    "font": {"family": "Inter, system-ui, sans-serif", "size": 13},
    "margin": {"t": 40, "r": 20, "b": 50, "l": 60},
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(248,249,250,0.8)",
    "xaxis": {"showgrid": True, "gridcolor": "rgba(0,0,0,0.08)", "zeroline": False},
    "yaxis": {"showgrid": True, "gridcolor": "rgba(0,0,0,0.08)", "zeroline": False},
    "showlegend": True,
    "legend": {"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
    "height": 380,
}
def _style_chart(fig, height=None):
    h = height or CHART_THEME["height"]
    fig.update_layout(template=CHART_THEME["template"], font=CHART_THEME["font"], margin=CHART_THEME["margin"],
                     paper_bgcolor=CHART_THEME["paper_bgcolor"], plot_bgcolor=CHART_THEME["plot_bgcolor"],
                     xaxis=CHART_THEME["xaxis"], yaxis=CHART_THEME["yaxis"], height=h)
    return fig

# -----------------------------
# App Config
# -----------------------------
st.set_page_config(page_title="SoCo Lifecycle Dashboard", layout="wide", initial_sidebar_state="expanded")

# Professional, web-page-style layout and styling
st.markdown("""
<style>
    /* Main layout — clean, readable max-width feel */
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1400px; font-size: 1rem; }
    
    /* Header area */
    h1 { 
        font-size: 1.75rem !important; 
        font-weight: 700 !important; 
        color: #1a1a2e !important;
        margin-bottom: 0.25rem !important;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e8e8ed;
    }
    .stCaptionContainer { color: #5a5a6e; font-size: 0.9rem; margin-bottom: 1rem; }
    
    /* Sidebar — organized, scannable */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f8f9fc 0%, #f0f2f7 100%);
    }
    [data-testid="stSidebar"] .stMarkdown { font-weight: 600; color: #2d3748; font-size: 0.95rem !important; }
    [data-testid="stSidebar"] label { font-size: 0.95rem !important; }
    [data-testid="stSidebar"] .streamlit-expanderHeader { font-weight: 500; }
    
    /* Section headers in content */
    h2, h3 { color: #1a1a2e !important; font-weight: 600 !important; }
    h2 { font-size: 1.25rem !important; margin-top: 1.5rem !important; }
    h3 { font-size: 1.1rem !important; }
    h4 { color: #2d3748 !important; font-weight: 600 !important; font-size: 1.05rem !important; margin-top: 1rem !important; }
    
    /* Proportional text visibility */
    .stMarkdown, .stMarkdown p { font-size: 1rem !important; line-height: 1.5 !important; }
    .stCaptionContainer, .stCaptionContainer p { font-size: 0.95rem !important; color: #5a5a6e !important; }
    .stDataFrame { font-size: 0.95rem !important; }
    
    /* Metrics / KPI cards */
    [data-testid="stMetric"] {
        background: #fff;
        padding: 1rem 1.25rem;
        border-radius: 8px;
        border: 1px solid #e8e8ed;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }
    [data-testid="stMetricValue"] { font-weight: 700 !important; color: #1a1a2e !important; font-size: 1.35rem !important; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem !important; color: #5a5a6e !important; }
    
    /* Charts */
    .stPlotlyChart { 
        border-radius: 8px; 
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
        border: 1px solid #e8e8ed;
    }
    
    /* Tabs — web-style navigation */
    .stTabs [data-baseweb="tab-list"] { gap: 0; border-bottom: 1px solid #e8e8ed; }
    .stTabs [data-baseweb="tab"] { 
        padding: 0.75rem 1.25rem; 
        font-weight: 500;
        font-size: 1rem !important;
        border-radius: 6px 6px 0 0;
    }
    .stTabs [aria-selected="true"] { background: #f0f4ff; color: #2563eb; }
    
    /* Dividers and spacing */
    hr { margin: 1.5rem 0 !important; border-color: #e8e8ed !important; }
    
    /* DataFrames */
    .stDataFrame { border-radius: 8px; overflow: hidden; border: 1px solid #e8e8ed; }
    
    /* Buttons */
    .stButton > button { 
        border-radius: 6px; 
        font-weight: 500;
        padding: 0.4rem 1rem;
    }
    
    /* Status / info boxes */
    .stAlert { border-radius: 6px; }
</style>
""", unsafe_allow_html=True)

# Header
st.title("Southern Company — Lifecycle Dashboard")
st.caption("Evaluate network equipment costs, identify improvement opportunities, and plan lifecycle bundling. Use the sidebar to load data and filter by state, affiliate, or device type.")

# -----------------------------
# Defaults (resolved relative to app directory)
# -----------------------------
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join("data", "outputs")
DEFAULT_EXCEL_PATH = os.path.join("data", "raw", "UAInnovateDataset-SoCo.xlsx")

def _resolve_path(path: str, base: str = _APP_DIR) -> str:
    """Resolve path relative to app directory if not absolute."""
    if not path:
        return path
    return path if os.path.isabs(path) else os.path.normpath(os.path.join(base, path))

def _resolve_excel_path(path: str) -> str:
    """Resolve Excel path, trying .xlsx if the given path doesn't exist but looks incomplete."""
    resolved = _resolve_path(path.strip())
    if file_exists(resolved):
        return resolved
    # If path looks incomplete (ends with "." or missing extension), try .xlsx
    if resolved and not resolved.lower().endswith((".xlsx", ".xls")):
        base_path = resolved.rstrip(".")
        alt = (base_path + ".xlsx") if not base_path.lower().endswith(".xlsx") else base_path
        if file_exists(alt):
            return alt
    return resolved

CSV_FILES = {
    "core": "core_device_table.csv",
    "approaching": "devices_approaching_eol.csv",
    "past": "devices_past_eol.csv",
    "approaching_eos": "devices_approaching_eos.csv",
    "past_eos": "devices_past_eos.csv",
    "unknown": "devices_unknown_lifecycle.csv",
    "exceptions": "devices_exceptions.csv",
}

# -----------------------------
# Utilities
# -----------------------------
def file_exists(path: str) -> bool:
    return bool(path) and os.path.exists(path)

def ensure_cols(df: pd.DataFrame, defaults: dict) -> pd.DataFrame:
    for c, v in defaults.items():
        if c not in df.columns:
            df[c] = v
    return df

def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def safe_unique_sorted(series: pd.Series):
    if series is None:
        return []
    vals = series.dropna().astype(str).str.strip()
    vals = vals[vals != ""].unique().tolist()
    return sorted(vals)

# Haversine distance (miles) — WGS84 mean radius, vectorized
_EARTH_R_MI = 3958.8  # miles (WGS84 authalic mean)

def haversine_miles(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [np.asarray(lat1), np.asarray(lon1), np.asarray(lat2), np.asarray(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * (np.sin(dlon / 2.0) ** 2)
    return 2 * _EARTH_R_MI * np.arcsin(np.minimum(1.0, np.sqrt(a)))

def build_radius_clusters(sites_df: pd.DataFrame, radius_mi: float):
    """
    Connected-component clustering: sites within radius_mi are grouped.
    Deduplicates by site_code (mean lat/lon) so each physical site counts once.
    sites_df must have: site_code, latitude, longitude plus any metrics.
    """
    sites_df = sites_df.dropna(subset=["latitude", "longitude"]).copy()
    # One row per site_code: mean lat/lon, sum counts (avoids duplicate coords skewing clusters)
    agg_dict = {"latitude": "mean", "longitude": "mean"}
    for c in ["device_count", "expired_count", "approaching_count", "unknown_count", "exception_count"]:
        if c in sites_df.columns:
            agg_dict[c] = "sum"
    for c in ["State", "affiliate", "county"]:
        if c in sites_df.columns:
            agg_dict[c] = "first"
    sites_df = sites_df.groupby("site_code", as_index=False).agg(agg_dict)
    sites_df = sites_df.reset_index(drop=True)
    n = len(sites_df)
    if n == 0:
        return sites_df.assign(cluster_id=pd.Series(dtype=int)), pd.DataFrame()

    lat = sites_df["latitude"].to_numpy(dtype=float)
    lon = sites_df["longitude"].to_numpy(dtype=float)

    visited = np.zeros(n, dtype=bool)
    cluster_ids = np.full(n, -1, dtype=int)
    cluster = 0

    for i in range(n):
        if visited[i]:
            continue
        stack = [i]
        visited[i] = True
        cluster_ids[i] = cluster

        while stack:
            u = stack.pop()
            d = haversine_miles(lat[u], lon[u], lat, lon)
            neigh = np.where((d <= radius_mi) & (~visited))[0]
            for v in neigh:
                visited[v] = True
                cluster_ids[v] = cluster
                stack.append(v)

        cluster += 1

    out = sites_df.copy()
    out["cluster_id"] = cluster_ids

    # Max distance within each cluster (for verification)
    def _max_dist_in_cluster(cluster_rows):
        lat = cluster_rows["latitude"].values
        lon = cluster_rows["longitude"].values
        n = len(lat)
        if n < 2:
            return 0.0
        mx = 0.0
        for i in range(n):
            d = haversine_miles(lat[i], lon[i], lat, lon)
            mx = max(mx, float(np.nanmax(d)))
        return round(mx, 2)

    summary = (
        out.groupby("cluster_id")
        .agg(
            site_count=("site_code", "nunique"),
            device_count=("device_count", "sum"),
            expired_count=("expired_count", "sum"),
            approaching_count=("approaching_count", "sum"),
            unknown_count=("unknown_count", "sum"),
            exception_count=("exception_count", "sum"),
        )
        .reset_index()
    )
    summary["max_dist_mi"] = summary["cluster_id"].apply(
        lambda cid: _max_dist_in_cluster(out[out["cluster_id"] == cid])
    )
    summary = summary.sort_values(["site_count", "device_count"], ascending=False).reset_index(drop=True)

    return out, summary

# -----------------------------
# Data Loaders
# -----------------------------
@st.cache_data
def load_csv_outputs(output_dir: str):
    resolved_dir = _resolve_path(output_dir)
    def read_one(name: str):
        path = os.path.join(resolved_dir, CSV_FILES[name])
        if not file_exists(path):
            return None, path
        return pd.read_csv(path, low_memory=False), path

    core, core_path = read_one("core")
    approaching, approaching_path = read_one("approaching")
    past, past_path = read_one("past")
    approaching_eos, approaching_eos_path = read_one("approaching_eos")
    past_eos, past_eos_path = read_one("past_eos")
    unknown, unknown_path = read_one("unknown")
    exceptions, exceptions_path = read_one("exceptions")

    paths = {
        "core": core_path,
        "approaching": approaching_path,
        "past": past_path,
        "approaching_eos": approaching_eos_path,
        "past_eos": past_eos_path,
        "unknown": unknown_path,
        "exceptions": exceptions_path,
    }

    return core, approaching, past, approaching_eos, past_eos, unknown, exceptions, paths

@st.cache_data
def load_solid_loc_geo(excel_path: str):
    resolved = _resolve_excel_path(excel_path)
    if not file_exists(resolved):
        return None
    excel_path = resolved
    # Load only SOLID-Loc for geo enrichment
    solid_loc = pd.read_excel(excel_path, sheet_name="SOLID-Loc")
    # Standardize to names we’ll use
    rename = {
        "Site Code": "Site Code Extracted",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "PhysicalAddressCounty": "county",
        "Call Group": "affiliate",
        "Site Name": "site_name",
    }
    solid_loc = solid_loc.rename(columns={k: v for k, v in rename.items() if k in solid_loc.columns})
    keep = [c for c in ["Site Code Extracted", "latitude", "longitude", "county", "affiliate", "site_name"] if c in solid_loc.columns]
    if "Site Code Extracted" not in keep:
        return None
    solid_loc = solid_loc[keep].drop_duplicates("Site Code Extracted")
    return solid_loc

def validate_required_columns(df: pd.DataFrame, required: list[str]):
    missing = [c for c in required if c not in df.columns]
    return missing

# -----------------------------
# Sidebar Config
# -----------------------------
with st.sidebar:
    st.markdown("### Settings")

    with st.expander("Data & Pipeline", expanded=True):
        output_dir = st.text_input("CSV Output Directory", DEFAULT_OUTPUT_DIR, help="Folder containing device CSVs")
        excel_path = st.text_input("Excel Path", DEFAULT_EXCEL_PATH, help="For geo enrichment & pipeline")
        if st.button("Run Pipeline from Excel", use_container_width=True):
            backend_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ua-innovate-backend.py")
            base_dir = os.path.dirname(backend_script)
            out_abs = output_dir if os.path.isabs(output_dir) else os.path.normpath(os.path.join(base_dir, output_dir))
            excel_abs = _resolve_excel_path(excel_path)
            if file_exists(excel_abs) and os.path.exists(backend_script):
                with st.spinner("Processing Excel..."):
                    result = subprocess.run(
                        [sys.executable, backend_script, excel_abs, out_abs],
                        capture_output=True,
                        text=True,
                        cwd=base_dir,
                    )
                if result.returncode == 0:
                    st.success(result.stdout or "Pipeline complete. Click Reload Data.")
                    st.cache_data.clear()
                else:
                    st.error(result.stderr or result.stdout or "Pipeline failed.")
            else:
                st.warning("Excel path not found.")
        use_geo_enrichment = st.toggle("Geo Enrichment", value=True, help="Load lat/long & affiliate from Excel")
        radius_mi = st.select_slider("Bundling Radius (mi)", options=[1, 5, 10, 25, 50], value=5)
        refresh = st.button("Reload Data", use_container_width=True, type="primary")

    if refresh:
        st.cache_data.clear()

core, approaching, past, approaching_eos, past_eos, unknown, exceptions, paths = load_csv_outputs(output_dir)

# Load status in sidebar
with st.sidebar:
    with st.expander("Data Status", expanded=core is None):
        if core is None:
            st.error("Core CSV not found. Check output directory.")
            st.stop()
        st.success(f"**{len(core):,}** devices loaded")
        for k, p in paths.items():
            st.caption(f"{k}: `{p}`")

# -----------------------------
# Normalize / ensure columns exist
# -----------------------------
# These columns ARE present in your cousin’s outputs (based on the CSVs you uploaded):
# Host Name, Device Status, Device Type Standard, Device Model, EoL, EoL_Date, Days_to_EoL, State, Site Code Extracted, Exception_Flag, Exception_Reason
core = ensure_cols(core, {
    "Host Name": "",
    "Device Status": "",
    "Device Type Standard": "",
    "Device Model": "",
    "State": "",
    "Site Code Extracted": "",
    "Days_to_EoL": np.nan,
    "EoL_Date": "",
    "EoL": "",
    "Days_to_EoS": np.nan,
    "EoS_Date": "",
    "EoS": "",
    "Exception_Flag": False,
    "Exception_Reason": "",
    "Repl Device": "",
    "ReplaceNow": "",
    "PrepareToReplace": "",
    "Material Cost": np.nan,
    "Labor Cost": np.nan,
    "Device Cost": np.nan,
})

if "EoS" in core.columns and ("Days_to_EoS" not in core.columns or core["Days_to_EoS"].isna().all()):
    core["EoS_Date"] = pd.to_datetime(core.get("EoS"), errors="coerce")
    core["Days_to_EoS"] = (core["EoS_Date"] - pd.Timestamp.today()).dt.days
core = coerce_numeric(core, ["Days_to_EoL", "Days_to_EoS", "Material Cost", "Labor Cost", "Device Cost"])

# Optional: enrich with geo from Excel SOLID-Loc (toggle has clear effect: OFF = no geo data)
if use_geo_enrichment:
    geo = load_solid_loc_geo(excel_path)
    if geo is None:
        st.warning("Geo enrichment enabled, but SOLID-Loc could not be loaded. Geo tab will be limited.")
    else:
        # Drop existing geo cols before merge to avoid _x/_y suffixes; geo becomes source of truth
        core = core.drop(columns=["latitude", "longitude", "county", "affiliate", "site_name"], errors="ignore")
        core = core.merge(geo, on="Site Code Extracted", how="left")
        core = ensure_cols(core, {"latitude": np.nan, "longitude": np.nan, "county": "Unknown", "affiliate": "Unknown", "site_name": ""})
else:
    # Geo OFF: overwrite with defaults so toggle has visible effect (don't use CSV's baked-in geo)
    core = core.drop(columns=["latitude", "longitude", "county", "affiliate", "site_name"], errors="ignore")
    core = ensure_cols(core, {"latitude": np.nan, "longitude": np.nan, "county": "Unknown", "affiliate": "Unknown", "site_name": ""})

# Fill NaN in affiliate/county so charts always have displayable values (including "Unknown")
for col in ["affiliate", "county"]:
    if col in core.columns:
        core[col] = core[col].fillna("Unknown").astype(str).replace("nan", "Unknown")

# Build summary counts by site using the pre-filtered CSVs (if present)
def make_site_counts(df_subset: pd.DataFrame, label: str):
    if df_subset is None or df_subset.empty:
        return pd.DataFrame(columns=["Site Code Extracted", label])
    tmp = df_subset.copy()
    tmp = ensure_cols(tmp, {"Site Code Extracted": ""})
    return tmp.groupby("Site Code Extracted").size().reset_index(name=label)

site_counts = core.groupby("Site Code Extracted").size().reset_index(name="device_count")

site_counts = site_counts.merge(make_site_counts(past, "expired_count"), on="Site Code Extracted", how="left")
site_counts = site_counts.merge(make_site_counts(approaching, "approaching_count"), on="Site Code Extracted", how="left")
site_counts = site_counts.merge(make_site_counts(unknown, "unknown_count"), on="Site Code Extracted", how="left")
site_counts = site_counts.merge(make_site_counts(exceptions, "exception_count"), on="Site Code Extracted", how="left")
for c in ["expired_count", "approaching_count", "unknown_count", "exception_count"]:
    if c in site_counts.columns:
        site_counts[c] = site_counts[c].fillna(0).astype(int)

# Add geo columns to site_counts if available in core
site_geo_cols = core[["Site Code Extracted", "latitude", "longitude", "county", "affiliate", "site_name"]].drop_duplicates("Site Code Extracted")
site_counts = site_counts.merge(site_geo_cols, on="Site Code Extracted", how="left")

# Drop duplicate 'model' column (CSV has both 'model' and 'Device Model') before rename
if "model" in core.columns and "Device Model" in core.columns:
    core = core.drop(columns=["model"], errors="ignore")
# Rename to simpler names for UI
core = core.rename(columns={
    "Host Name": "hostname",
    "Device Status": "device_status",
    "Device Type Standard": "device_type",
    "Device Model": "model",
    "Site Code Extracted": "site_code",
})
site_counts = site_counts.rename(columns={"Site Code Extracted": "site_code"})

# -----------------------------
# Sidebar Filters
# -----------------------------
with st.sidebar:
    with st.expander("Filter View", expanded=True):
        state_opts = safe_unique_sorted(core["State"]) if "State" in core.columns else []
        devtype_opts = safe_unique_sorted(core["device_type"])
        status_opts = safe_unique_sorted(core["device_status"])
        affiliate_opts = safe_unique_sorted(core["affiliate"]) if "affiliate" in core.columns else []
        county_opts = safe_unique_sorted(core["county"]) if "county" in core.columns else []

        sel_states = st.multiselect("State", state_opts, default=state_opts)
        sel_types = st.multiselect("Device Type", devtype_opts, default=devtype_opts)
        sel_status = st.multiselect("Device Status", status_opts, default=status_opts)
        sel_affiliate = st.multiselect("Affiliate", affiliate_opts, default=affiliate_opts) if affiliate_opts else None
        sel_county = st.multiselect("County", county_opts, default=county_opts) if county_opts else None
        show_only_active = st.toggle("Only Active devices", value=True)

# Apply filters
filtered = core.copy()
if "State" in filtered.columns and sel_states:
    filtered = filtered[filtered["State"].astype(str).isin(sel_states)]
if sel_types:
    filtered = filtered[filtered["device_type"].astype(str).isin(sel_types)]
if sel_status:
    filtered = filtered[filtered["device_status"].astype(str).isin(sel_status)]
if sel_affiliate is not None and "affiliate" in filtered.columns:
    filtered = filtered[filtered["affiliate"].astype(str).isin(sel_affiliate)]
if sel_county is not None and "county" in filtered.columns:
    filtered = filtered[filtered["county"].astype(str).isin(sel_county)]
if show_only_active:
    filtered = filtered[filtered["device_status"].astype(str).str.lower().eq("active")]


def _filter_by_hostnames(df: pd.DataFrame | None, hostnames: set) -> int:
    """Count rows in df whose Host Name is in the filtered hostname set."""
    if df is None or df.empty or not hostnames:
        return 0
    hn_col = "Host Name" if "Host Name" in df.columns else "hostname"
    if hn_col not in df.columns:
        return len(df)
    return int((df[hn_col].astype(str).str.strip().isin(hostnames)).sum())

# -----------------------------
# Tabs
# -----------------------------
tab_overview, tab_lifecycle, tab_cost, tab_geo, tab_ex, tab_docs = st.tabs([
    "Overview",
    "Lifecycle",
    "Cost & Risk",
    "Geo Bundling",
    "Exceptions",
    "Pipeline",
])

# -----------------------------
# OVERVIEW TAB
# -----------------------------
with tab_overview:
    st.markdown("### Executive Summary")
    st.caption("Key metrics for your filtered device set. **In Scope** excludes Decom exceptions. Use the sidebar to narrow by state, affiliate, or device type.")

    # KPI cards — exclude exceptions from "in scope" totals (governance)
    in_scope = filtered[~filtered.get("Exception_Flag", pd.Series(dtype=bool)).fillna(False)] if "Exception_Flag" in filtered.columns else filtered
    exc_in_filtered = filtered[filtered.get("Exception_Flag", pd.Series(dtype=bool)).fillna(False)] if "Exception_Flag" in filtered.columns else filtered.iloc[0:0]
    total_devices = len(filtered)
    in_scope_count = len(in_scope)
    exc_count = len(exc_in_filtered)
    total_sites = filtered["site_code"].nunique()
    filtered_hostnames = set(filtered["hostname"].dropna().astype(str).str.strip())
    in_scope_hostnames = set(in_scope["hostname"].dropna().astype(str).str.strip())

    approaching_count = _filter_by_hostnames(approaching, in_scope_hostnames)
    past_count = _filter_by_hostnames(past, in_scope_hostnames)
    approaching_eos_count = _filter_by_hostnames(approaching_eos, in_scope_hostnames)
    past_eos_count = _filter_by_hostnames(past_eos, in_scope_hostnames)
    unknown_count = _filter_by_hostnames(unknown, in_scope_hostnames)
    exc_file_count = _filter_by_hostnames(exceptions, filtered_hostnames)
    days_eos = pd.to_numeric(in_scope.get("Days_to_EoS", np.nan), errors="coerce")
    # Fallback EoS counts from core when backend CSVs don't exist
    if (approaching_eos is None or approaching_eos.empty) and days_eos.notna().any():
        approaching_eos_count = int(((days_eos >= 0) & (days_eos <= 365)).sum())
    if (past_eos is None or past_eos.empty) and days_eos.notna().any():
        past_eos_count = int((days_eos < 0).sum())

    k1, k2, k3, k4, k5, k6, k7, k8 = st.columns(8)
    k1.metric("Devices (In Scope)", f"{in_scope_count:,}", help="Excludes Decom/exceptions")
    k2.metric("Sites", f"{total_sites:,}")
    k3.metric("Approaching EoL", f"{approaching_count:,}")
    k4.metric("Past EoL", f"{past_count:,}")
    k5.metric("Approaching EoS", f"{approaching_eos_count:,}", help="Support ends within 1 yr")
    k6.metric("Past EoS", f"{past_eos_count:,}", help="Support ended, no software updates")
    k7.metric("Unknown Lifecycle", f"{unknown_count:,}")
    k8.metric("Exceptions (Decom)", f"{exc_count:,}", help="Excluded from refresh scope")

    st.divider()
    st.markdown("### Device Distribution")
    st.markdown("#### Metrics by State, Affiliate, County, Model, Device Type")
    c1, c2 = st.columns(2)
    with c1:
        if "State" in filtered.columns:
            by_state = filtered.groupby("State").size().reset_index(name="count").sort_values("count", ascending=False)
            st.plotly_chart(_style_chart(px.bar(by_state, x="State", y="count", title="Devices by State")), use_container_width=True)
        else:
            st.info("No State column found.")
    with c2:
        by_type = filtered.groupby("device_type").size().reset_index(name="count").sort_values("count", ascending=False)
        st.plotly_chart(_style_chart(px.bar(by_type, x="device_type", y="count", title="Devices by Device Type")), use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        if "affiliate" in filtered.columns:
            by_aff = filtered.groupby("affiliate").size().reset_index(name="count").sort_values("count", ascending=False).head(15)
            if not by_aff.empty:
                st.plotly_chart(
                    _style_chart(px.bar(by_aff, x="affiliate", y="count", title="Devices by Affiliate (Call Group)",
                           labels={"affiliate": "Affiliate (Call Group)", "count": "Count"})),
                    use_container_width=True
                )
            else:
                st.info("No affiliate data to display.")
        else:
            st.info("Affiliate data requires geo enrichment.")
    with c4:
        if "county" in filtered.columns and filtered["county"].notna().any() and (filtered["county"] != "Unknown").any():
            by_county = filtered[filtered["county"] != "Unknown"].groupby("county").size().reset_index(name="count").sort_values("count", ascending=False).head(15)
            if not by_county.empty:
                st.plotly_chart(_style_chart(px.bar(by_county, x="county", y="count", title="Devices by County")), use_container_width=True)
            else:
                st.info("County data requires geo enrichment.")
        else:
            st.info("County data requires geo enrichment.")

    if "model" in filtered.columns and filtered["model"].notna().any():
        by_model = filtered.groupby("model").size().reset_index(name="count").sort_values("count", ascending=False).head(20)
        st.plotly_chart(_style_chart(px.bar(by_model, x="model", y="count", title="Devices by Model")), use_container_width=True)
    else:
        st.info("Model data not available.")

    st.divider()
    st.markdown("### Priority Devices")
    st.markdown("#### Top 25 (Past EoL first, then by Days to EoL)")
    tmp = filtered.copy()
    tmp["Days_to_EoL"] = pd.to_numeric(tmp.get("Days_to_EoL", np.nan), errors="coerce")

    # Priority: Past EoL (Days_to_EoL < 0), then approaching (0..365), then others
    tmp["priority_bucket"] = np.select(
        [
            tmp["Days_to_EoL"].notna() & (tmp["Days_to_EoL"] < 0),
            tmp["Days_to_EoL"].notna() & (tmp["Days_to_EoL"] >= 0) & (tmp["Days_to_EoL"] <= 365),
        ],
        ["Past EoL", "Approaching EoL"],
        default="Other/Unknown",
    )

    tmp = tmp.sort_values(
        by=["priority_bucket", "Days_to_EoL"],
        ascending=[True, True]  # Past EoL/Approaching come first because of string order? We'll enforce order
    )

    # Enforce bucket order manually
    bucket_order = {"Past EoL": 0, "Approaching EoL": 1, "Other/Unknown": 2}
    tmp["bucket_rank"] = tmp["priority_bucket"].map(bucket_order).fillna(9)
    tmp = tmp.sort_values(["bucket_rank", "Days_to_EoL"], ascending=[True, True])

    show_cols = [
        "State", "affiliate", "county", "site_code",
        "hostname", "device_type", "model", "Repl Device",
        "EoS", "EoS_Date", "Days_to_EoS", "EoL", "EoL_Date", "Days_to_EoL",
        "Exception_Flag", "Exception_Reason"
    ]
    show_cols = [c for c in show_cols if c in tmp.columns]
    st.dataframe(tmp[show_cols].head(25), use_container_width=True, height=420)

    st.download_button(
        "Download filtered devices (CSV)",
        data=filtered.to_csv(index=False).encode("utf-8"),
        file_name="filtered_devices.csv",
        mime="text/csv"
    )

# -----------------------------
# LIFECYCLE TAB
# -----------------------------
with tab_lifecycle:
    st.markdown("### Lifecycle Drilldown")
    st.caption("Explore devices by EoL (End of Life) and EoS (End of Support) status. EoS = when software updates end; EoL = when hardware is fully end-of-life.")

    dataset_choice = st.radio(
        "Choose view",
        ["Approaching EoL", "Past EoL", "Approaching EoS", "Past EoS", "Unknown Lifecycle"],
        horizontal=True
    )

    if dataset_choice == "Approaching EoL":
        view = approaching if approaching is not None else pd.DataFrame()
        title = "Devices Approaching EoL (<= 365 days)"
    elif dataset_choice == "Past EoL":
        view = past if past is not None else pd.DataFrame()
        title = "Devices Past EoL"
    elif dataset_choice == "Approaching EoS":
        view = approaching_eos if approaching_eos is not None and not approaching_eos.empty else pd.DataFrame()
        if view.empty and "Days_to_EoS" in filtered.columns:
            days_eos = pd.to_numeric(filtered.get("Days_to_EoS", np.nan), errors="coerce")
            view = filtered[(days_eos.notna()) & (days_eos >= 0) & (days_eos <= 365)].copy()
        title = "Devices Approaching EoS (support ends within 1 yr)"
    elif dataset_choice == "Past EoS":
        view = past_eos if past_eos is not None and not past_eos.empty else pd.DataFrame()
        if view.empty and "Days_to_EoS" in filtered.columns:
            days_eos = pd.to_numeric(filtered.get("Days_to_EoS", np.nan), errors="coerce")
            view = filtered[(days_eos.notna()) & (days_eos < 0)].copy()
        title = "Devices Past EoS (support ended — no software updates)"
    else:
        view = unknown if unknown is not None else pd.DataFrame()
        title = "Devices with Unknown Lifecycle"

    st.markdown(f"#### {title}")
    if view is None or view.empty:
        st.info("No rows found for this dataset.")
    else:
        # Normalize column names for display (same as core)
        view2 = view.copy()
        if "model" in view2.columns and "Device Model" in view2.columns:
            view2 = view2.drop(columns=["model"], errors="ignore")
        view2 = view2.rename(columns={
            "Host Name": "hostname",
            "Device Status": "device_status",
            "Device Type Standard": "device_type",
            "Device Model": "model",
            "Site Code Extracted": "site_code",
        })
        view2 = coerce_numeric(view2, ["Days_to_EoL", "Days_to_EoS"])

        # Quick breakdown charts
        c1, c2 = st.columns(2)
        with c1:
            if "State" in view2.columns:
                by_state = view2.groupby("State").size().reset_index(name="count").sort_values("count", ascending=False)
                st.plotly_chart(_style_chart(px.bar(by_state.head(25), x="State", y="count", title="Devices by State")), use_container_width=True)
            else:
                st.info("No State column for this dataset.")
        with c2:
            if "device_type" in view2.columns:
                by_type = view2.groupby("device_type").size().reset_index(name="count").sort_values("count", ascending=False)
                st.plotly_chart(_style_chart(px.bar(by_type, x="device_type", y="count", title="Devices by Device Type")), use_container_width=True)
            else:
                st.info("No Device Type column for this dataset.")

        st.divider()

        # Table + download
        cols = [
            "State", "site_code", "hostname", "device_type", "model",
            "EoS", "EoS_Date", "Days_to_EoS", "EoL", "EoL_Date", "Days_to_EoL",
            "Exception_Flag", "Exception_Reason"
        ]
        cols = [c for c in cols if c in view2.columns]
        st.dataframe(view2[cols].sort_values(["State", "site_code", "hostname"]).reset_index(drop=True),
                     use_container_width=True, height=520)

        st.download_button(
            f"Download {dataset_choice} (CSV)",
            data=view2.to_csv(index=False).encode("utf-8"),
            file_name=f"{dataset_choice.replace(' ', '_').lower()}.csv",
            mime="text/csv"
        )

# -----------------------------
# COST & RISK TAB
# -----------------------------
with tab_cost:
    st.markdown("### Cost & Risk Analysis")
    st.caption("Prioritize refresh and investment based on risk tier and estimated replacement costs.")

    # Build risk dataframe (always - risk is calculated from lifecycle)
    risk_df = filtered.copy()
    risk_df["Days_to_EoL"] = pd.to_numeric(risk_df.get("Days_to_EoL", np.nan), errors="coerce")
    risk_df["Exception_Flag"] = risk_df.get("Exception_Flag", pd.Series(dtype=bool)).fillna(False)

    # Risk calculation: tier + numeric score (1-10, 10 = highest risk)
    def _risk_tier_and_score(row):
        days = row.get("Days_to_EoL")
        exc = row.get("Exception_Flag", False)
        if exc:
            return "Exception (Decom)", 2  # Lower priority for planning
        if pd.isna(days):
            return "Unknown Lifecycle", 6  # Can't plan, moderate risk
        if days < 0:
            return "Past EoL (Critical)", 10  # Support ended, security risk
        if days <= 90:
            return "Approaching (< 90 days)", 8
        if days <= 365:
            return "Approaching (≤ 1 yr)", 5
        return "Within Lifecycle", 1

    risk_df["risk_tier"] = risk_df.apply(lambda r: _risk_tier_and_score(r)[0], axis=1)
    risk_df["risk_score"] = risk_df.apply(lambda r: _risk_tier_and_score(r)[1], axis=1)

    # Cost calculation (use ModelData when available, else 0 for ranking)
    cost_cols_avail = [c for c in ["Material Cost", "Labor Cost", "Device Cost"] if c in risk_df.columns]
    if cost_cols_avail:
        for c in cost_cols_avail:
            risk_df[c] = pd.to_numeric(risk_df.get(c, np.nan), errors="coerce").fillna(0)
        risk_df["total_cost"] = risk_df.get("Material Cost", 0) + risk_df.get("Labor Cost", 0) + risk_df.get("Device Cost", 0)
    else:
        risk_df["total_cost"] = 0
    has_costs = cost_cols_avail and (risk_df["total_cost"] > 0).any()

    # Priority score: risk_score * log(1 + cost) — favors high risk, then high cost
    risk_df["priority_score"] = risk_df["risk_score"] * np.log1p(np.maximum(risk_df["total_cost"], 0))

    # --- Risk KPIs ---
    st.markdown("#### Risk Summary")
    at_risk = risk_df[risk_df["risk_score"] >= 5]
    critical = risk_df[risk_df["risk_tier"] == "Past EoL (Critical)"]
    approx_90 = risk_df[risk_df["risk_tier"] == "Approaching (< 90 days)"]
    approx_365 = risk_df[risk_df["risk_tier"] == "Approaching (≤ 1 yr)"]
    unknown_r = risk_df[risk_df["risk_tier"] == "Unknown Lifecycle"]

    r1, r2, r3, r4, r5 = st.columns(5)
    r1.metric("Critical (Past EoL)", f"{len(critical):,}", help="Support ended — highest security/operational risk")
    r2.metric("High (< 90 days)", f"{len(approx_90):,}", help="Urgent — refresh within quarter")
    r3.metric("Medium (≤ 1 yr)", f"{len(approx_365):,}", help="Plan refresh within year")
    r4.metric("Unknown Lifecycle", f"{len(unknown_r):,}", help="Cannot plan — needs model mapping")
    r5.metric("Total At-Risk", f"{len(at_risk):,}", help="Risk score ≥ 5")

    # --- Cost KPIs (when available) ---
    if has_costs:
        st.markdown("#### Cost Summary")
        st.caption("Replacement cost = Material + Labor + Device Cost. Past EoL and At-Risk totals exclude Decom exceptions.")
        cost_critical = critical["total_cost"].sum()
        cost_at_risk = at_risk["total_cost"].sum()
        risk_in_scope = risk_df[~risk_df.get("Exception_Flag", pd.Series(dtype=bool)).fillna(False)]
        cost_total = risk_in_scope["total_cost"].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Est. Cost (Past EoL)", f"${cost_critical:,.0f}")
        c2.metric("Est. Cost (All At-Risk)", f"${cost_at_risk:,.0f}")
        c3.metric("Total Est. Refresh Cost (In Scope)", f"${cost_total:,.0f}")
    else:
        st.info("Cost data from ModelData (Material + Labor + Device Cost). Run pipeline from Excel to include costs.")

    st.divider()

    # --- Risk Distribution Chart ---
    st.markdown("#### Risk Distribution by Tier")
    risk_counts = risk_df.groupby("risk_tier").size().reset_index(name="count")
    tier_order = ["Past EoL (Critical)", "Approaching (< 90 days)", "Approaching (≤ 1 yr)", "Unknown Lifecycle", "Exception (Decom)", "Within Lifecycle"]
    risk_counts["sort_key"] = risk_counts["risk_tier"].map({t: i for i, t in enumerate(tier_order)}).fillna(99)
    risk_counts = risk_counts.sort_values("sort_key").drop(columns=["sort_key"], errors="ignore")
    fig_risk = _style_chart(px.bar(risk_counts, x="risk_tier", y="count", title="Device Count by Risk Tier", color="count", color_continuous_scale="Reds"))
    st.plotly_chart(fig_risk, use_container_width=True)

    # --- Lifecycle vs Cost & Support (Q5: correlation) ---
    st.markdown("#### Lifecycle, Support, and Cost Correlation")
    st.caption("How lifecycle tier correlates with support risk, security risk, and replacement cost. Past EoL = no vendor support, highest security risk.")
    tier_corr = risk_df.groupby("risk_tier").agg(
        device_count=("hostname", "count"),
        total_cost=("total_cost", "sum"),
    ).reset_index()
    tier_corr["avg_cost"] = np.where(tier_corr["device_count"] > 0, tier_corr["total_cost"] / tier_corr["device_count"], 0)
    tier_order_corr = ["Past EoL (Critical)", "Approaching (< 90 days)", "Approaching (≤ 1 yr)", "Unknown Lifecycle", "Exception (Decom)", "Within Lifecycle"]
    tier_corr["sort_key"] = tier_corr["risk_tier"].map({t: i for i, t in enumerate(tier_order_corr)}).fillna(99)
    tier_corr = tier_corr.sort_values("sort_key")
    disp_corr = tier_corr[["risk_tier", "device_count", "total_cost", "avg_cost"]].copy()
    disp_corr.columns = ["Risk Tier", "Devices", "Total Est. Cost ($)", "Avg Cost/Device ($)"]
    disp_corr["Total Est. Cost ($)"] = disp_corr["Total Est. Cost ($)"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
    disp_corr["Avg Cost/Device ($)"] = disp_corr["Avg Cost/Device ($)"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
    st.dataframe(disp_corr, use_container_width=True, height=220)

    st.divider()

    # --- Risk by State / Affiliate ---
    st.markdown("#### Risk by State (at-risk devices only)")
    risk_by_state = at_risk.groupby("State").agg(
        device_count=("hostname", "count"),
        total_cost=("total_cost", "sum"),
    ).reset_index()
    risk_by_state = risk_by_state.sort_values("device_count", ascending=False)

    col_a, col_b = st.columns(2)
    with col_a:
        if not risk_by_state.empty:
            st.plotly_chart(
                _style_chart(px.bar(risk_by_state.head(15), x="State", y="device_count", title="At-Risk Devices by State", color="device_count", color_continuous_scale="Oranges")),
                use_container_width=True,
            )
    with col_b:
        if has_costs and not risk_by_state.empty and risk_by_state["total_cost"].sum() > 0:
            st.plotly_chart(
                _style_chart(px.bar(risk_by_state.head(15), x="State", y="total_cost", title="Est. Replacement Cost by State", color="total_cost", color_continuous_scale="Blues")),
                use_container_width=True,
            )
        else:
            critical_by_state = risk_df[risk_df["risk_tier"] == "Past EoL (Critical)"].groupby("State").size().reset_index(name="critical_count")
            critical_by_state = critical_by_state.sort_values("critical_count", ascending=False).head(15)
            if not critical_by_state.empty:
                st.plotly_chart(_style_chart(px.bar(critical_by_state, x="State", y="critical_count", title="Critical (Past EoL) by State", color="critical_count", color_continuous_scale="Reds")), use_container_width=True)
            else:
                st.info("No Past EoL devices in filtered view.")

    st.divider()

    # --- Highest Risk by Geography (Q1: quantify & rank) ---
    st.markdown("#### Highest Risk by Geography")
    st.caption("States and counties ranked by critical (Past EoL) device count — answers *where is highest risk?*")
    at_risk_in_scope = at_risk[~at_risk.get("Exception_Flag", pd.Series(dtype=bool)).fillna(False)]
    if "State" in at_risk_in_scope.columns and not at_risk_in_scope.empty:
        crit = at_risk_in_scope[at_risk_in_scope["risk_tier"] == "Past EoL (Critical)"] if "risk_tier" in at_risk_in_scope.columns else pd.DataFrame()
        if not crit.empty:
            risk_by_state = crit.groupby("State").size().reset_index(name="critical_count")
            risk_by_state = risk_by_state.merge(
                at_risk_in_scope.groupby("State").size().reset_index(name="at_risk_count"),
                on="State", how="outer"
            ).fillna(0).astype({"critical_count": int, "at_risk_count": int})
        else:
            risk_by_state = at_risk_in_scope.groupby("State").size().reset_index(name="at_risk_count")
            risk_by_state["critical_count"] = 0
        risk_by_state = risk_by_state.sort_values("critical_count", ascending=False).head(15)
        risk_by_state["risk_rank"] = range(1, len(risk_by_state) + 1)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(risk_by_state[["risk_rank", "State", "critical_count", "at_risk_count"]], use_container_width=True, height=320)
        with col2:
            if not risk_by_state.empty and risk_by_state["critical_count"].sum() > 0:
                st.plotly_chart(_style_chart(px.bar(risk_by_state.head(10), x="State", y="critical_count", title="Past EoL by State (Top 10)", color="critical_count", color_continuous_scale="Reds")), use_container_width=True)
    if "county" in at_risk_in_scope.columns and (at_risk_in_scope["county"] != "Unknown").any():
        crit = at_risk_in_scope[at_risk_in_scope["risk_tier"] == "Past EoL (Critical)"] if "risk_tier" in at_risk_in_scope.columns else at_risk_in_scope
        by_county = crit[crit["county"] != "Unknown"].groupby("county").size().reset_index(name="critical_count").sort_values("critical_count", ascending=False).head(15)
        if not by_county.empty:
            st.markdown("**Top counties by critical count**")
            st.dataframe(by_county.assign(rank=range(1, len(by_county) + 1))[["rank", "county", "critical_count"]], use_container_width=True, height=280)

    # --- Risk by Affiliate (when available) ---
    if "affiliate" in at_risk.columns and at_risk["affiliate"].notna().any() and (at_risk["affiliate"] != "Unknown").any():
        st.markdown("#### Risk by Affiliate (Call Group)")
        risk_by_aff = at_risk[at_risk["affiliate"].notna() & (at_risk["affiliate"] != "Unknown")].groupby("affiliate").agg(
            device_count=("hostname", "count"),
            total_cost=("total_cost", "sum"),
        ).reset_index().sort_values("device_count", ascending=False).head(15)
        if not risk_by_aff.empty:
            st.plotly_chart(_style_chart(px.bar(risk_by_aff, x="affiliate", y="device_count", title="At-Risk Devices by Affiliate")), use_container_width=True)

    st.divider()

    # --- Cost by Device Type ---
    st.markdown("#### Cost by Device Type")
    cost_agg = {"device_count": ("hostname", "count"), "total_cost": ("total_cost", "sum")}
    for c in ["Material Cost", "Labor Cost", "Device Cost"]:
        if c in risk_df.columns:
            cost_agg[c] = (c, "sum")
    cost_by_type = risk_df.groupby("device_type").agg(**cost_agg).reset_index()
    cost_by_type["avg_cost_per_device"] = np.where(cost_by_type["device_count"] > 0, cost_by_type["total_cost"] / cost_by_type["device_count"], 0)
    cost_by_type = cost_by_type.sort_values("total_cost", ascending=False)

    if has_costs and cost_by_type["total_cost"].sum() > 0:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(
                _style_chart(px.bar(cost_by_type, x="device_type", y="total_cost", title="Total Est. Cost by Device Type", color="total_cost", color_continuous_scale="Greens", labels={"total_cost": "Est. Cost ($)"})),
                use_container_width=True,
            )
        with col2:
            st.plotly_chart(
                _style_chart(px.bar(cost_by_type, x="device_type", y="avg_cost_per_device", title="Avg Cost per Device by Type", color="avg_cost_per_device", color_continuous_scale="Blues", labels={"avg_cost_per_device": "Avg Cost ($)"})),
                use_container_width=True,
            )
        # Table with cost breakdown by device type
        disp_type_cols = ["device_type", "device_count", "total_cost", "avg_cost_per_device"]
        for c in ["Material Cost", "Labor Cost", "Device Cost"]:
            if c in cost_by_type.columns:
                disp_type_cols.append(c)
        disp_type_cols = [c for c in disp_type_cols if c in cost_by_type.columns]
        cost_by_type_display = cost_by_type[disp_type_cols].copy()
        for c in cost_by_type_display.columns:
            if "cost" in str(c).lower():
                cost_by_type_display[c] = cost_by_type_display[c].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "")
        st.dataframe(cost_by_type_display, use_container_width=True, height=300)
    else:
        # Show device count by type even without cost data
        st.plotly_chart(_style_chart(px.bar(cost_by_type, x="device_type", y="device_count", title="Device Count by Type")), use_container_width=True)
        st.info("Cost breakdown by device type requires ModelData. Run pipeline from Excel to include costs.")

    st.divider()

    # --- Model Numbers by Urgency + Costs ---
    st.markdown("#### Model Numbers by Urgency of Replacement")
    st.caption("Breakdown of device models by risk tier. Same replacement model (Repl Device) may apply to multiple source models.")
    model_urgency = risk_df.groupby(["model", "risk_tier"]).agg(
        device_count=("hostname", "count"),
        total_cost=("total_cost", "sum"),
    ).reset_index()
    model_urgency = model_urgency.sort_values(["risk_tier", "device_count"], ascending=[True, False])
    # Pivot for display: model rows, urgency columns
    urgency_cols = ["Past EoL (Critical)", "Approaching (< 90 days)", "Approaching (≤ 1 yr)", "Unknown Lifecycle"]
    pivot_count = model_urgency.pivot_table(index="model", columns="risk_tier", values="device_count", aggfunc="sum", fill_value=0)
    pivot_cost = model_urgency.pivot_table(index="model", columns="risk_tier", values="total_cost", aggfunc="sum", fill_value=0)
    if "Repl Device" in risk_df.columns:
        repl_dict = risk_df[["model", "Repl Device"]].drop_duplicates("model").set_index("model")["Repl Device"].to_dict()
        pivot_count["Repl Device"] = pivot_count.index.map(lambda m: repl_dict.get(m, ""))
        pivot_cost["Repl Device"] = pivot_cost.index.map(lambda m: repl_dict.get(m, ""))
    # Show counts
    st.markdown("**Device count by model and urgency**")
    st.dataframe(pivot_count.head(40), use_container_width=True, height=350)
    if has_costs and not pivot_cost.empty:
        st.markdown("**Cost by model and urgency**")
        pivot_cost_display = pivot_cost.copy()
        for c in pivot_cost_display.columns:
            if c != "Repl Device" and pd.api.types.is_numeric_dtype(pivot_cost_display[c]):
                pivot_cost_display[c] = pivot_cost_display[c].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and x != 0 else "")
        st.dataframe(pivot_cost_display.head(40), use_container_width=True, height=350)

    st.divider()

    # --- Costs by Replacement Model (Repl Device) ---
    st.markdown("#### Costs by Replacement Model (Repl Device)")
    st.caption("Multiple source models may map to the same replacement. Use this to plan purchases by replacement SKU.")
    if "Repl Device" in risk_df.columns:
        repl_agg = {"device_count": ("hostname", "count"), "total_cost": ("total_cost", "sum")}
        for c in ["Material Cost", "Labor Cost", "Device Cost"]:
            if c in risk_df.columns:
                repl_agg[c] = (c, "sum")
        repl_subset = risk_df[risk_df["Repl Device"].notna() & (risk_df["Repl Device"] != "")]
        cost_by_repl = repl_subset.groupby("Repl Device").agg(**repl_agg).reset_index()
        source_models = repl_subset.groupby("Repl Device")["model"].apply(lambda x: ", ".join(sorted(x.dropna().astype(str).unique()))).reset_index()
        source_models.columns = ["Repl Device", "source_models"]
        cost_by_repl = cost_by_repl.merge(source_models, on="Repl Device", how="left")
        cost_by_repl = cost_by_repl.sort_values("total_cost", ascending=False)
        if not cost_by_repl.empty:
            if has_costs and cost_by_repl["total_cost"].sum() > 0:
                st.plotly_chart(_style_chart(px.bar(cost_by_repl.head(20), x="Repl Device", y="total_cost", title="Total Cost by Replacement Model", color="total_cost", color_continuous_scale="Purples")), use_container_width=True)
            repl_disp = cost_by_repl[["Repl Device", "device_count", "total_cost", "source_models"]].copy()
            if "Material Cost" in cost_by_repl.columns:
                repl_disp["Material Cost"] = cost_by_repl["Material Cost"]
                repl_disp["Labor Cost"] = cost_by_repl["Labor Cost"]
                repl_disp["Device Cost"] = cost_by_repl["Device Cost"]
            for c in repl_disp.columns:
                if c != "source_models" and "cost" in str(c).lower():
                    repl_disp[c] = repl_disp[c].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else str(x))
            st.dataframe(repl_disp.head(30), use_container_width=True, height=400)
        else:
            st.info("No Repl Device data. Run pipeline from Excel with ModelData.")
    else:
        st.info("Repl Device from ModelData. Run pipeline from Excel.")

    st.divider()

    # --- Costs by Site ---
    st.markdown("#### Costs by Site")
    cost_by_site = risk_df.groupby("site_code").agg(
        device_count=("hostname", "count"),
        total_cost=("total_cost", "sum"),
    ).reset_index()
    if "State" in risk_df.columns:
        cost_by_site = cost_by_site.merge(risk_df[["site_code", "State"]].drop_duplicates("site_code"), on="site_code", how="left")
    if "affiliate" in risk_df.columns:
        cost_by_site = cost_by_site.merge(risk_df[["site_code", "affiliate"]].drop_duplicates("site_code"), on="site_code", how="left")
    cost_by_site = cost_by_site.sort_values("total_cost", ascending=False)
    cost_by_site = cost_by_site[cost_by_site["site_code"].notna() & (cost_by_site["site_code"] != "")]

    if not cost_by_site.empty:
        if has_costs and cost_by_site["total_cost"].sum() > 0:
            st.plotly_chart(_style_chart(px.bar(cost_by_site.head(25), x="site_code", y="total_cost", title="Est. Replacement Cost by Site", color="total_cost", color_continuous_scale="Teal")), use_container_width=True)
        site_disp = cost_by_site.copy()
        for c in site_disp.columns:
            if "cost" in str(c).lower():
                site_disp[c] = site_disp[c].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "")
        st.dataframe(site_disp.head(50), use_container_width=True, height=450)
    else:
        st.info("No site data available.")

    st.divider()

    # --- Highest Price + Highest Turnover ---
    st.markdown("#### Highest Price + Highest Turnover")
    st.caption("Items scoring high on BOTH cost and device volume. Prioritize where spend and replacement volume are greatest.")
    at_risk_agg = at_risk.groupby("model").agg(device_count=("hostname", "count"), total_cost=("total_cost", "sum")).reset_index()
    at_risk_agg = at_risk_agg[at_risk_agg["model"].notna() & (at_risk_agg["model"] != "")]
    if not at_risk_agg.empty:
        at_risk_agg["avg_cost"] = np.where(at_risk_agg["device_count"] > 0, at_risk_agg["total_cost"] / at_risk_agg["device_count"], 0)
        at_risk_agg["cost_pct"] = at_risk_agg["total_cost"].rank(pct=True)
        at_risk_agg["count_pct"] = at_risk_agg["device_count"].rank(pct=True)
        at_risk_agg["price_turnover_score"] = at_risk_agg["cost_pct"] * at_risk_agg["count_pct"]
        price_turnover = at_risk_agg.nlargest(25, "price_turnover_score").copy()
        if "Repl Device" in risk_df.columns:
            repl_dict = risk_df[["model", "Repl Device"]].drop_duplicates("model").set_index("model")["Repl Device"].to_dict()
            price_turnover["Repl Device"] = price_turnover["model"].map(lambda m: repl_dict.get(m, ""))
        st.markdown("**By model (at-risk devices)**")
        pt_cols = ["model", "device_count", "total_cost", "avg_cost"]
        if "Repl Device" in price_turnover.columns:
            pt_cols.append("Repl Device")
        pt_disp = price_turnover[pt_cols]
        pt_disp = pt_disp.rename(columns={"device_count": "turnover (count)", "total_cost": "total cost ($)", "avg_cost": "avg cost ($)"})
        for c in pt_disp.columns:
            if "cost" in str(c).lower() or c == "total cost ($)":
                pt_disp[c] = pt_disp[c].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else str(x))
        st.dataframe(pt_disp, use_container_width=True, height=350)
        if has_costs and at_risk_agg["total_cost"].sum() > 0:
            fig_pt = px.scatter(at_risk_agg, x="device_count", y="total_cost", size="total_cost", hover_name="model", color="price_turnover_score", color_continuous_scale="Reds", title="Price vs Turnover — Top-right = Highest Both (prioritize)")
            fig_pt.update_layout(xaxis_title="Turnover (device count)", yaxis_title="Total Cost ($)")
            st.plotly_chart(_style_chart(fig_pt), use_container_width=True)
    # Same for sites
    pt_site = at_risk.groupby("site_code").agg(device_count=("hostname", "count"), total_cost=("total_cost", "sum")).reset_index()
    pt_site = pt_site[pt_site["site_code"].notna() & (pt_site["site_code"] != "")]
    if not pt_site.empty:
        pt_site["cost_pct"] = pt_site["total_cost"].rank(pct=True)
        pt_site["count_pct"] = pt_site["device_count"].rank(pct=True)
        pt_site["price_turnover_score"] = pt_site["cost_pct"] * pt_site["count_pct"]
        pt_site_top = pt_site.nlargest(25, "price_turnover_score").copy()
        if "State" in at_risk.columns:
            pt_site_top = pt_site_top.merge(at_risk[["site_code", "State"]].drop_duplicates("site_code"), on="site_code", how="left")
        st.markdown("**By site (at-risk devices)**")
        pt_site_disp = pt_site_top[["site_code", "device_count", "total_cost"] + (["State"] if "State" in pt_site_top.columns else [])]
        pt_site_disp = pt_site_disp.rename(columns={"device_count": "turnover (count)", "total_cost": "total cost ($)"})
        for c in pt_site_disp.columns:
            if "cost" in str(c).lower():
                pt_site_disp[c] = pt_site_disp[c].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else str(x))
        st.dataframe(pt_site_disp, use_container_width=True, height=350)

    st.divider()

    # --- Where to Prioritize (Q6: ranked investment) ---
    st.markdown("#### Where to Prioritize Refresh Investment")
    st.caption("Ranked by priority score (risk_score × log(1+cost)). Past EoL + high cost = invest first. Exceptions (Decom) excluded.")
    if "State" in at_risk.columns and "priority_score" in at_risk.columns and not at_risk.empty:
        invest_by_state = at_risk.groupby("State").agg(
            device_count=("hostname", "count"),
            total_priority=("priority_score", "sum"),
        ).reset_index().sort_values("total_priority", ascending=False).head(10)
        invest_by_state["rank"] = range(1, len(invest_by_state) + 1)
        st.markdown("**Top states by investment priority**")
        st.dataframe(invest_by_state[["rank", "State", "device_count", "total_priority"]].rename(columns={"total_priority": "priority_score_sum"}), use_container_width=True, height=280)

    # --- Top Priority Table (risk × cost) ---
    st.markdown("#### Top 25 Priority Devices (Risk Score × Cost)")
    priority_df = at_risk.nlargest(25, "priority_score")
    disp_cols = ["State", "affiliate", "site_code", "hostname", "device_type", "model", "Repl Device", "risk_tier", "risk_score", "Days_to_EoS", "Days_to_EoL"]
    if has_costs:
        disp_cols.extend(["total_cost", "Material Cost", "Labor Cost", "Device Cost"])
    disp_cols = [c for c in disp_cols if c in priority_df.columns]
    st.dataframe(priority_df[disp_cols].head(25), use_container_width=True, height=420)

    st.download_button("Download risk & cost data (CSV)", data=risk_df.to_csv(index=False).encode("utf-8"), file_name="risk_cost_analysis.csv", mime="text/csv")

# -----------------------------
# GEO BUNDLING TAB
# -----------------------------
with tab_geo:
    st.markdown("### Geo Bundling")
    st.markdown("**Are there sites within a specified radius to lifecycle together?** Yes. The map and table below show sites grouped by distance — clusters indicate sites that can be bundled for joint refresh planning.")
    st.caption("Sites within your selected radius (1–50 mi) are clustered using haversine distance. Each site uses a single coordinate (mean if duplicates). Adjust the Bundling Radius in the sidebar to see different groupings.")

    if filtered.empty:
        st.warning("No devices match the current filters. Try adjusting State, Affiliate, or other filters in the sidebar.")
    elif "latitude" not in filtered.columns or "longitude" not in filtered.columns:
        st.warning(
            "Latitude/Longitude columns are missing. Enable geo enrichment and ensure the Excel exists at the given path "
            "so we can pull SOLID-Loc coordinates."
        )
    elif filtered["latitude"].isna().all() and filtered["longitude"].isna().all():
        st.warning(
            "Latitude/Longitude are missing. Enable geo enrichment and ensure the Excel exists at the given path "
            "(e.g. `data/raw/UAInnovateDataset-SoCo.xlsx` or full path like `C:\\Users\\...\\UAInnovateDataset-SoCo.xlsx`). "
            "The CSV pipeline also embeds geo when run with a valid Excel—try **Run Pipeline from Excel** then **Reload Data**."
        )
    else:
        st.caption("This view aggregates devices to unique sites (one lat/lon per site) and clusters nearby sites within the selected radius. **Change the Bundling Radius in the sidebar** (1, 5, 10, 25, or 50 mi) to see different groupings. Use max_dist_mi to verify cluster tightness.")

        # Build a site-level view from filtered devices
        site_view = (
            filtered.dropna(subset=["latitude", "longitude"])
            .groupby(["site_code", "State", "affiliate", "county", "latitude", "longitude"], dropna=False)
            .agg(device_count=("hostname", "count"))
            .reset_index()
        )

        # Merge in counts from the pre-filtered CSV subsets (past/approach/unknown/exceptions) by site
        site_view = site_view.merge(
            site_counts[["site_code", "expired_count", "approaching_count", "unknown_count", "exception_count"]],
            on="site_code",
            how="left",
        )
        for c in ["expired_count", "approaching_count", "unknown_count", "exception_count"]:
            if c in site_view.columns:
                site_view[c] = site_view[c].fillna(0).astype(int)

        # KPIs
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Sites with coords", f"{len(site_view):,}")
        a2.metric("Devices in view", f"{int(site_view['device_count'].sum()):,}")
        a3.metric("Radius (mi)", f"{radius_mi}")
        a4.metric("Past EoL devices", f"{int(site_view['expired_count'].sum()):,}")

        st.divider()

        # Map of sites
        st.markdown("#### Site Map")
        map_fig = px.scatter_mapbox(
            site_view,
            lat="latitude",
            lon="longitude",
            size="device_count",
            hover_name="site_code",
            hover_data={
                "State": True,
                "affiliate": True,
                "county": True,
                "device_count": True,
                "expired_count": True,
                "approaching_count": True,
                "unknown_count": True,
                "exception_count": True,
            },
            zoom=4,
        )
        map_fig.update_layout(
            mapbox_style="carto-positron",
            margin={"r": 10, "t": 10, "l": 10, "b": 10},
            height=500,
            font=CHART_THEME["font"],
        )
        st.plotly_chart(map_fig, use_container_width=True)

        st.divider()

        # Clustering
        clustered, cluster_summary = build_radius_clusters(
            sites_df=site_view.rename(columns={"site_code": "site_code"}).assign(
                expired_count=site_view["expired_count"],
                approaching_count=site_view["approaching_count"],
                unknown_count=site_view["unknown_count"],
                exception_count=site_view["exception_count"],
            ).rename(columns={"site_code": "site_code"}),
            radius_mi=float(radius_mi),
        )

        st.markdown(f"#### Bundle Candidate Clusters (within {radius_mi} mi radius — largest first)")
        st.dataframe(cluster_summary.head(50), use_container_width=True, height=520)

        st.download_button(
            "Download cluster summary (CSV)",
            data=cluster_summary.to_csv(index=False).encode("utf-8"),
            file_name="cluster_summary.csv",
            mime="text/csv"
        )

# -----------------------------
# EXCEPTIONS TAB
# -----------------------------
with tab_ex:
    st.markdown("### Exceptions")
    st.markdown("**How are exceptions captured and incorporated?** Exceptions (Decom sites) are flagged and **excluded from refresh/investment totals**. In-scope KPIs and prioritization do not include these devices.")
    st.caption("Decommissioned or omitted devices — review sites and devices outside project scope. These are not counted in Past EoL, At-Risk, or Cost Summary for planning.")

    if exceptions is None or exceptions.empty:
        st.info("No exceptions file loaded or exceptions dataset is empty.")
    else:
        ex = exceptions.copy()
        if "model" in ex.columns and "Device Model" in ex.columns:
            ex = ex.drop(columns=["model"], errors="ignore")
        ex = ex.rename(columns={
            "Host Name": "hostname",
            "Device Status": "device_status",
            "Device Type Standard": "device_type",
            "Device Model": "model",
            "Site Code Extracted": "site_code",
        })
        ex = coerce_numeric(ex, ["Days_to_EoL"])

        k1, k2, k3 = st.columns(3)
        k1.metric("Exceptions", f"{len(ex):,}")
        k2.metric("Exceptions Past EoL", f"{int((ex['Days_to_EoL'] < 0).sum() if 'Days_to_EoL' in ex.columns else 0):,}")
        k3.metric("Unique sites", f"{ex['site_code'].nunique():,}" if "site_code" in ex.columns else "—")

        st.divider()

        cols = [
            "State", "site_code", "hostname", "device_type", "model",
            "EoL_Date", "Days_to_EoL", "Exception_Flag", "Exception_Reason"
        ]
        cols = [c for c in cols if c in ex.columns]
        st.dataframe(ex[cols].sort_values(["State", "site_code", "hostname"]).reset_index(drop=True),
                     use_container_width=True, height=560)

        st.download_button(
            "Download exceptions (CSV)",
            data=ex.to_csv(index=False).encode("utf-8"),
            file_name="exceptions.csv",
            mime="text/csv"
        )

# -----------------------------
# DATA PIPELINE TAB
# -----------------------------
with tab_docs:
    st.markdown("### Data Pipeline")
    st.caption("How data flows from Excel into the dashboard.")

    st.markdown("#### Key Questions This Dashboard Answers")
    st.markdown("""
| Question | Where to Find |
|----------|---------------|
| **1. What equipment is approaching EoS/EoL — where is highest risk?** | Overview KPIs (Past/Approaching EoS & EoL); Cost & Risk → Highest Risk by Geography; Lifecycle tab (EoS & EoL views) |
| **2. Sites within a radius to lifecycle together?** | Geo Bundling tab — map + clusters within 1/5/10 mi |
| **3. Devices past lifecycle but still in production?** | Lifecycle tab → Past EoL |
| **4. How are exceptions incorporated?** | Exceptions tab; Overview shows In Scope vs Exceptions; totals exclude Decom |
| **5. Lifecycle vs support, security, cost?** | Cost & Risk → Lifecycle, Support, and Cost Correlation table |
| **6. Where to prioritize refresh/investment?** | Cost & Risk → Where to Prioritize (ranked states); Top 25 Priority Devices |
""")

    st.markdown("""
    **Process for loading data into the visualization:**

    1. **Input:** Excel workbook with sheets: NA, CatCtr, PrimeAP, PrimeWLC, SOLID, SOLID-Loc, ModelData, Decom.

    2. **Pipeline** (`ua-innovate-backend.py`): Filter active devices, standardize Device Type, CatCtr/Prime override NA duplicates, extract State/Site Code from host name, merge ModelData (lifecycle + costs), flag Decom exceptions.

    3. **Output:** CSVs in data/outputs/ (core, approaching EoL, past EoL, unknown, exceptions).

    4. **Dashboard:** Reads CSVs, enriches with geo from SOLID-Loc.

    **To load new data:** Use **Run Pipeline from Excel** in the sidebar, then **Reload Data**.
    """)
    st.markdown("**Exception handling:** Devices at decommissioned sites (Decom) are flagged and omitted from project scope.")

# st.caption("Tip: If geo doesn’t show, enable Geo Enrichment and ensure the Excel file exists at the specified path.")