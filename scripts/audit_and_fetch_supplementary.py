#!/usr/bin/env python3
"""
Great Powers Index (2024) - Targeted Harvester & Recency Vetting Engine
======================================================================
1. Loads target countries from `data/etf.duckdb` in READ-ONLY mode (no DB writes).
2. Fetches external data (WHO, PWT, BIS, WHR, and WB API).
3. Evaluates all indicators against your exact recency and coverage gates.
4. Outputs diagnostics to terminal and local CSV files.

Requirements:
    pip install country_converter duckdb pandas numpy requests openpyxl urllib3
"""

import io
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import country_converter as coco
import duckdb
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration & Vetting Parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("VettingHarvester")
logging.getLogger("country_converter").setLevel(logging.ERROR)

DATA_DIR = Path("./dalio_external_data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = "data/etf.duckdb"

# Exact configuration from your vetting pipeline
CFG = dict(
    min_coverage=0.76,  # share of active countries with any value
    min_recent_cov=0.70,  # share of active countries with a value in last `recent_years`
    recent_years=3.0,
    min_recent_grid=0.50,  # share of active country x recent-year grid filled
    min_grid=0.55,  # share of active country x year grid filled
    min_recent_obs=0,
    min_obs=0,
    min_distinct=1,
    max_mode_share=1.0,
    max_zero_pct=1.0,
    dyn_structural=0.10,
    dyn_dynamic=0.30,
    eval_start_year=2020,  # Matches your '2020:2026' World Bank query window
    eval_end_year=2026,
)

NON_COUNTRY_CODES = {
    "WLD",
    "AFE",
    "AFW",
    "ARB",
    "CSS",
    "CEB",
    "EAR",
    "EAS",
    "EAP",
    "TEA",
    "EMU",
    "ECS",
    "ECA",
    "TEC",
    "EUU",
    "FCV",
    "HPC",
    "IBD",
    "IBT",
    "IDB",
    "IDX",
    "IDA",
    "LTE",
    "LCN",
    "LAC",
    "TLA",
    "LDC",
    "LMY",
    "MEA",
    "MNA",
    "TMN",
    "MIC",
    "NAC",
    "OED",
    "OSS",
    "PSS",
    "PST",
    "PRE",
    "SST",
    "SAS",
    "TSA",
    "SSF",
    "SSA",
    "TSS",
    "EUR",
    "AMR",
    "AFR",
    "SEAR",
    "WPR",
    "GLOBAL",
}


def get_http_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "Dalio-Vetting-Audit/1.0"})
    return session


# 1. Target Universe Loader (Read-Only)
def load_target_universe(db_path: str = DB_PATH) -> tuple[set[str], Callable[[Any], str | None]]:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found at: {db_path}")

    # Read-only connection ensures data/etf.duckdb is untouched
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            """
            select *
            from silver.observations
            where family in ('country')
            """
        ).df()
    finally:
        con.close()

    df_target = df[["metric", "code"]].drop_duplicates()
    cc = coco.CountryConverter()
    df_target["iso3"] = cc.pandas_convert(series=df_target["code"], to="ISO3")

    target_iso3: set[str] = set(df_target["iso3"].dropna().unique())
    logger.info(f"Loaded {len(target_iso3)} unique target countries (DB opened in read-only mode).")

    iso_map = dict(zip(cc.data["ISO2"].dropna(), cc.data["ISO3"].dropna()))
    iso_map.update({"EL": "GRC", "UK": "GBR", "EU": "EMU"})
    memo: dict[str, str | None] = {}

    def resolve_iso3(val: Any) -> str | None:
        if val is None or pd.isna(val):
            return None
        raw = str(val).strip()
        if not raw:
            return None
        if raw in memo:
            return memo[raw]

        up = raw.upper()
        if up.startswith("WB_") or up in NON_COUNTRY_CODES:
            memo[raw] = None
            return None

        res = None
        if len(up) == 3 and up in target_iso3:
            res = up
        elif up in iso_map and iso_map[up] in target_iso3:
            res = iso_map[up]
        else:
            try:
                c_conv = cc.convert(names=raw, to="ISO3", not_found=None)
                if c_conv and c_conv in target_iso3:
                    res = c_conv
            except Exception:
                res = None

        memo[raw] = res
        return res

    return target_iso3, resolve_iso3


# 2. Targeted Fetchers
INDICATOR_LABELS = {
    "WHO.SUICIDE.RATE": "Suicide mortality rate (per 100,000 pop)",
    "WHO.UHC.INDEX": "UHC Service Coverage Index (0-100)",
    "PWT.HOURS.WEEKLY": "Average weekly hours worked per worker",
    "PWT.CAPITAL.STOCK": "Real capital stock at constant PPPs",
    "PWT.LABOR.SHARE": "Share of labor compensation in GDP",
    "BIS.CREDIT.GOVT.GDP": "Total credit to general government (% of GDP)",
    "BIS.CREDIT.PRIVATE.GDP": "Total credit to private non-financial sector (% of GDP)",
    "BIS.DEBT.SERVICE.RATIO": "Debt service ratio, private non-financial (% of income)",
    "WHR.CANTRIL.LADDER": "Reported life satisfaction (Cantril Ladder, 0-10)",
    "WHR.SOCIAL.SUPPORT": "Share reporting good social support network",
    "WB.VC.IHR.PSRC.P5": "Intentional homicides (per 100,000 people)",
    "WB.EN.ATM.PM25.MC.M3": "PM2.5 air pollution, mean annual exposure (mcg/m3)",
    "WB.GB.XPD.RSDV.GD.ZS": "Research and development expenditure (% of GDP)",
    "WB.SP.POP.SCIE.RD.P6": "Researchers in R&D (per million people)",
    "WB.CM.MKT.LCAP.GD.ZS": "Market capitalization of listed domestic companies (% of GDP)",
    "WB.SH.STA.SUIC.P5": "Suicide mortality rate (per 100,000 population)",
}


def fetch_all_supplementary(session: requests.Session, resolve_iso3: Callable) -> pd.DataFrame:
    records = []

    # --- A. WHO GHO API ---
    logger.info("Fetching WHO indicators...")
    who_endpoints = {
        "WHO.SUICIDE.RATE": "https://ghoapi.azureedge.net/api/MH_12",
        "WHO.UHC.INDEX": "https://ghoapi.azureedge.net/api/UHC_INDEX_REPORTED",
    }
    for code, url in who_endpoints.items():
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                for row in resp.json().get("value", []):
                    dim = row.get("Dim1")
                    if dim not in [None, "BTSX", "SEX_BTSX", "TOTAL", ""]:
                        continue
                    iso3 = resolve_iso3(row.get("SpatialDim"))
                    year = row.get("TimeDim")
                    val = row.get("NumericValue")
                    if iso3 and year and val is not None:
                        records.append(
                            {
                                "code": code,
                                "indicator_label": INDICATOR_LABELS[code],
                                "iso3": iso3,
                                "year": int(year),
                                "value": float(val),
                            }
                        )
        except Exception as e:
            logger.warning(f"Error fetching {code}: {e}")

    # --- B. Penn World Table 10.01 (DataverseNL API) ---
    logger.info("Fetching Penn World Table 10.01...")
    pwt_url = "https://dataverse.nl/api/access/datafile/354108"
    pwt_path = DATA_DIR / "pwt1001.xlsx"
    if not pwt_path.exists():
        try:
            r = session.get(pwt_url, timeout=60)
            if r.status_code == 200:
                with open(pwt_path, "wb") as f:
                    f.write(r.content)
        except Exception as e:
            logger.warning(f"Could not download PWT: {e}")

    if pwt_path.exists():
        try:
            pwt_df = pd.read_excel(pwt_path, sheet_name="Data")
            pwt_df["iso3"] = pwt_df["countrycode"].apply(resolve_iso3)
            pwt_df = pwt_df[pwt_df["iso3"].notna()]

            if "avh" in pwt_df.columns:
                pwt_df["PWT.HOURS.WEEKLY"] = pwt_df["avh"] / 52.0

            pwt_map = {
                "PWT.HOURS.WEEKLY": "PWT.HOURS.WEEKLY",
                "cn": "PWT.CAPITAL.STOCK",
                "labsh": "PWT.LABOR.SHARE",
            }
            for col, code in pwt_map.items():
                if col in pwt_df.columns:
                    sub = pwt_df[["iso3", "year", col]].dropna()
                    for _, r in sub.iterrows():
                        records.append(
                            {
                                "code": code,
                                "indicator_label": INDICATOR_LABELS[code],
                                "iso3": r["iso3"],
                                "year": int(r["year"]),
                                "value": float(r[col]),
                            }
                        )
        except Exception as e:
            logger.warning(f"Error parsing PWT: {e}")

    # --- C. BIS SDMX API v2 ---
    logger.info("Fetching BIS credit and debt service ratios...")
    bis_endpoints = {
        "BIS.CREDIT.GOVT.GDP": "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_TC/2.0/Q..G.A.M.770.A?format=csv",
        "BIS.CREDIT.PRIVATE.GDP": "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_TC/2.0/Q..P.A.M.770.A?format=csv",
        "BIS.DEBT.SERVICE.RATIO": "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_DSR/1.0/Q..N?format=csv",
    }
    for code, url in bis_endpoints.items():
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                raw = pd.read_csv(io.StringIO(resp.text))
                time_cols = [c for c in raw.columns if "TIME" in c.upper()]
                val_cols = [c for c in raw.columns if "OBS_VALUE" in c.upper() or "VALUE" in c.upper()]
                area_cols = [c for c in raw.columns if "BORROWERS_CTY" in c.upper() or "REF_AREA" in c.upper()]

                if time_cols and val_cols and area_cols:
                    t_col, v_col, a_col = time_cols[0], val_cols[0], area_cols[0]
                    for _, row in raw[[a_col, t_col, v_col]].dropna().iterrows():
                        iso3 = resolve_iso3(row[a_col])
                        if not iso3:
                            continue
                        t_str = str(row[t_col])
                        if len(t_str) >= 4 and t_str[:4].isdigit():
                            records.append(
                                {
                                    "code": code,
                                    "indicator_label": INDICATOR_LABELS[code],
                                    "iso3": iso3,
                                    "year": int(t_str[:4]),
                                    "value": float(row[v_col]),
                                }
                            )
        except Exception as e:
            logger.warning(f"Error fetching {code}: {e}")

    # --- D. World Happiness Report ---
    logger.info("Fetching World Happiness Report series...")
    whr_url = "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/World%20Happiness%20Report%20(2022)/World%20Happiness%20Report%20(2022).csv"
    try:
        resp = session.get(whr_url, timeout=25)
        if resp.status_code == 200:
            whr = pd.read_csv(io.StringIO(resp.text))
            ent_col = [c for c in whr.columns if "Entity" in c or "Country" in c][0]
            yr_col = [c for c in whr.columns if "Year" in c][0]
            lad_col = [c for c in whr.columns if "Ladder" in c or "Happiness" in c]
            sup_col = [c for c in whr.columns if "Social support" in c]

            for _, row in whr.iterrows():
                iso3 = resolve_iso3(row[ent_col])
                if not iso3:
                    continue
                year = int(row[yr_col])
                if lad_col and pd.notna(row[lad_col[0]]):
                    records.append(
                        {
                            "code": "WHR.CANTRIL.LADDER",
                            "indicator_label": INDICATOR_LABELS["WHR.CANTRIL.LADDER"],
                            "iso3": iso3,
                            "year": year,
                            "value": float(row[lad_col[0]]),
                        }
                    )
                if sup_col and pd.notna(row[sup_col[0]]):
                    records.append(
                        {
                            "code": "WHR.SOCIAL.SUPPORT",
                            "indicator_label": INDICATOR_LABELS["WHR.SOCIAL.SUPPORT"],
                            "iso3": iso3,
                            "year": year,
                            "value": float(row[sup_col[0]]),
                        }
                    )
    except Exception as e:
        logger.warning(f"Error fetching WHR: {e}")

    # --- E. World Bank API: Dropped Indicators ---
    logger.info("Fetching dropped indicators via World Bank API...")
    wb_codes = [
        "VC.IHR.PSRC.P5",
        "EN.ATM.PM25.MC.M3",
        "GB.XPD.RSDV.GD.ZS",
        "SP.POP.SCIE.RD.P6",
        "CM.MKT.LCAP.GD.ZS",
        "SH.STA.SUIC.P5",
    ]
    base_url = "https://api.worldbank.org/v2/country/all/indicator/{ind}?format=json&per_page=1000&date=2000:2026"

    for ind in wb_codes:
        full_code = f"WB.{ind}"
        page, total_pages = 1, 1
        while page <= total_pages:
            url = f"{base_url.format(ind=ind)}&page={page}"
            try:
                resp = session.get(url, timeout=45)
                if resp.status_code != 200:
                    break
                payload = resp.json()
                if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
                    break

                total_pages = int(payload[0].get("pages", 1))
                for entry in payload[1]:
                    val = entry.get("value")
                    cid = entry.get("countryiso3code") or entry.get("country", {}).get("id")
                    yr = entry.get("date")
                    if val is None or not cid or not yr:
                        continue
                    iso3 = resolve_iso3(cid)
                    if iso3:
                        records.append(
                            {
                                "code": full_code,
                                "indicator_label": INDICATOR_LABELS.get(full_code, ind),
                                "iso3": iso3,
                                "year": int(yr),
                                "value": float(val),
                            }
                        )
                page += 1
            except Exception as e:
                logger.warning(f"Error on WB {ind} page {page}: {e}")
                break

    df = pd.DataFrame(records)
    # Deduplicate and compute annual average for quarterly feeds (e.g. BIS)
    df = df.groupby(["code", "indicator_label", "iso3", "year"], as_index=False)["value"].mean()
    return df


# 3. Exact Vetting Diagnostics Pipeline
def run_vetting_diagnostics(panel: pd.DataFrame, target_universe_size: int):
    logger.info(f"Evaluating panel against window {CFG['eval_start_year']}:{CFG['eval_end_year']}...")

    # Filter strictly to the evaluation window
    eval_panel = panel[(panel["year"] >= CFG["eval_start_year"]) & (panel["year"] <= CFG["eval_end_year"])].copy()
    eval_panel["value"] = pd.to_numeric(eval_panel["value"], errors="coerce")
    eval_panel = eval_panel.dropna(subset=["value"])

    active_iso = eval_panel["iso3"].unique()
    n_active = len(active_iso) if len(active_iso) > 0 else target_universe_size
    years = sorted(eval_panel["year"].unique()) if not eval_panel.empty else [CFG["eval_start_year"]]
    n_years = len(years)

    recent_cut = max(years) - CFG["recent_years"] + 1
    n_recent_years = sum(y >= recent_cut for y in years)

    def to_scale(v):
        if len(v) > 2 and (v > 0).all() and v.skew() > 1:
            return np.log(v)
        return v

    eval_panel["t"] = eval_panel.groupby("code")["value"].transform(to_scale)

    def diagnose(g):
        by = g.groupby("iso3")["t"]
        means = by.mean()
        between_var = means.var()
        within_var = by.var().mean()
        total_sd = g["t"].std()
        between_sd = np.sqrt(between_var) if pd.notna(between_var) else np.nan
        within_sd = np.sqrt(within_var) if pd.notna(within_var) else np.nan
        denom = (between_var or 0) + (within_var if pd.notna(within_var) else 0)
        recent = g[g["year"] >= recent_cut]
        recent_obs = recent.groupby("iso3").size().mean() if len(recent) else np.nan

        # Also capture the actual max year with observations across all countries
        latest_reported_year = g["year"].max()

        return pd.Series(
            {
                "latest_year": latest_reported_year,
                "coverage": g["iso3"].nunique() / n_active,
                "recent_cov": recent["iso3"].nunique() / n_active if n_recent_years > 0 else 0.0,
                "grid_cov": len(g) / (n_active * n_years),
                "recent_grid_cov": len(recent) / (n_active * n_recent_years) if n_recent_years > 0 else 0.0,
                "obs_per_country": by.size().mean(),
                "recent_obs_per_country": recent_obs,
                "n_distinct": g["value"].nunique(),
                "mode_share": g["value"].value_counts(normalize=True).iloc[0],
                "zero_pct": (g["value"] == 0).mean(),
                "logged": bool((g["t"] != g["value"]).any()),
                "total_sd": total_sd,
                "between_sd": between_sd,
                "within_sd": within_sd,
                "dynamism": within_sd / (between_sd + 1e-9) if pd.notna(within_sd) else np.nan,
                "icc": between_var / denom if denom > 0 else np.nan,
            }
        )

    diag = eval_panel.groupby("code").apply(diagnose, include_groups=False)
    diag.insert(0, "indicator", diag.index.map(INDICATOR_LABELS))

    # Apply your exact gating conditions
    gates = pd.DataFrame(
        {
            "coverage": diag["coverage"] >= CFG["min_coverage"],
            "recent": diag["recent_cov"] >= CFG["min_recent_cov"],
            "non_degenerate": (
                (diag["n_distinct"] >= CFG["min_distinct"])
                & (diag["mode_share"] <= CFG["max_mode_share"])
                & (diag["zero_pct"] <= CFG["max_zero_pct"])
                & (diag["total_sd"] > 0)
            ),
            "recent_grid": diag["recent_grid_cov"] >= CFG["min_recent_grid"],
            "recent_obs": diag["recent_obs_per_country"] >= CFG["min_recent_obs"],
            "grid": diag["grid_cov"] >= CFG["min_grid"],
            "obs": diag["obs_per_country"] >= CFG["min_obs"],
            "moves": diag["within_sd"] > 0,
        }
    )

    LEVEL_GATES = ["coverage", "recent", "non_degenerate"]
    CHANGE_GATES = LEVEL_GATES + ["recent_grid", "recent_obs", "grid", "obs", "moves"]

    diag["level_ok"] = gates[LEVEL_GATES].all(axis=1)
    diag["change_ok"] = gates[CHANGE_GATES].all(axis=1)

    diag["role"] = pd.cut(
        diag["dynamism"],
        bins=[-np.inf, CFG["dyn_structural"], CFG["dyn_dynamic"], np.inf],
        labels=["structural", "mixed", "dynamic"],
    )

    # Export to CSV
    panel_out = DATA_DIR / "supplementary_vetted_panel.csv"
    diag_out = DATA_DIR / "vetting_diagnostics.csv"

    eval_panel.to_csv(panel_out, index=False)
    diag.to_csv(diag_out)

    logger.info(f"✓ Panel output saved to:       {panel_out}")
    logger.info(f"✓ Diagnostics output saved to: {diag_out}")

    # Display Vetting Results to Terminal
    display_cols = [
        "indicator",
        "latest_year",
        "coverage",
        "recent_cov",
        "grid_cov",
        "recent_grid_cov",
        "dynamism",
        "role",
        "level_ok",
        "change_ok",
    ]
    formatted = diag[display_cols].copy()
    for col in ["coverage", "recent_cov", "grid_cov", "recent_grid_cov"]:
        formatted[col] = (formatted[col] * 100).round(1).astype(str) + "%"
    formatted["dynamism"] = formatted["dynamism"].round(3)

    print("\n" + "=" * 115)
    print(
        f"VETTING AUDIT: EVALUATION WINDOW {CFG['eval_start_year']}-{CFG['eval_end_year']} (Recent Cut: {recent_cut})"
    )
    print(
        f"Active Target Countries: {n_active} | min_coverage: {CFG['min_coverage'] * 100:.0f}% | min_recent_cov: {CFG['min_recent_cov'] * 100:.0f}%"
    )
    print("=" * 115)
    print(formatted.to_string())
    print("=" * 115 + "\n")

    print(f"Total Evaluated: {len(diag)}")
    print(f"Passed LEVEL_OK:  {diag['level_ok'].sum()} (Can be used for static/cross-sectional levels)")
    print(f"Passed CHANGE_OK: {diag['change_ok'].sum()} (Can be used for annual changes / momentum)")
    failed_recent = diag[~gates["recent"]].index.tolist()
    if failed_recent:
        print(f"\nDisqualified by recent_cov (< {CFG['min_recent_cov'] * 100:.0f}% in last {CFG['recent_years']} yrs):")
        for code in failed_recent:
            print(
                f"  ❌ {code:<25} -> Latest Year: {diag.loc[code, 'latest_year']} (Recent Cov: {diag.loc[code, 'recent_cov'] * 100:.1f}%)"
            )


# 4. Main Entry Point
def main():
    target_iso3, resolve_iso3 = load_target_universe(DB_PATH)
    session = get_http_session()

    raw_panel = fetch_all_supplementary(session, resolve_iso3)
    if raw_panel.empty:
        logger.error("No supplementary data retrieved.")
        return

    run_vetting_diagnostics(raw_panel, len(target_iso3))


if __name__ == "__main__":
    main()
