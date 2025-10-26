from __future__ import annotations

import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# ------------------------------
# Basic helpers
# ------------------------------

def _find_ts_col(df: pd.DataFrame) -> str:
    """Timestamp column ko detect kare: case-insensitive match on common names."""
    for col in df.columns:
        key = str(col).strip().lower()
        if key in ("timestamp", "time", "datetime", "ts", "date_time"):
            return col
    # try exact first column fallback if looks datetime-like
    c0 = str(df.columns[0])
    return c0


def _normalize_ts_utc(df: pd.DataFrame, ts_col: str = "") -> pd.DataFrame:
    """Timestamp ko tz-aware UTC ISO string banata hai (YYYY-MM-DD HH:MM:SS+00:00)."""
    df = df.copy()
    if not ts_col:
        ts_col = _find_ts_col(df)

    # Parse tolerant + coerce
    ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    # If timezone-naive parsed without utc=True, force UTC
    if ts.dt.tz is None:
        ts = pd.to_datetime(df[ts_col], errors="coerce").dt.tz_localize("UTC")
    df["ts_utc"] = ts.dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S%z")  # +0000
    # make RFC-like with colon in offset (e.g., +00:00)
    df["ts_utc"] = df["ts_utc"].str.replace(r"(\+|\-)(\d{2})(\d{2})$", lambda m: f"{m.group(1)}{m.group(2)}:{m.group(3)}", regex=True)

    df = df.dropna(subset=["ts_utc"]).sort_values("ts_utc").drop_duplicates(subset=["ts_utc"], keep="last")
    return df


def _prefix_columns(df: pd.DataFrame, prefix: str, exclude: Iterable[str]) -> pd.DataFrame:
    """Non-excluded columns ko prefix kare—name collisions avoid karne ke liye."""
    df = df.copy()
    ren = {}
    excl = {c.lower() for c in exclude}
    for c in df.columns:
        if c.lower() in excl:
            continue
        ren[c] = f"{prefix}_{c}"
    return df.rename(columns=ren)


def _select_sheet_name(xls: pd.ExcelFile, hint: str) -> Optional[str]:
    """Sheet selection by substring hint (case-insensitive). Not found -> None."""
    h = (hint or "").lower()
    for nm in xls.sheet_names:
        if h and h in nm.lower():
            return nm
    return None


# ------------------------------
# Public API
# ------------------------------

def parse_workbook(
    path: str | Path,
    sheet_hints: Sequence[str] = ("A", "B", "C"),
) -> Dict[str, pd.DataFrame]:
    """
    Workbook (XLSX/CSV) ko parse karke sheets ka cleaned DataFrame dict return.
    - Excel: given hints ke basis pe matching sheets parse karta hai; jo na mile, skip.
    - CSV: single 'CSV' sheet ke naam se load.
    Har sheet:
      - Timestamp detect & normalize -> 'ts_utc'
      - Timestamp sort + dedupe
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")

    sheets: Dict[str, pd.DataFrame] = {}

    if p.suffix.lower() in (".xlsx", ".xls", ".xlsm", ".xlsb"):
        try:
            xls = pd.ExcelFile(p)
        except Exception:
            xls = pd.ExcelFile(p, engine="openpyxl")

        for h in sheet_hints:
            nm = _select_sheet_name(xls, h)
            if nm is None:
                continue
            df = xls.parse(nm)
            if df.empty:
                continue
            df = _normalize_ts_utc(df, _find_ts_col(df))
            # keep ts_utc + value columns (non-empty)
            if "ts_utc" not in df.columns:
                continue
            # Drop completely empty columns (except ts_utc)
            keep_cols = [c for c in df.columns if c == "ts_utc" or df[c].notna().any()]
            sheets[nm] = df[keep_cols].reset_index(drop=True)

        # If no hint matched, fallback to first sheet
        if not sheets and xls.sheet_names:
            nm = xls.sheet_names[0]
            df = xls.parse(nm)
            if not df.empty:
                df = _normalize_ts_utc(df, _find_ts_col(df))
                keep_cols = [c for c in df.columns if c == "ts_utc" or df[c].notna().any()]
                sheets[nm] = df[keep_cols].reset_index(drop=True)

    elif p.suffix.lower() == ".csv":
        df = pd.read_csv(p)
        if not df.empty:
            df = _normalize_ts_utc(df, _find_ts_col(df))
            keep_cols = [c for c in df.columns if c == "ts_utc" or df[c].notna().any()]
            sheets["CSV"] = df[keep_cols].reset_index(drop=True)
    else:
        raise ValueError(f"Unsupported file type: {p.suffix}")

    if not sheets:
        raise ValueError("No usable sheets/rows parsed from workbook")

    return sheets


def merge_sheets_on_ts(
    sheets: Dict[str, pd.DataFrame],
    order_for_merge: Optional[Sequence[str]] = None
) -> pd.DataFrame:
    """
    Multiple sheets ko 'ts_utc' par outer-join merge karta hai.
    Column collisions avoid karne ke liye har sheet ko apne naam se prefix kiya jata hai.
    """
    if not sheets:
        raise ValueError("No sheets provided")

    # Decide merge order
    names = list(sheets.keys())
    if order_for_merge:
        ordered = [nm for nm in order_for_merge if nm in sheets] + [nm for nm in names if nm not in order_for_merge]
    else:
        ordered = names

    # Prefix non-ts columns
    prepped: Dict[str, pd.DataFrame] = {}
    for nm in ordered:
        df = sheets[nm]
        prepped[nm] = _prefix_columns(df, prefix=nm, exclude=("ts_utc",))

    # Merge
    merged = None
    for nm in ordered:
        df = prepped[nm]
        if merged is None:
            merged = df
        else:
            merged = merged.merge(df, on="ts_utc", how="outer")
    merged = merged.sort_values("ts_utc").reset_index(drop=True)
    return merged


def wide_to_long_timeseries(
    df_wide: pd.DataFrame,
    sheet_name_map: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """
    Wide merged frame (prefixed columns) ko long format me convert karta hai:
      columns => ts_utc, parameter, value, sheet_name
    'sheet_name' ko prefix se infer karta hai (prefix == original sheet name).
    'sheet_name_map' se display name map kar sakte ho (optional).
    """
    if "ts_utc" not in df_wide.columns:
        raise ValueError("ts_utc column missing in merged dataframe")

    value_cols = [c for c in df_wide.columns if c != "ts_utc"]
    if not value_cols:
        # only timestamps
        return pd.DataFrame(columns=["ts_utc", "parameter", "value", "sheet_name"])

    long = df_wide.melt(id_vars=["ts_utc"], var_name="parameter_raw", value_name="value")
    # Drop fully NaN values
    long = long[long["value"].notna()].copy()

    # Extract prefix as sheet_name, rest as parameter
    def split_param(s: str) -> Tuple[str, str]:
        if "_" in s:
            pref, rest = s.split("_", 1)
            return pref, rest
        return "SHEET", s  # fallback

    parts = long["parameter_raw"].apply(split_param)
    long["sheet_name"] = parts.apply(lambda x: x[0])
    long["parameter"] = parts.apply(lambda x: x[1])

    if sheet_name_map:
        long["sheet_name"] = long["sheet_name"].map(lambda k: sheet_name_map.get(k, k))

    # Clean types: ensure numeric where possible
    def _to_float_or_none(x):
        try:
            if isinstance(x, str) and not x.strip():
                return None
            v = float(x)
            if math.isnan(v):
                return None
            return v
        except Exception:
            return None

    long["value"] = long["value"].apply(_to_float_or_none)

    # Final columns
    out = long[["ts_utc", "parameter", "value", "sheet_name"]].reset_index(drop=True)
    return out


def parse_and_merge_xlsx(
    xlsx_path: str,
    hints: Sequence[str] = ("A", "B", "C")
) -> pd.DataFrame:
    """
    Backward-compatible helper (tumhari original function ka drop-in).
    - Excel ko parse karta hai
    - Selected sheets ko merge karta hai (outer join on ts_utc)
    - Prefixed wide dataframe return karta hai
    """
    sheets = parse_workbook(xlsx_path, sheet_hints=hints)
    merged = merge_sheets_on_ts(sheets, order_for_merge=list(hints))
    return merged


# ------------------------------
# Records for DB insert
# ------------------------------

def to_timeseries_records(
    long_df: pd.DataFrame,
    *,
    client: str,
    region: Optional[str],
    workspace: Optional[str],
    message_id: Optional[str] = None,
    received_utc_iso: Optional[str] = None,
) -> List[dict]:
    """
    Long-format DF -> TimeseriesData insertable dicts.
    Expected columns in long_df: ts_utc, parameter, value, sheet_name
    """
    req = {"ts_utc", "parameter", "value", "sheet_name"}
    missing = req - set(map(str, long_df.columns))
    if missing:
        raise ValueError(f"Missing columns in long_df: {missing}")

    recs: List[dict] = []
    for _, row in long_df.iterrows():
        recs.append(
            {
                "client": client,
                "region": region,
                "workspace": workspace,
                "sheet_name": str(row["sheet_name"]),
                "parameter": str(row["parameter"]),
                "ts_utc": str(row["ts_utc"]),
                "value": row["value"],
                "message_id": message_id,
                "received_utc": pd.to_datetime(received_utc_iso, errors="coerce", utc=True)
                if received_utc_iso else None,
            }
        )
    return recs
