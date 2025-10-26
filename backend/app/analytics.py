import os, re, sqlite3
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Tuple, List

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))
os.makedirs(DATA_DIR, exist_ok=True)

DB_FILE = os.path.join(DATA_DIR, "analytics.db")

# ------------ TABLE + INDEX INIT ------------

def _ensure_table():
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        # Base table without sender_email
        cur.execute("""
        CREATE TABLE IF NOT EXISTS timeseries_data (
            client TEXT,
            region TEXT,
            workspace TEXT,
            parameter TEXT,
            ts_utc TEXT,          -- "YYYY-MM-DD HH:MM:SS+00:00"
            value REAL,
            message_id TEXT,
            received_utc TEXT,    -- ISO datetime string
            sheet_name TEXT
        )
        """)
        # Helpful read indexes
        cur.execute("""CREATE INDEX IF NOT EXISTS ix_tsd_lookup_pair_ts
                       ON timeseries_data (client, region, workspace, ts_utc)""")
        cur.execute("""CREATE INDEX IF NOT EXISTS ix_tsd_param_ts
                       ON timeseries_data (client, region, parameter, ts_utc)""")
        # Uniqueness (partial unique indexes)
        # 1) Regionful rows unique on (client, region, sheet_name, parameter, ts_utc)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_tsd_with_region
        ON timeseries_data (client, region, sheet_name, parameter, ts_utc)
        WHERE region IS NOT NULL
        """)
        # 2) Regionless rows unique on (client, sheet_name, parameter, ts_utc)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_tsd_without_region
        ON timeseries_data (client, sheet_name, parameter, ts_utc)
        WHERE region IS NULL
        """)
        conn.commit()

# ------------ TIMESTAMP HELPERS ------------

_ISO_NO_COLON_RE = re.compile(r"(\+|\-)(\d{2})(\d{2})$")

def _iso_utc(dt: pd.Timestamp | datetime | str | None) -> Optional[str]:
    """
    Normalize to 'YYYY-MM-DD HH:MM:SS+00:00' (UTC). Returns None if invalid.
    """
    if dt is None:
        return None
    try:
        ts = pd.to_datetime(dt, utc=True, errors="coerce")
    except Exception:
        return None
    if ts is None or pd.isna(ts):
        return None
    s = ts.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S%z")  # +0000
    # insert colon in offset
    return _ISO_NO_COLON_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}:{m.group(3)}", s)

# ------------ HEADER PARSE (from columns like 08/21-0930 etc.) ------------

def _parse_header(col: str, year_hint: int) -> Optional[pd.Timestamp]:
    """
    Accepts headers like 'MM/DD-HHMM' or 'MM/DD HHMM' or 'MM/DD_HHMM'.
    Returns timezone-aware UTC Timestamp or None.
    """
    txt = str(col).strip()
    m = re.match(r"(\d{1,2})/(\d{1,2})[-_ ]?(\d{1,2})(\d{2})$", txt)
    if not m:
        return None
    mm, dd, hh, mi = m.groups()
    try:
        dt = pd.to_datetime(f"{year_hint}-{mm}-{dd} {hh}:{mi}", format="%Y-%m-%d %H:%M")
    except Exception:
        return None
    # mark UTC
    return pd.Timestamp(dt).tz_localize("UTC")

def _infer_year(received_iso: Optional[str]) -> int:
    ts = _iso_utc(received_iso)
    if ts:
        try:
            return pd.to_datetime(ts, utc=True).year
        except Exception:
            pass
    return datetime.now(timezone.utc).year

# ------------ NORMALIZE ONE SHEET ------------

def _normalize_sheet(
    df: pd.DataFrame,
    *,
    client: str,
    region: Optional[str],
    workspace: str,
    message_id: str,
    received_iso: Optional[str],
    sheet_name: str
) -> pd.DataFrame:
    """
    Convert a single wide sheet to long format with columns:
      client, region, workspace, parameter, ts_utc, value, message_id, received_utc, sheet_name
    """
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    if df.empty:
        return pd.DataFrame()

    # first col = parameter
    param_col = df.columns[0]
    df[param_col] = df[param_col].astype(str).str.strip()
    df = df[df[param_col] != ""]
    df.rename(columns={param_col: "parameter"}, inplace=True)

    # parse headers into UTC timestamps
    year_hint = _infer_year(received_iso)
    col_ts = {}
    for col in df.columns[1:]:
        ts = _parse_header(str(col), year_hint)
        if ts is not None:
            col_ts[str(col)] = ts

    usable = ["parameter"] + list(col_ts.keys())
    df = df[[c for c in usable if c in df.columns]]
    if df.shape[1] <= 1:
        return pd.DataFrame()

    long_df = df.melt(id_vars=["parameter"], var_name="col", value_name="value")
    # numeric coercion
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df = long_df.dropna(subset=["value"])

    # map to ts_utc (ISO)
    long_df["ts_utc"] = long_df["col"].map(col_ts)
    long_df = long_df.dropna(subset=["ts_utc"])
    long_df["ts_utc"] = long_df["ts_utc"].apply(_iso_utc)

    # attach meta
    long_df["client"] = client
    long_df["region"] = region if (region and str(region).strip()) else None
    long_df["workspace"] = workspace
    long_df["message_id"] = message_id
    long_df["received_utc"] = _iso_utc(received_iso)
    long_df["sheet_name"] = sheet_name

    return long_df[["client","region","workspace","parameter","ts_utc","value","message_id","received_utc","sheet_name"]]

# ------------ APPEND TO SQLITE ------------

def append_timeseries(df_long: pd.DataFrame) -> Tuple[int, int]:
    """
    Upsert (INSERT OR REPLACE) long-frame into timeseries_data.
    Returns: (rows_written, unique_parameters_written)
    """
    if df_long is None or df_long.empty:
        return (0, 0)

    _ensure_table()
    rows: List[tuple] = [
        (
            r.client,
            r.region,
            r.workspace,
            r.parameter,
            str(r.ts_utc),
            float(r.value),
            r.message_id,
            str(r.received_utc) if r.received_utc else None,
            r.sheet_name
        )
        for r in df_long.itertuples(index=False)
    ]

    with sqlite3.connect(DB_FILE) as conn:
        conn.executemany("""
        INSERT OR REPLACE INTO timeseries_data
        (client, region, workspace, parameter, ts_utc, value, message_id, received_utc, sheet_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()

    return (len(rows), df_long["parameter"].nunique())

# ------------ PUBLIC: INGEST WHOLE EXCEL ------------

def ingest_excel(
    file_path: str,
    *,
    client: str,
    region: Optional[str],
    message_id: str,
    received_iso: Optional[str]
) -> tuple[int, int]:
    """
    Parse all sheets of the Excel and append normalized timeseries rows.
    Returns: (rows_written, unique_parameters_written)
    """
    _ensure_table()
    xls = pd.ExcelFile(file_path)
    all_dfs = []

    for sh in xls.sheet_names:
        ws = str(sh).strip().upper()
        df = pd.read_excel(xls, sh, dtype=object)
        long_df = _normalize_sheet(
            df,
            client=client,
            region=region,
            workspace=ws,
            message_id=message_id,
            received_iso=received_iso,
            sheet_name=sh
        )
        if not long_df.empty:
            all_dfs.append(long_df)

    if not all_dfs:
        return (0, 0)

    final_df = pd.concat(all_dfs, ignore_index=True)
    return append_timeseries(final_df)
