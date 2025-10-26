import os, sqlite3
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from fastapi import APIRouter, Query, HTTPException

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))
DB_FILE  = os.path.join(DATA_DIR, "analytics.db")

router = APIRouter()

# -------------------- helpers --------------------

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    # return rows as dicts
    conn.row_factory = lambda cur, row: {cur.description[i][0]: row[i] for i in range(len(row))}
    return conn

def _region_where_and_params(region: Optional[str]) -> Tuple[str, List[Any]]:
    """
    Region optional filter:
      - region provided => WHERE region=?
      - region missing/empty => WHERE region IS NULL OR region=''
    """
    if region and region.strip():
        return "region = ?", [region.strip()]
    return "(region IS NULL OR region='')", []

def _read_df(client: str, region: Optional[str], workspace: str) -> pd.DataFrame:
    ws = (workspace or "").strip().upper()
    where_region, params_region = _region_where_and_params(region)

    q = f"""
        SELECT client, region, workspace, parameter, ts_utc, value
        FROM timeseries_data
        WHERE client=? AND {where_region} AND UPPER(workspace)=?
        ORDER BY ts_utc
    """
    params = [client] + params_region + [ws]

    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql_query(q, conn, params=params)

    if df.empty:
        return df

    # normalize dtypes
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_utc"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    return df

def _daily_stats_for_param(df_param: pd.DataFrame) -> Dict[str, Any]:
    """
    Returns per-day stats for a single parameter:
    { "daily":[{date, avg, max:{time,value}, min:{time,value}}...] }
    """
    if df_param.empty:
        return {"daily": []}

    df = df_param.copy()
    df["date"] = df["ts_utc"].dt.date

    out = []
    for d, g in df.groupby("date", sort=True):
        g = g.sort_values("ts_utc")
        avg_val = float(g["value"].mean())
        idx_max = g["value"].idxmax()
        idx_min = g["value"].idxmin()
        out.append({
            "date": str(d),
            "avg": round(avg_val, 2),
            "max": {
                "time": g.loc[idx_max, "ts_utc"].isoformat(),
                "value": float(g.loc[idx_max, "value"]),
            },
            "min": {
                "time": g.loc[idx_min, "ts_utc"].isoformat(),
                "value": float(g.loc[idx_min, "value"]),
            },
        })
    return {"daily": out}

def _rolling_days_summary(daily_rows: list) -> Dict[str, Any]:
    if not daily_rows:
        return {}
    overall_avg = round(sum(r["avg"] for r in daily_rows) / len(daily_rows), 2)
    max_day = max(daily_rows, key=lambda r: r["max"]["value"])
    min_day = min(daily_rows, key=lambda r: r["min"]["value"])
    return {
        "avg": overall_avg,
        "max_day": {"date": max_day["date"], "value": float(max_day["max"]["value"])},
        "min_day": {"date": min_day["date"], "value": float(min_day["min"]["value"])},
    }

def _monthly_stats_for_param(df_param: pd.DataFrame) -> Dict[str, Any]:
    """
    Returns per-month avg rows for a single parameter:
    { "months":[{month, avg}...] }
    """
    if df_param.empty:
        return {"months": []}
    df = df_param.copy()
    df["month"] = df["ts_utc"].dt.to_period("M").astype(str)
    gb = df.groupby("month", sort=True)["value"].mean().round(2)
    months = [{"month": m, "avg": float(v)} for m, v in gb.items()]
    return {"months": months}

def _rolling_months_summary(month_rows: list) -> Dict[str, Any]:
    if not month_rows:
        return {}
    overall_avg = round(sum(r["avg"] for r in month_rows) / len(month_rows), 2)
    max_m = max(month_rows, key=lambda r: r["avg"])
    min_m = min(month_rows, key=lambda r: r["avg"])
    return {
        "overall_avg": overall_avg,
        "max_month": max_m,
        "min_month": min_m,
    }

# -------------------- core --------------------

def compute_stats(
    client: str,
    region: Optional[str],
    workspace: str,
    basis: str = "days",   # "days" | "months"
    n: int = 1,
    anchor: str = "latest", # kept for compatibility; currently unused
) -> Dict[str, Any]:
    """
    Returns per-parameter stats limited to the **last n distinct days/months** present in DB.
    - If region is provided -> filter by that region
    - If not provided -> use rows with region IS NULL/'' (clients without regions)
    """
    df = _read_df(client, region, workspace)
    if df.empty:
        return {"ok": True, "note": "No data for selected range"}

    out: Dict[str, Any] = {
        "ok": True,
        "client": client,
        "region": region,
        "workspace": workspace,
        "basis": basis,
        "n": int(n),
        "by_parameter": {},
    }

    if basis == "months":
        n = max(1, min(6, int(n)))
        df["month_per"] = df["ts_utc"].dt.to_period("M")
        distinct = sorted(df["month_per"].unique())
        last_n = distinct[-n:] if len(distinct) >= n else distinct
        df = df[df["month_per"].isin(last_n)]
        if df.empty:
            return {"ok": True, "note": "No data for selected range"}
        out["range"] = {"from": df["ts_utc"].min().isoformat(), "to": df["ts_utc"].max().isoformat()}

        for p, g in df.groupby("parameter"):
            mon = _monthly_stats_for_param(g)
            mon["months"] = [r for r in mon["months"] if pd.Period(r["month"]) in last_n]
            roll = _rolling_months_summary(mon["months"])
            out["by_parameter"][p] = {
                f"{n}m": mon,
                f"rolling_{n}m": roll
            }
        return out

    # default: basis == "days"
    n = max(1, min(7, int(n)))
    df["date"] = df["ts_utc"].dt.date
    distinct_days = sorted(df["date"].unique())
    last_n_days = distinct_days[-n:] if len(distinct_days) >= n else distinct_days
    df = df[df["date"].isin(last_n_days)]
    if df.empty:
        return {"ok": True, "note": "No data for selected range"}

    out["range"] = {"from": df["ts_utc"].min().isoformat(), "to": df["ts_utc"].max().isoformat()}

    for p, g in df.groupby("parameter"):
        daily = _daily_stats_for_param(g)["daily"]
        daily = [d for d in sorted(daily, key=lambda r: r["date"])]
        roll = _rolling_days_summary(daily)
        out["by_parameter"][p] = {
            f"{n}d": {"daily": daily},
            f"rolling_{n}d": roll
        }

    return out

# -------------------- API: /api/stats --------------------

@router.get("/api/stats")
def api_stats(
    client: str = Query(...),
    workspace: str = Query(...),
    region: Optional[str] = Query(None, description="Provide when client has regions; omit when client has no regions."),
    basis: str = Query("days", description="'days' or 'months'"),
    n: int = Query(2, ge=1, le=6, description="How many days or months"),
    anchor: str = Query("latest"),
):
    if basis not in ("days", "months"):
        raise HTTPException(status_code=400, detail="basis must be 'days' or 'months'")
    return compute_stats(client, region, workspace, basis=basis, n=n, anchor=anchor)

# -------------------- API: /api/display/latest --------------------
# (latest snapshot for given client/[region]/workspace)

@router.get("/api/display/latest")
def api_display_latest(
    client: str = Query(...),
    workspace: str = Query(...),
    region: Optional[str] = Query(None, description="Provide when client has regions; omit when client has no regions."),
    mode: str = Query("aligned", description="'aligned' or 'per-parameter'")
):
    """
    aligned       => all parameters at the single, global latest ts_utc
    per-parameter => each parameter's own latest ts_utc
    Region filter logic:
      - region provided => that exact region
      - region omitted  => rows where region IS NULL or ''
    """
    if mode not in ("aligned", "per-parameter"):
        return {"ok": False, "reason": "mode must be 'aligned' or 'per-parameter'"}

    where_region, params_region = _region_where_and_params(region)

    conn = _conn()
    try:
        if mode == "aligned":
            # 1) find global latest ts_utc
            row = conn.execute(f"""
                SELECT MAX(ts_utc) AS max_ts
                FROM timeseries_data
                WHERE client=? AND {where_region} AND UPPER(workspace)=?
            """, [client] + params_region + [workspace.strip().upper()]).fetchone()
            max_ts = row and row.get("max_ts")
            if not max_ts:
                return {"ok": True, "mode": mode, "count": 0, "rows": []}

            # 2) fetch all parameters at that exact timestamp
            rows = conn.execute(f"""
                SELECT parameter, value, ts_utc, sheet_name, received_utc, message_id
                FROM timeseries_data
                WHERE client=? AND {where_region} AND UPPER(workspace)=? AND ts_utc=?
                ORDER BY parameter COLLATE NOCASE
            """, [client] + params_region + [workspace.strip().upper(), max_ts]).fetchall()

            latest_date = max_ts.split(" ")[0] if " " in max_ts else max_ts[:10]
            return {
                "ok": True,
                "mode": mode,
                "client": client, "region": region, "workspace": workspace,
                "latest_ts": max_ts,
                "latest_date": latest_date,
                "count": len(rows),
                "rows": rows
            }

        # mode == per-parameter
        rows = conn.execute(f"""
            SELECT t.parameter, t.value, t.ts_utc, t.sheet_name, t.received_utc, t.message_id
            FROM timeseries_data t
            JOIN (
              SELECT parameter, MAX(ts_utc) AS max_ts
              FROM timeseries_data
              WHERE client=? AND {where_region} AND UPPER(workspace)=?
              GROUP BY parameter
            ) m
              ON t.parameter=m.parameter AND t.ts_utc=m.max_ts
            WHERE t.client=? AND {where_region} AND UPPER(t.workspace)=?
            ORDER BY t.parameter COLLATE NOCASE
        """, [client] + params_region + [workspace.strip().upper()] + [client] + params_region + [workspace.strip().upper()]).fetchall()

        latest_ts = None
        if rows:
            latest_ts = max(r["ts_utc"] for r in rows if r.get("ts_utc"))
        latest_date = latest_ts.split(" ")[0] if latest_ts and " " in latest_ts else (latest_ts[:10] if latest_ts else None)

        return {
            "ok": True,
            "mode": mode,
            "client": client, "region": region, "workspace": workspace,
            "latest_ts": latest_ts,
            "latest_date": latest_date,
            "count": len(rows),
            "rows": rows
        }

    finally:
        try: conn.close()
        except: pass
