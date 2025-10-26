from __future__ import annotations

import os, sqlite3
from datetime import datetime, timezone
from typing import Optional, List, Any, Dict

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

# ---- DB helper ----
def _get_conn() -> sqlite3.Connection:
    BASE_DIR = os.path.dirname(__file__)
    DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))
    DB_FILE  = os.path.join(DATA_DIR, "analytics.db")
    conn = sqlite3.connect(DB_FILE)
    # dict rows
    conn.row_factory = lambda cur, row: {cur.description[i][0]: row[i] for i in range(len(row))}
    return conn

# ---- region filter helper (region optional) ----
def _region_where_and_params(region: Optional[str]) -> tuple[str, list[Any]]:
    """
    If region provided -> 'region=?'
    If region omitted/blank -> '(region IS NULL OR region='')'
    """
    if region and region.strip():
        return "region = ?", [region.strip()]
    return "(region IS NULL OR region='')", []

# ---- time slot config (UTC) ----
# Allowed user slots in UTC: 01:30, 09:30, 17:30 (exact match in DB)
TIME_SLOTS = {"01:30:00", "09:30:00", "17:30:00"}

def _normalize_slot(s: str) -> Optional[str]:
    """
    Accept '09:30', '09:30:00', '930', '9:30' etc. and return 'HH:MM:SS'
    only if it is one of TIME_SLOTS.
    """
    if not s:
        return None
    raw = s.strip()

    # "0930" -> "09:30:00"
    if len(raw) == 4 and raw.isdigit():
        raw = raw[:2] + ":" + raw[2:]

    # "9:30" or "09:30" -> "09:30:00"
    if ":" in raw and len(raw) <= 5:
        parts = raw.split(":")
        if len(parts) == 2:
            hh = parts[0].zfill(2)
            mm = parts[1].zfill(2)
            raw = f"{hh}:{mm}:00"

    # If already HH:MM:SS, validate against TIME_SLOTS
    return raw if raw in TIME_SLOTS else None

def _iso_for_db(dt: datetime) -> str:
    # Our DB stores "YYYY-MM-DD HH:MM:SS+00:00"
    return dt.replace(microsecond=0).isoformat().replace("T", " ")

@router.get("/api/display/by-date")
def display_by_date(
    client: str = Query(...),
    workspace: str = Query(...),
    region: Optional[str] = Query(
        None,
        description="Provide when client has regions; omit when client has no regions."
    ),
    date: str = Query(..., description="YYYY-MM-DD (calendar date, UTC)"),
    time_slot: str = Query(..., description="One of 01:30, 09:30, 17:30 (UTC)"),
    sheet: Optional[str] = Query(None, description="Optional sheet/tab filter"),
):
    """
    Return exact point-in-time snapshot for given client/[region]/workspace at
    **date + time_slot (UTC)**. If not found, 404 with a friendly JSON.

    Region logic:
      - region provided -> exact region
      - region omitted  -> region IS NULL or ''
    """
    # normalize inputs
    client = (client or "").strip()
    workspace_raw = (workspace or "").strip()
    workspace_up = workspace_raw.upper()
    sheet_raw = (sheet or "").strip() if sheet else None

    if not client or not workspace_up:
        return JSONResponse(status_code=400, content={"ok": False, "reason": "client and workspace are required"})

    # validate date
    try:
        d = datetime.strptime(date.strip(), "%Y-%m-%d").date()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "reason": "Invalid date. Use YYYY-MM-DD."})

    # validate time slot
    slot = _normalize_slot(time_slot)
    if not slot:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "reason": "Invalid time_slot. Allowed: 01:30, 09:30, 17:30 (UTC).",
                "allowed": sorted(TIME_SLOTS),
            },
        )

    # build aware UTC datetime -> "YYYY-MM-DD HH:MM:SS+00:00"
    hh, mm, ss = map(int, slot.split(":"))
    dt_utc = datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=timezone.utc)
    ts_db = _iso_for_db(dt_utc)

    # region clause
    where_region, params_region = _region_where_and_params(region)

    # query
    q = f"""
      SELECT parameter, value, ts_utc, sheet_name, message_id
      FROM timeseries_data
      WHERE client=? AND {where_region} AND UPPER(workspace)=?
        AND ts_utc=?
    """
    args: List[Any] = [client] + params_region + [workspace_up, ts_db]
    if sheet_raw:
        q += " AND UPPER(sheet_name)=?"
        args.append(sheet_raw.upper())

    conn = _get_conn()
    try:
        rows = conn.execute(q + " ORDER BY parameter COLLATE NOCASE", args).fetchall()
    finally:
        try: conn.close()
        except: pass

    if not rows:
        return JSONResponse(
            status_code=404,
            content={
                "ok": False,
                "reason": "No data for given timestamp",
                "client": client,
                "region": region,
                "workspace": workspace_raw,
                "sheet": sheet_raw,
                "ts": dt_utc.isoformat(),
                "hint": "Check that this exact date+time exists in DB (UTC) or try another slot.",
            },
        )

    out_rows: List[Dict[str, Any]] = []
    for r in rows:
        v = r.get("value")
        try:
            v = float(v)
        except Exception:
            pass
        out_rows.append({
            "parameter": r.get("parameter"),
            "value": v,
            "ts_utc": r.get("ts_utc"),
            "sheet_name": r.get("sheet_name"),
            "message_id": r.get("message_id"),
        })

    return {
        "ok": True,
        "mode": "by-date-slot",
        "client": client,
        "region": region,
        "workspace": workspace_raw,
        "sheet": sheet_raw,
        "date": date,
        "time_slot": slot,          # normalized HH:MM:SS
        "ts": dt_utc.isoformat(),   # ISO Z
        "rows": out_rows,
        "count": len(out_rows),
    }
