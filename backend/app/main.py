from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Any, Optional, Dict
from datetime import datetime
import sqlite3
from .analytics import DB_FILE
import os
import json
import pandas as pd
from fastapi.responses import StreamingResponse
import asyncio
import threading
from glob import glob
import re

# --- Local config / wrappers ---
from .config import EXCEL_LOCAL_PATH, FRONTEND_BASE, ADMIN_SECRET, load_clients, save_clients
from .mapi_ps import (
    list_recent,
    fetch_save_for_pair,
    load_allowed_senders,     # derives from clients.json if explicit list missing
    lookup_sender_for_pair,   # resolves exact SMTP for (client, region)
)
from .analytics import ingest_excel
from .stats import router as stats_router
from .display_by_date import router as display_by_date_router
from .config import AUTO_FETCH_ENABLED, AUTO_FETCH_INTERVAL_MINUTES
from .auto_fetcher import auto_fetch_loop

# ----------------- small helpers -----------------
def _safe_dir(name: str) -> str:
    """Creates a filesystem-safe directory name."""
    s = str(name or "").strip()
    if not s:
        return "__no_region__"
    # Remove characters that are invalid in Windows/Linux directory names
    s = re.sub(r'[\\/:*?"<>|]+', '_', s)
    return s.strip(" .")

def _best_key(d: Dict[str, Any]) -> Optional[str]:
    return d.get("internetMessageId") or d.get("searchKeyHex") or d.get("entryId")

def current_role(request: Request) -> str:
    return request.cookies.get("role", "viewer")

def require_admin(role: str = Depends(current_role)):
    if role != "admin":
        raise HTTPException(status_code=403, detail="Admins only.")

def _compose_subject(client: str, region: Optional[str], keyword: Optional[str]) -> str:
    parts = []
    if client: parts.append(client.strip())
    if region and region.strip(): parts.append(region.strip())
    if keyword and keyword.strip(): parts.append(keyword.strip())
    return " ".join(parts)

# Add this function to set current Excel path
def set_current_excel_path(path: str):
    global CURRENT_EXCEL_PATH
    CURRENT_EXCEL_PATH = path

# ============== FastAPI app + CORS ==============
app = FastAPI(title="Mail Crawler API (MAPI)")
app.include_router(stats_router)
app.include_router(display_by_date_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_BASE, "http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add auto-fetcher startup event
@app.on_event("startup")
def startup_event():
    print("DEBUG: Startup event triggered")
    if AUTO_FETCH_ENABLED:
        print("DEBUG: Auto-fetch is enabled, starting thread...")
        try:
            thread = threading.Thread(target=auto_fetch_loop, daemon=True)
            thread.start()
            print(f"Auto-fetcher started (interval: {AUTO_FETCH_INTERVAL_MINUTES} minutes)")
        except Exception as e:
            print(f"Failed to start auto-fetcher: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("DEBUG: Auto-fetch is disabled")

@app.get("/api/excel/find-and-list-sheets", tags=["Excel Viewer"])
def api_excel_find_and_list_sheets(
    client: str = Query(..., description="Client to search for."),
    region: Optional[str] = Query(None, description="Region to search for (if applicable).")
):
    """
    Finds the latest Excel/CSV file for a given client/region pair,
    sets it as the current file for the viewer, and returns its sheet names.
    """
    global CURRENT_EXCEL_PATH
    # --- Resolve directory using the same logic as the ingest process ---
    region_folder = _safe_dir(region) if region and region.strip() else "__no_region__"
    search_root = os.path.join(EXCEL_LOCAL_PATH, _safe_dir(client), region_folder)

    # --- Find all Excel or CSV files recursively ---
    files = glob(os.path.join(search_root, "**", "*.xls*"), recursive=True) + \
            glob(os.path.join(search_root, "**", "*.csv"), recursive=True)

    # If the list is empty, it means no files were found in any subdirectories.
    if not files:
        # IMPROVED: The error message now includes the exact path that was searched.
        absolute_path = os.path.abspath(search_root)
        error_detail = (
            f"No Excel or CSV files found for '{client}/{region_folder}'. "
            f"Searched recursively in: {absolute_path}"
        )
        raise HTTPException(status_code=404, detail=error_detail)

    # --- Pick the most recently modified file ---
    try:
        latest_file = max(files, key=os.path.getmtime)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error determining the latest file: {e}")

    # Set this file as the one to be read by other Excel Viewer endpoints
    CURRENT_EXCEL_PATH = latest_file
    file_name = os.path.basename(latest_file)
    ext = os.path.splitext(file_name)[1].lower()

    # --- Read sheet names from the file ---
    try:
        if ext == ".csv":
            # For a CSV, the "sheet" is just the filename itself
            return {"ok": True, "sheets": [file_name], "file_found": latest_file}
        else:
            # For Excel files, read the actual sheet names
            xls = pd.ExcelFile(latest_file, engine='openpyxl')
            return {"ok": True, "sheets": xls.sheet_names, "file_found": latest_file}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read sheets from '{file_name}': {e}")

# ============== Real-time Notifications (SSE) ==============

async def notification_generator(request: Request):
    """
    This is a placeholder for a real-time notification stream.
    It sends a 'ping' every 15 seconds to keep the connection alive.
    A full implementation would use a message queue (like Redis or asyncio.Queue)
    to push real events (e.g., 'new_mail_found') to connected clients.
    """
    while True:
        # Check if the client has disconnected
        if await request.is_disconnected():
            break

        # Send a keep-alive message (ping)
        yield f"data: {json.dumps({'event': 'ping'})}\n\n"
        await asyncio.sleep(15)

@app.get("/api/notifications/stream")
async def stream_notifications(request: Request):
    """
    Endpoint for Server-Sent Events (SSE) to notify the frontend.
    """
    return StreamingResponse(notification_generator(request), media_type="text/event-stream")


# ============== Static dashboard ==============
BASE_DIR = os.path.dirname(__file__)
STATIC_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "static"))
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index_path):
        return HTMLResponse("<h3>index.html not found at backend/static/index.html</h3>", status_code=404)
    with open(index_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/dashboard", status_code=307)

# ============== Health ==============
@app.get("/health")
def health():
    # Local Outlook MAPI ka koi online token nahi hota; assume available if PS can run.
    return {"status": "ok", "source": "mapi", "outlook_connected": True}

# ============== Clients/Regions (single source: clients.json) ==============
class ClientsResponse(BaseModel):
    clients: List[str]

class RegionsResponse(BaseModel):
    client: str
    regions: List[str]

class AddClientRequest(BaseModel):
    name: str
    # sender: region-less client ke liye REQUIRED, regionful client ke liye optional fallback
    sender: Optional[str] = None
    # has_regions: true => client has regions; false => region-less client
    has_regions: bool = False

class AddRegionRequest(BaseModel):
    client: str
    region: str
    # per-region sender REQUIRED
    sender: str

# Define a Pydantic model for the response
class CleanupResponse(BaseModel):
    ok: bool
    deleted_rows: int
    reason: Optional[str] = None

@app.get("/api/options/clients", response_model=ClientsResponse)
def list_clients_dynamic():
    store = load_clients() or {}
    return ClientsResponse(clients=sorted(list(store.keys())))

@app.get("/api/options/store", dependencies=[Depends(require_admin)])
def view_store():
    return load_clients() or {}

@app.get("/api/options/regions", response_model=RegionsResponse)
def list_regions_dynamic(client: str = Query(..., description="Client name")):
    store = load_clients() or {}
    meta = store.get(client) or {}
    regions = []
    if isinstance(meta, dict):
        regs = meta.get("regions")
        if isinstance(regs, dict):
            regions = list(regs.keys())
        elif isinstance(regs, list):
            regions = list(regs)
    elif isinstance(meta, list):
        regions = list(meta)
    return RegionsResponse(client=client, regions=sorted(regions))

@app.post("/api/options/client", response_model=ClientsResponse, dependencies=[Depends(require_admin)])
def add_client(req: AddClientRequest):
    name = (req.name or "").strip()
    if not name:
        raise HTTPException(400, "Client name required")

    store = load_clients() or {}
    node = store.get(name)
    if not isinstance(node, dict):
        node = {}

    if not req.has_regions:
        # region-less => sender REQUIRED
        if not req.sender:
            raise HTTPException(400, "sender required for region-less client")
        node["sender"] = req.sender.strip()
        node["regions"] = {}  # keep dict for uniform shape
    else:
        # client has regions
        if not isinstance(node.get("regions"), dict):
            node["regions"] = {}
        if req.sender:
            node["sender"] = req.sender.strip()

    store[name] = node
    save_clients(store)
    return ClientsResponse(clients=sorted(store.keys()))

@app.post("/api/options/region", response_model=RegionsResponse, dependencies=[Depends(require_admin)])
def add_region(req: AddRegionRequest):
    client = (req.client or "").strip()
    region = (req.region or "").strip()
    sender = (req.sender or "").strip()

    if not client or not region:
        raise HTTPException(400, "Client and region required")
    if not sender:
        raise HTTPException(400, "sender required for region")

    store = load_clients() or {}
    node = store.get(client)
    if not isinstance(node, dict):
        node = {}

    if not isinstance(node.get("regions"), dict):
        node["regions"] = {}
    node["regions"][region] = sender
    store[client] = node
    save_clients(store)

    return RegionsResponse(client=client, regions=sorted(node["regions"].keys()))

@app.delete("/api/options/client", dependencies=[Depends(require_admin)])
def delete_client(client: str = Query(...)):
    store = load_clients() or {}
    if client in store:
        del store[client]
        save_clients(store)
    return {"ok": True}

@app.delete("/api/options/region", dependencies=[Depends(require_admin)])
def delete_region(client: str = Query(...), region: str = Query(...)):
    store = load_clients() or {}
    node = store.get(client)
    if isinstance(node, dict) and isinstance(node.get("regions"), dict):
        if region in node["regions"]:
            del node["regions"][region]
            save_clients(store)
    return {"ok": True}

# ============== Admin session APIs ==============
class AdminLoginReq(BaseModel):
    secret: str

@app.post("/auth/admin/login")
def admin_login(req: AdminLoginReq):
    if req.secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")
    resp = JSONResponse({"status": "ok", "role": "admin"})
    resp.set_cookie("role", "admin", httponly=True, samesite="lax", max_age=8*3600)
    return resp

@app.post("/auth/admin/logout")
def admin_logout():
    resp = JSONResponse({"status": "ok"})
    resp.delete_cookie("role")
    return resp

@app.get("/auth/role")
def get_role(role: str = Depends(current_role)):
    return {"role": role, "outlook_connected": True}

@app.post("/api/admin/cleanup/old-data", 
          response_model=CleanupResponse, 
          dependencies=[Depends(require_admin)],
          tags=["Admin"])
def cleanup_old_data(
    months: int = Query(6, ge=1, le=120, description="Delete data older than this many months.")
):
    """
    **Admin-only**: Deletes timeseries data older than a specified number of months.
    The comparison is performed on the `ts_utc` column, which stores the timestamp
    for each data point.
    """
    if not os.path.exists(DB_FILE):
        raise HTTPException(status_code=404, detail=f"Database file not found at {DB_FILE}")

    # The `timeseries_data.ts_utc` column is TEXT in "YYYY-MM-DD HH:MM:SS+00:00" format.
    # SQLite's date functions can correctly parse and compare this format.
    cutoff_period = f"-{months} months"
    
    try:
        # Connect to the SQLite database
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            # Formulate the DELETE query using a parameterized value for safety
            # This query deletes rows where the timestamp is earlier than the calculated cutoff date.
            query = f"DELETE FROM timeseries_data WHERE ts_utc < date('now', ?)"
            
            # Execute the query
            cursor.execute(query, (cutoff_period,))
            
            # Get the number of rows that were deleted
            deleted_count = cursor.rowcount
            
            # Commit the transaction to save the changes
            conn.commit()

        # Optionally, run VACUUM to reclaim disk space after a large delete.
        # This can be slow, so consider running it during off-peak hours.
        # with sqlite3.connect(DB_FILE) as conn:
        #     conn.execute("VACUUM;")

        return CleanupResponse(
            ok=True,
            deleted_rows=deleted_count,
            reason=f"Successfully deleted {deleted_count} rows older than {months} months."
        )

    except sqlite3.Error as e:
        # Handle potential database errors
        raise HTTPException(status_code=500, detail=f"A database error occurred: {e}")
    except Exception as e:
        # Handle other unexpected errors
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

# ============== Recent Mails (MAPI via PowerShell) ==============
class MailWithAtt(BaseModel):
    id: Optional[str] = None
    subject: Optional[str] = None
    receivedDateTime: Optional[str] = None
    from_name: Optional[str] = None
    from_email: Optional[str] = None
    hasAttachments: bool = False
    attachments: List[str] = []
    internetMessageId: Optional[str] = None
    entryId: Optional[str] = None
    searchKeyHex: Optional[str] = None
    conversationIdHex: Optional[str] = None

class MailWithAttList(BaseModel):
    items: List[MailWithAtt]

# helper: flatten allowed senders from clients.json
def _allowed_from_clients_json() -> list[str]:
    store = load_clients() or {}
    out = set()
    for _, meta in store.items():
        if isinstance(meta, dict):
            em = str(meta.get("sender") or "").strip().lower()
            if em: out.add(em)
            regs = meta.get("regions", {})
            if isinstance(regs, dict):
                for _, v in regs.items():
                    emr = str(v or "").strip().lower()
                    if emr: out.add(emr)
        elif isinstance(meta, list):
            # legacy shape: no senders at client-level
            pass
    return sorted(out)

from collections import Counter


@app.get("/mail/recent-with-attachments")
def mail_recent_with_attachments(
    limit: int = Query(20, ge=1, le=50),
    hours: int = Query(240, ge=1, le=720),
    client: Optional[str] = Query(None, description="Scope to this client's sender(s)"),
    sender: Optional[str] = Query(None, description="Debug: force a single SMTP sender"),
    only_with_attachments: bool = Query(True, description="Return only rows that have attachments"),
    debug: bool = Query(False)
):
    try:
        # Build allow list: sender > client > all
        if sender:
            allowed = [sender.strip().lower()]
        else:
            allowed = _allowed_from_clients_json_for(client)

        data = list_recent(limit=limit, hours=hours, allowed_senders=allowed)

        if not data.get("ok"):
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": data.get("error", "List failed"), "__debug": data.get("__debug")}
            )

        items = []
        for it in (data.get("items") or []):
            row = {
                "id": _best_key(it),
                "subject": it.get("subject"),
                "from_name": it.get("from"),
                "from_email": it.get("fromEmail"),
                "receivedDateTime": it.get("receivedDateTime"),
                "hasAttachments": bool(it.get("hasAttachments")),
                "attachments": [str(a) for a in (it.get("attachments") or []) if str(a).strip()],
                "internetMessageId": it.get("internetMessageId"),
                "entryId": it.get("entryId"),
                "searchKeyHex": it.get("searchKeyHex"),
                "conversationIdHex": it.get("conversationIdHex"),
            }
            items.append(row)

        # Keep only rows with attachments if requested
        if only_with_attachments:
            items = [r for r in items if r["hasAttachments"] and r["attachments"]]

        # Enrich debug
        dbg = data.get("__debug", {})
        if debug:
            counts = Counter([(i.get("from_email") or "").lower() for i in items])
            dbg.update({
                "result_sender_counts": dict(counts),
                "allowed_from_clients_json": _allowed_from_clients_json_for(client),
                "client_scoped": bool(client),
                "forced_sender": sender.strip().lower() if sender else None,
                "only_with_attachments": only_with_attachments,
            })

        return {"ok": True, "items": items, "total": len(items), "__debug": dbg if debug else None}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

# ============== Ingest by Client+Region+Subject (PowerShell) ==============
class PairPreviewRequest(BaseModel):
    client: str
    region: str                                # region-less clients: pass "" (empty)
    subject_hint: Optional[str] = None         # keyword from user
    hours: Optional[int] = 240
    


class PairPreviewResponse(BaseModel):
    ok: bool
    reason: Optional[str] = None
    mail_subject: Optional[str] = None
    saved_path: Optional[str] = None
    file_type: Optional[str] = None
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    internetMessageId: Optional[str] = None
    entryId: Optional[str] = None
    searchKeyHex: Optional[str] = None
    conversationIdHex: Optional[str] = None
    bestMessageKey: Optional[str] = None

# Track latest ingested file in-memory for Excel viewer endpoints
CURRENT_EXCEL_PATH: Optional[str] = None

@app.post("/api/ingest/by-client-region", response_model=PairPreviewResponse)
def api_ingest_by_client_region(req: PairPreviewRequest):
    """
    1) MAPI (PowerShell) se latest matching mail ka Excel/CSV download
    2) Preview banaye (columns + rows)
    3) analytics.ingest_excel se DB me append + dedupe
    """
    global CURRENT_EXCEL_PATH
    try:
        # --- STRICT VALIDATION: only configured clients/regions may ingest ---
        store = load_clients() or {}
        node = store.get(req.client)

        if not node:
            # client is not configured -> hard stop
            raise HTTPException(status_code=400, detail=f"Unknown client '{req.client}'. Please add it in options before ingest.")

        # normalize shapes
        regions_meta = None
        if isinstance(node, dict):
            regions_meta = node.get("regions")
        else:
            # legacy list shape means regions list without senders (not allowed for ingest)
            regions_meta = node if isinstance(node, list) else None

        # Case A: client has regions (dict non-empty) -> region required AND must be valid
        if isinstance(regions_meta, dict) and len(regions_meta) > 0:
            if not (req.region and req.region.strip()):
                raise HTTPException(status_code=400, detail=f"Region is required for client '{req.client}'.")
            if req.region not in regions_meta:
                raise HTTPException(status_code=400, detail=f"Region '{req.region}' is not configured for client '{req.client}'.")
            # Optional but recommended: must have sender for this region
            region_sender = (regions_meta.get(req.region) or "").strip()
            if not region_sender:
                raise HTTPException(status_code=409, detail=f"Sender not configured for client '{req.client}' region '{req.region}'. Add sender first.")

        # Case B: client is region-less -> region must be empty, and client-level sender must exist
        else:
            if req.region and req.region.strip():
                raise HTTPException(status_code=400, detail=f"Client '{req.client}' does not have regions; do not pass region.")
            client_sender = (node.get("sender") or "").strip() if isinstance(node, dict) else ""
            if not client_sender:
                raise HTTPException(status_code=409, detail=f"Sender not configured for client '{req.client}'. Add sender first.")


        # (1) resolve sender for this pair (region-less => region="")
        sender_email = _sender_from_clients_store(req.client, req.region)
        # (2) auto-compose subject: CLIENT [REGION] KEYWORD
        subj = _compose_subject(req.client, req.region or "", req.subject_hint or "")
        # (3) fetch via PS with sender filter (if available)
        res = fetch_save_for_pair(
            req.client,
            req.region or "",
            req.subject_hint or "",
            req.hours or 240,
            sender=sender_email
        )

        ok = bool(res.get("ok") or res.get("saved"))
        saved_path = res.get("saved_path") or res.get("path")
        if not ok:
            return PairPreviewResponse(ok=False, reason=res.get("reason") or "No matching mail/attachment")
        if not saved_path or not os.path.exists(saved_path):
            raise HTTPException(500, "Saved file not found after fetch")

        CURRENT_EXCEL_PATH = saved_path

        # IDs & metadata
        internet_id = res.get("internetMessageId")
        entry_id    = res.get("entryId")
        search_key  = res.get("searchKeyHex")
        conv_hex    = res.get("conversationIdHex")
        best_key    = _best_key(res)

        mail_subject = res.get("mail_subject") or res.get("subject") or ""
        received_iso = res.get("receivedDateTime") or res.get("received_iso") or None

        # Preview
        ext = os.path.splitext(saved_path)[1].lower()
        try:
            if ext == ".csv":
                df = pd.read_csv(saved_path, dtype=object)
                ftype = "csv"
            else:
                try:
                    xls = pd.ExcelFile(saved_path)
                except Exception:
                    xls = pd.ExcelFile(saved_path, engine="openpyxl")
                first = xls.sheet_names[0]
                df = pd.read_excel(xls, first, dtype=object)
                ftype = "excel"
            cols = [str(c) for c in df.columns]
            rows = df.where(pd.notna(df), None).values.tolist()
        except Exception as e:
            raise HTTPException(500, f"File parse failed: {e}")

        # Ingest to analytics DB
        try:
            rows_written, uniq_params = ingest_excel(
                saved_path,
                client=req.client,
                region=req.region or "",
                message_id=best_key or "<unknown>",
                received_iso=received_iso,
            )
            print(f"[ingest] wrote {rows_written} rows, {uniq_params} unique parameters")
        except Exception as e:
            print("timeseries ingest failed:", e)
            rows_written, uniq_params = 0, 0

        return PairPreviewResponse(
            ok=True,
            mail_subject=mail_subject,
            saved_path=saved_path,
            file_type=ftype,
            columns=cols,
            rows=rows,
            internetMessageId=internet_id,
            entryId=entry_id,
            searchKeyHex=search_key,
            conversationIdHex=conv_hex,
            bestMessageKey=best_key,
            reason=f"DB ingest: {rows_written} rows, {uniq_params} params"
        )
    except Exception as e:
        raise HTTPException(500, str(e))

# ============== Excel Viewer (uses CURRENT_EXCEL_PATH) ==============
class SheetsResponse(BaseModel):
    sheets: List[str]

class GridResponse(BaseModel):
    columns: List[str]
    rows: List[List[Any]]

@app.get("/api/excel/sheets", response_model=SheetsResponse)
def api_list_sheets():
    path = CURRENT_EXCEL_PATH
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Excel file not found. Ingest first.")
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return SheetsResponse(sheets=["CSV"])
    try:
        xls = pd.ExcelFile(path)
        return SheetsResponse(sheets=xls.sheet_names)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/excel/sheet/{name}", response_model=GridResponse)
def api_get_sheet(name: str):
    path = CURRENT_EXCEL_PATH or EXCEL_LOCAL_PATH
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Excel file not found. Ingest first.")
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(path, dtype=object)
        else:
            df = pd.read_excel(path, sheet_name=name, dtype=object)
        cols = [str(c) for c in df.columns]
        rows = df.where(pd.notna(df), None).values.tolist()
        return GridResponse(columns=cols, rows=rows)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Sheet '{name}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============== Debug SQL helpers ==============
class MonthlyStatsResp(BaseModel):
    ok: bool
    rows: int
    items: list[dict]
    months: list[str]

@app.get("/debug/sql-head")
def debug_sql_head(
    client: str,
    region: str,
    workspace: Optional[str] = None,
    limit: int = 20
):
    import sqlite3
    from .analytics import DB_FILE, _ensure_table

    _ensure_table()
    if not os.path.exists(DB_FILE):
        return {"ok": False, "reason": f"DB file not found at {DB_FILE}"}

    q = (
        "SELECT client,region,workspace,parameter,ts_utc,value,"
        "message_id,received_utc,sheet_name "
        "FROM timeseries_data WHERE client=? AND region=?"
    )
    args = [client, region]
    if workspace:
        q += " AND workspace=?"
        args.append(workspace)
    q += " ORDER BY ts_utc DESC LIMIT ?"
    args.append(max(1, min(limit, 1000)))

    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute(q, args).fetchall()]

    return {"ok": True, "db_file": DB_FILE, "rows": rows, "count": len(rows)}

# --- public view of clients.json (masked) ---
@app.get("/api/options/store-public")
def view_store_public(include_senders: bool = False):
    """
    Read-only view of the client/region config (from clients.json).
    By default masks emails; pass ?include_senders=true to show raw emails.
    """
    data = load_clients() or {}

    def _mask_email(s: str):
        if not isinstance(s, str) or "@" not in s:
            return s
        name, dom = s.split("@", 1)
        if len(name) <= 2:
            masked = "*" * len(name)
        else:
            masked = name[0] + "*" * max(1, len(name) - 2) + name[-1]
        return f"{masked}@{dom}"

    def maybe_mask(v):
        return v if include_senders else _mask_email(v)

    out = {}

    for client, meta in data.items():
        if isinstance(meta, dict):
            entry = {}
            if "sender" in meta:
                entry["sender"] = maybe_mask(meta.get("sender"))
            regs = meta.get("regions", {})
            if isinstance(regs, dict):
                entry["regions"] = {r: maybe_mask(s) if isinstance(s, str) else s for r, s in regs.items()}
            elif isinstance(regs, list):
                entry["regions"] = regs
            else:
                entry["regions"] = []
            out[client] = entry
        elif isinstance(meta, list):
            out[client] = {"regions": meta}
        else:
            out[client] = {"regions": []}

    return out

@app.get("/api/options/allowed-senders")
def api_allowed_senders():
    data = load_clients() or {}
    out = set()
    for _, meta in data.items():
        if isinstance(meta, dict):
            # client-level (regionless fallback)
            em = str(meta.get("sender") or "").strip().lower()
            if em: out.add(em)
            # per-region
            regs = meta.get("regions", {})
            if isinstance(regs, dict):
                for _, v in regs.items():
                    emr = str(v or "").strip().lower()
                    if emr: out.add(emr)
    return {"ok": True, "senders": sorted(out)}

def _allowed_from_clients_json_for(client: Optional[str] = None) -> list[str]:
    """
    If client is given -> return only that client's sender(s).
    Else -> return all senders across all clients.
    """
    store = load_clients() or {}
    out = set()

    def add_email(s):
        s = (s or "").strip().lower()
        if s:
            out.add(s)

    if client:
        meta = store.get(client)
        if isinstance(meta, dict):
            add_email(meta.get("sender"))
            regs = meta.get("regions", {})
            if isinstance(regs, dict):
                for _, v in regs.items():
                    add_email(v)
        # legacy list shape has no senders
        return sorted(out)

    # all clients
    for _, meta in store.items():
        if isinstance(meta, dict):
            add_email(meta.get("sender"))
            regs = meta.get("regions", {})
            if isinstance(regs, dict):
                for _, v in regs.items():
                    add_email(v)
    return sorted(out)


from .config import load_clients

def _sender_from_clients_store(client: str, region: str | None) -> str | None:
    data = load_clients() or {}         
    meta = data.get(client)
    if not isinstance(meta, dict):
        return None
    r = (region or "").strip()

    # per-region sender (if region provided and mapped)
    regs = meta.get("regions")
    if isinstance(regs, dict) and r:
        s = regs.get(r)
        if isinstance(s, str) and s.strip():
            return s.strip()

    # regionless / fallback client-level sender
    s = meta.get("sender")
    if isinstance(s, str) and s.strip():
        return s.strip()

    return None

# ============== Additional Debug Endpoints ==============

@app.get("/debug/ps-raw")
def debug_ps_raw(client: str = "Citi", region: str = "EMEA", subject: str = "healthcheck", hours: int = 240):
    from .mapi_ps import fetch_save_for_pair
    res = fetch_save_for_pair(client, region, subject, hours)
    return {
        "raw_response": res,
        "best_key": res.get("internetMessageId") or res.get("searchKeyHex") or res.get("entryId"),
        "subject": res.get("mail_subject") or res.get("subject"),
        "received": res.get("receivedDateTime"),
    }

@app.get("/debug/init-sql")
def debug_init_sql():
    from .analytics import _ensure_table
    _ensure_table()
    return {"ok": True, "msg": "Table ensured"}


@app.get("/api/excel/latest-by-pair")
def api_excel_latest_by_pair(
    client: str,
    workspace: str,
    region: Optional[str] = None
):
    """
    Finds the most recently saved Excel for the given client/[region]/workspace,
    reads its first sheet and returns columns+rows.
    """
    from glob import glob
    import os
    import pandas as pd
 
    # --- resolve directory ---
    BASE_DIR = os.path.dirname(__file__)
    DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))
    region_folder = region.strip() if region and region.strip() else "__no_region__"
    search_root = os.path.join(DATA_DIR, client, region_folder)
 
    if not os.path.exists(search_root):
        return {"ok": False, "reason": f"No folder for {client}/{region_folder}"}
 
    # --- find all Excel files under that folder ---
    files = glob(os.path.join(search_root, "**", "*.xls*"), recursive=True)
    if not files:
        return {"ok": False, "reason": f"No Excel files for {client}/{region_folder}"}
 
    # --- pick latest modified file ---
    latest_file = max(files, key=os.path.getmtime)
    file_name = os.path.basename(latest_file)
 
    # --- read Excel content ---
    try:
        xls = pd.ExcelFile(latest_file)
        sheet_names = xls.sheet_names
        if workspace.strip().upper() in [s.strip().upper() for s in sheet_names]:
            sheet = workspace.strip().upper()
        else:
            sheet = sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet, dtype=object)
        cols = [str(c) for c in df.columns]
        rows = df.where(pd.notna(df), None).values.tolist()
    except Exception as e:
        raise HTTPException(500, f"Failed to read Excel: {e}")
 
    return {
        "ok": True,
        "client": client,
        "region": region,
        "workspace": workspace,
        "latest_file": latest_file,
        "sheet_used": sheet,
        "columns": cols,
        "rows": rows,
    }

# Add debug prints for auto-fetcher config
print(f"DEBUG: AUTO_FETCH_ENABLED = {AUTO_FETCH_ENABLED}")
print(f"DEBUG: AUTO_FETCH_INTERVAL_MINUTES = {AUTO_FETCH_INTERVAL_MINUTES}")

# Add this import at the top with other router imports
from .routes.auto_cycle import router as auto_cycle_router

# Add this line after other router includes (around line 42)
app.include_router(auto_cycle_router)