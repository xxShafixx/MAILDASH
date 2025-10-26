import os
import json
import subprocess
from typing import Any, Dict, List, Optional
from datetime import datetime
import re
import shutil

# Use clients.json helpers (single source of truth)
from .config import load_clients  # reads data/clients.json

BASE_DIR = os.path.dirname(__file__)

# Try common locations in order (first that exists will be used)
CANDIDATES = [
    os.path.join(BASE_DIR, "scripts", "Get-OutlookMail.ps1"),
    os.path.join(BASE_DIR, "Get-OutlookMail.ps1"),
    os.path.abspath(os.path.join(BASE_DIR, "..", "scripts", "Get-OutlookMail.ps1")),
    os.path.abspath(os.path.join(BASE_DIR, "..", "Get-OutlookMail.ps1")),
]

SCRIPT_PATH = next((p for p in CANDIDATES if os.path.exists(p)), None)

DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))
os.makedirs(DATA_DIR, exist_ok=True)
SAVE_DIR = DATA_DIR  # attachments land here

# Legacy optional files (still supported, but not required)
ALLOWED_SENDERS_FILE = os.path.join(DATA_DIR, "allowed_senders.json")   # [ "a@b", ... ] (lowercase)
SENDER_MAP_FILE      = os.path.join(DATA_DIR, "sender_map.json")        # { "Client": { "": "x@y", "EMEA": "z@w" } }

# Prefer pwsh (PS7), fallback to Windows PowerShell
PS_EXES = ["pwsh", "powershell", "powershell.exe"]
def _pick_ps_exe() -> str:
    for exe in PS_EXES:
        try:
            subprocess.run(
                [exe, "-NoLogo", "-NoProfile", "-Command", "$PSVersionTable.PSVersion"],
                capture_output=True, text=True, timeout=3
            )
            return exe
        except Exception:
            continue
    return "powershell"

def _extract_json(text: str) -> Dict[str, Any]:
    s = (text or "").strip()
    # 1) raw
    try:
        return json.loads(s)
    except Exception:
        pass
    # 2) last line object
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if ln.startswith("{") and ln.endswith("}"):
            try:
                return json.loads(ln)
            except Exception:
                continue
    # 3) first/last braces
    try:
        first = s.find("{"); last = s.rfind("}")
        if first != -1 and last != -1 and last > first:
            return json.loads(s[first:last+1])
    except Exception:
        pass
    raise RuntimeError("Invalid JSON from PowerShell. OUT(first 300): " + s[:300])

def _run_ps(args: List[str]) -> Dict[str, Any]:
    if not SCRIPT_PATH:
        raise RuntimeError("Get-OutlookMail.ps1 not found. Tried:\n" + "\n".join(CANDIDATES))
    ps = _pick_ps_exe()
    cmd = [ps, "-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", SCRIPT_PATH] + args
    # hide window on Windows
    creationflags = 0x08000000 if os.name == "nt" else 0
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", creationflags=creationflags)
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if not out:
        raise RuntimeError(f"PowerShell returned no output. STDERR: {err[:300]}")
    try:
        return _extract_json(out)
    except Exception:
        raise RuntimeError(f"Invalid JSON from PowerShell. OUT(first 300): {out[:300]} ERR(first 200): {err[:200]}")

def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-") or "na"


def _safe_dir(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return "__no_region__"
    s = re.sub(r'[\\/:*?"<>|]+', '_', s)   # windows-safe
    return s.strip(" .")


def _move_to_pair_dir(src_path: str, client: str, region: str) -> str:
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"Source file not found: {src_path}")
    now = datetime.now()
    dest_dir = os.path.join(
        SAVE_DIR,
        _safe_dir(client),                # no slug: keep case & spaces (sanitized)
        _safe_dir(region),
        now.strftime("%Y"),
        now.strftime("%m"),
    )
    os.makedirs(dest_dir, exist_ok=True)
    ext = os.path.splitext(src_path)[1] or ".xlsx"
    fname = f"{_safe_dir(region)}_{_safe_dir(client)}_{now.strftime('%Y%m%d_%H%M%S')}{ext}"
    dest_path = os.path.join(dest_dir, fname)
    shutil.move(src_path, dest_path)
    return dest_path


def _best_key(d: Dict[str, Any]) -> Optional[str]:
    return d.get("internetMessageId") or d.get("searchKeyHex") or d.get("entryId")

# --------------------------
# Allowed senders utilities
# --------------------------

def _build_allowed_from_clients_json() -> List[str]:
    """
    Extract allowed SMTP senders from data/clients.json structure:
    {
      "NARA": { "sender": "noreply@nara.com" },
      "CITI": { "regions": { "EMEA": "ops-emea@citi.com", ... } }
    }
    """
    allowed: List[str] = []
    store = load_clients() or {}
    for client, cfg in (store.items() if isinstance(store, dict) else []):
        if not isinstance(cfg, dict):
            continue
        # regionless
        s = cfg.get("sender")
        if s:
            allowed.append(str(s).strip().lower())
        # regionful
        regs = cfg.get("regions") or {}
        if isinstance(regs, dict):
            for r, se in regs.items():
                if se:
                    allowed.append(str(se).strip().lower())
    # uniquify
    out = []
    seen = set()
    for x in allowed:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def load_allowed_senders() -> List[str]:
    """
    Optional: read allow list from allowed_senders.json.
    If not present, **derive from clients.json** automatically.
    """
    # 1) explicit list file
    try:
        if os.path.exists(ALLOWED_SENDERS_FILE):
            with open(ALLOWED_SENDERS_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f) or []
                explicit = [str(x).strip().lower() for x in arr if x]
                if explicit:
                    return explicit
    except Exception:
        pass
    # 2) derive from clients.json
    return _build_allowed_from_clients_json()

def lookup_sender_for_pair(client: str, region: str) -> Optional[str]:
    """
    Resolve exact SMTP for (client, region) from **clients.json** primarily.
    Fallback to sender_map.json if present (legacy).
    """
    client_key = str(client or "")
    region_key = str(region or "")
    # Prefer clients.json
    try:
        store = load_clients() or {}
        cfg = store.get(client_key)
        if isinstance(cfg, dict):
            # regionful
            regs = cfg.get("regions")
            if isinstance(regs, dict) and region_key:
                se = regs.get(region_key)
                if se:
                    return str(se).strip()
            # regionless
            if not region_key:
                se = cfg.get("sender")
                if se:
                    return str(se).strip()
    except Exception:
        pass

    # Legacy fallback: sender_map.json
    try:
        if os.path.exists(SENDER_MAP_FILE):
            with open(SENDER_MAP_FILE, "r", encoding="utf-8") as f:
                mp = json.load(f) or {}
            se = (mp.get(client_key, {}) or {}).get(region_key)
            if se:
                return str(se).strip()
    except Exception:
        pass
    return None

# --------------------------
# Public API
# --------------------------

def list_recent(limit: int = 100, hours: int = 240, allowed_senders: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Calls PS List mode with:
      -Limit
      -Hours
      -AllowedSenders (once, as array)
    Returns PS JSON plus a __debug block indicating what we sent.
    """
    limit = max(1, min(limit, 50))
    hours = max(1, min(int(hours or 240), 720))

    args = ["-Mode", "List", "-Limit", str(limit), "-Hours", str(hours)]

    clean = []
    for x in (allowed_senders or []):
        if not x:
            continue
        s = str(x).strip().lower()
        if s and s not in clean:
            clean.append(s)
    if clean:
        # flag add only once, then values
        # args += ["-AllowedSenders"] + clean
        senders_as_string = ",".join(clean)
        args.extend(["-AllowedSenders", senders_as_string])

    res = _run_ps(args)
    # attach debug echo so we can see in API what was sent to PS
    if isinstance(res, dict):
        res["__debug"] = {
            "limit": limit,
            "hours": hours,
            "allowed_senders_sent": clean,
            "ps_script": SCRIPT_PATH,
        }
    return res



def fetch_save_for_pair(client: str, region: str, subject_hint: str = "", hours: int = 240, sender: Optional[str] = None) -> Dict[str, Any]:
    hours = max(1, hours)
    args = [
        "-Mode","FetchSave",
        "-Client", client or "",
        # "-Region", region or ""
        "-SubjectHint", subject_hint or "",
        "-Hours", str(hours),
        "-SaveDir", SAVE_DIR
    ]

    # only add -Region when it's truly non-empty
    if region and str(region).strip():
        args += ["-Region", region.strip()]

    # exact SMTP if we have one
    if sender:
        args += ["-Sender", sender]

    res = _run_ps(args)
    ...

    # Move saved file into structured dir
    path = res.get("path") or res.get("saved_path")
    if res.get("ok") and path and os.path.exists(path):
        try:
            final_path = _move_to_pair_dir(path, client, region or "")
            res["final_path"] = final_path
            res["saved_path"] = final_path
        except Exception as e:
            res["final_path"] = None
            res["move_error"] = str(e)
    else:
        res["final_path"] = None

    return res
