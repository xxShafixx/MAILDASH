from __future__ import annotations

import os
from typing import Dict, Any, List, Set, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from . import mapi_ps
from .config import load_clients

router = APIRouter()


# -----------------------------
# Helpers (single source: clients.json)
# -----------------------------

def _norm_email(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return s.strip().lower()


def _build_allowed_senders_from_clients() -> Set[str]:
    """
    Extract allowed SMTP senders from data/clients.json structure:
    {
      "NARA": { "sender": "noreply@nara.com" },
      "CITI": { "regions": { "EMEA": "ops-emea@citi.com", ... } }
    }
    """
    allow: Set[str] = set()
    store = load_clients() or {}
    if not isinstance(store, dict):
        return allow

    for _, cfg in store.items():
        if not isinstance(cfg, dict):
            continue
        # regionless
        em = _norm_email(cfg.get("sender"))
        if em:
            allow.add(em)
        # regionful
        regs = cfg.get("regions")
        if isinstance(regs, dict):
            for _r, se in regs.items():
                em2 = _norm_email(se)
                if em2:
                    allow.add(em2)
    return allow


def _compose_subject(client: str, region: Optional[str], keyword: Optional[str]) -> str:
    parts: List[str] = []
    if client:
        parts.append(str(client).strip())
    if region:
        region = str(region).strip()
        if region:
            parts.append(region)
    if keyword:
        kw = str(keyword).strip()
        if kw:
            parts.append(kw)
    return " ".join(parts)


# -----------------------------
# Routes
# -----------------------------

@router.get("/mail/recent-with-attachments")
def recent_with_attachments(
    limit: int = Query(20, ge=1, le=50),
    hours: int = Query(240, ge=1, le=720)
):
    """
    Recent mails (Outlook via PowerShell), filtered to ONLY those whose sender
    exists in clients.json (regionless client sender, OR each region sender).
    """
    try:
        allowed = list(_build_allowed_senders_from_clients())
        raw = mapi_ps.list_recent(limit=limit, hours=hours, allowed_senders=allowed)
        if not isinstance(raw, dict):
            return {"ok": False, "error": "Invalid response from mail provider"}

        items = raw.get("items", []) or []
        return {"ok": True, "items": items, "total": len(items)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.get("/mail/resolve-sender")
def resolve_sender(client: str = Query(...), region: Optional[str] = Query(None)):
    """
    Debug helper: which SMTP sender will be used for (client, region)?
    """
    try:
        se = mapi_ps.lookup_sender_for_pair(client, region or "")
        return {"ok": True, "client": client, "region": region, "sender": se}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/mail/fetch-save")
def fetch_and_save(
    client: str = Query(..., description="Client name"),
    region: Optional[str] = Query(None, description="Region name if client has regions"),
    keyword: Optional[str] = Query("", description="Subject keyword entered by user"),
    hours: int = Query(240, ge=1, le=720),
):
    """
    Fetch + save exactly one matching Excel/CSV attachment for the given selection.
    Rules:
      - Sender is auto-resolved (clients.json).
      - Subject is built as: 'client [region] keyword' (AND-match in PowerShell).
      - If no matching mail or Excel/CSV attachment, returns ok=false with reason.
    """
    try:
        # resolve sender from clients.json (single source of truth)
        sender = mapi_ps.lookup_sender_for_pair(client, region or "")
        subj = _compose_subject(client, region, keyword)

        res = mapi_ps.fetch_save_for_pair(
            client=client,
            region=region or "",
            subject_hint=subj,
            hours=hours,
            sender=sender  # exact SMTP; if None, PS will not filter by sender
        )

        # Shape the response consistently
        ok = bool(res.get("ok"))
        out: Dict[str, Any] = {
            "ok": ok,
            "client": client,
            "region": region,
            "sender": sender,
            "subject_hint": subj,
            "saved_path": res.get("saved_path"),
            "final_path": res.get("final_path"),
            "mail_subject": res.get("mail_subject"),
            "fromEmail": res.get("fromEmail"),
            "receivedDateTime": res.get("receivedDateTime"),
            "message_id": res.get("internetMessageId") or res.get("entryId") or res.get("searchKeyHex"),
            "raw": res,
        }
        if not ok:
            out["reason"] = res.get("reason") or "No matching mail or no Excel/CSV attachment found"
        return out

    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
