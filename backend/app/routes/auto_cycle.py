import os
import sqlite3
from typing import List, Dict, Any
from fastapi import APIRouter, Query, HTTPException

# Import the helper to load the canonical client list
from ..config import load_clients
# Import DB_FILE from the central analytics module for consistency
from ..analytics import DB_FILE

router = APIRouter()

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
# DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "data"))
# DB_FILE = os.path.join(DATA_DIR, "analytics.db")


def _get_conn() -> sqlite3.Connection:
    """Get a database connection with a dict row factory."""
    if not os.path.exists(DB_FILE):
        raise HTTPException(status_code=404, detail=f"Database file not found at {DB_FILE}")
    
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = lambda cur, row: {
        cur.description[i][0]: row[i] for i in range(len(row))
    }
    return conn


@router.get("/api/auto-cycle/combinations")
def get_auto_cycle_combinations(
    workspace_filter: str = Query(None, description="Optional: filter by workspace (e.g., 'LIVE', 'FED', 'MIG')")
) -> Dict[str, Any]:
    """
    CORRECTED: Query the database to get all unique client-region-workspace combinations
    that have existing data. This is the most accurate source of truth for the auto-cycler.
    """
    try:
        conn = _get_conn()
        
        base_query = """
            SELECT DISTINCT 
                client,
                COALESCE(region, '') as region,
                workspace
            FROM timeseries_data
            WHERE client IS NOT NULL AND client != '' AND workspace IS NOT NULL AND workspace != ''
        """
        
        params = []
        if workspace_filter:
            base_query += " AND UPPER(workspace) = UPPER(?)"
            params.append(workspace_filter)
        
        base_query += " ORDER BY client, region, workspace"
        
        rows = conn.execute(base_query, params).fetchall()
        conn.close()
        
        # Convert list of row objects to a list of standard dicts
        combinations = [dict(row) for row in rows]
        
        return {
            "ok": True,
            "count": len(combinations),
            "combinations": combinations,
            "workspace_filter": workspace_filter
        }
        
    except HTTPException as e:
         return {"ok": False, "error": e.detail, "count": 0, "combinations": []}
    except Exception as e:
        return {"ok": False, "error": f"An unexpected error occurred: {str(e)}", "count": 0, "combinations": []}


@router.get("/api/auto-cycle/clients")
def get_unique_clients() -> Dict[str, Any]:
    """
    Get all unique clients from the clients.json configuration file.
    """
    try:
        store = load_clients() or {}
        clients = sorted(store.keys())
        return {"ok": True, "count": len(clients), "clients": clients}
    except Exception as e:
        return {"ok": False, "error": str(e), "count": 0, "clients": []}


@router.get("/api/auto-cycle/regions")
def get_regions_for_client(client: str = Query(..., description="Client name")) -> Dict[str, Any]:
    """
    Get all unique regions for a specific client from the clients.json config.
    """
    try:
        store = load_clients() or {}
        client_config = store.get(client)
        regions = []
        
        if isinstance(client_config, dict):
            regions_map = client_config.get("regions", {})
            if isinstance(regions_map, dict):
                regions = sorted(regions_map.keys())
        
        # For region-less clients, an empty list is correctly returned.
        return {"ok": True, "client": client, "count": len(regions), "regions": regions}
    except Exception as e:
        return {"ok": False, "error": str(e), "client": client, "count": 0, "regions": []}


@router.get("/api/auto-cycle/workspaces")
def get_workspaces_for_client_region(
    client: str = Query(..., description="Client name"),
    region: str = Query("", description="Region name (empty for regionless clients)")
) -> Dict[str, Any]:
    """
    Get all unique workspaces for a specific client-region combination from the database.
    This logic is correct as workspaces are data-driven, not config-driven.
    """
    conn = None
    try:
        conn = _get_conn()
        
        query = "SELECT DISTINCT workspace FROM timeseries_data WHERE client = ?"
        params: List[Any] = [client]
        
        if region:
            query += " AND region = ?"
            params.append(region)
        else:
            # Correctly handle region-less clients (region is NULL or empty string)
            query += " AND (region IS NULL OR region = '')"
        
        query += " ORDER BY workspace"
        
        rows = conn.execute(query, params).fetchall()
        workspaces = [row["workspace"] for row in rows]
        
        return {
            "ok": True,
            "client": client,
            "region": region,
            "count": len(workspaces),
            "workspaces": workspaces
        }
    except Exception as e:
        # Re-raise HTTPException for FastAPI to handle.
        if isinstance(e, HTTPException):
            raise
        return {
            "ok": False,
            "error": str(e),
            "client": client,
            "region": region,
            "count": 0,
            "workspaces": []
        }
    finally:
        # Ensure the database connection is always closed.
        if conn:
            conn.close()