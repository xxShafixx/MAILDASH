import os
import json

# Admin login secret (for /auth/admin/login)
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "helloWorld")

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Database URL (default: SQLite in data/analytics.db)
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(DATA_DIR, 'analytics.db')}")

# Frontend base (default: local dev server)
FRONTEND_BASE = os.getenv("FRONTEND_BASE", "http://localhost:5173")

# Excel/CSV local path root
EXCEL_LOCAL_PATH = os.path.join(BASE_DIR, "..", "data")

# Parquet root (for analytics / backups)
PARQUET_ROOT = os.path.join(DATA_DIR, "parquet")
os.makedirs(PARQUET_ROOT, exist_ok=True)

# Clients/Regions store (simple JSON under data/)
CLIENTS_JSON = os.path.join(DATA_DIR, "clients.json")

# ---------- Helpers for Clients JSON ----------

def load_clients():
    """Load all clients + regions config from JSON file."""
    if not os.path.exists(CLIENTS_JSON):
        return {}
    with open(CLIENTS_JSON, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}

def save_clients(data: dict):
    """Save clients config back to JSON file."""
    with open(CLIENTS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

# ============== AUTO-FETCHER CONFIGURATION ==============
# Auto-fetch configuration
AUTO_FETCH_ENABLED = True
AUTO_FETCH_INTERVAL_MINUTES = 2
