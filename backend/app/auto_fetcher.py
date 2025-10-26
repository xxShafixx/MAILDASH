import time
import os
from datetime import datetime
import threading
from typing import List, Dict, Any

# Import the loader for your main configuration file
from .config import AUTO_FETCH_INTERVAL_MINUTES, AUTO_FETCH_ENABLED, load_clients
from .mapi_ps import fetch_save_for_pair, lookup_sender_for_pair
from .analytics import ingest_excel

def _best_key(d: Dict[str, Any]) -> str:
    """Get the best message identifier for logging"""
    return d.get("internetMessageId") or d.get("searchKeyHex") or d.get("entryId") or "<unknown>"

def generate_auto_fetch_filters() -> List[Dict[str, Any]]:
    """
    Dynamically generates auto-fetch filters from the clients.json configuration.
    Uses default values for subject_hint and hours for all clients.
    """
    print("Dynamically generating auto-fetch filters from clients.json...")
    try:
        client_store = load_clients() or {}
        filters = []
        
        default_subject_hint = "healthcheck"
        default_hours = 240

        for client, config in client_store.items():
            if not isinstance(config, dict):
                continue
            
            # 1. Always process all defined regions, if they exist.
            regions_config = config.get("regions", {})
            if isinstance(regions_config, dict) and regions_config:
                for region in regions_config.keys():
                    filters.append({
                        "client": client,
                        "region": region,
                        "subject_hint": default_subject_hint,
                        "hours": default_hours
                    })

            # 2. ALSO process the top-level sender if it exists.
            # This handles both region-less clients and clients with a fallback/dual config.
            if "sender" in config:
                filters.append({
                    "client": client,
                    "region": "", # Empty string for region-less config
                    "subject_hint": default_subject_hint,
                    "hours": default_hours
                })
        
        print(f"Generated {len(filters)} filters to process.")
        # Deduplicate in case a client has a top-level sender but empty regions object
        unique_filters = [dict(t) for t in {tuple(d.items()) for d in filters}]
        return sorted(unique_filters, key=lambda x: (x['client'], x['region']))

    except Exception as e:
        print(f"Error generating auto-fetch filters: {e}")
        return []

def process_auto_fetch_filter(filter_config: Dict[str, Any]) -> bool:
    """
    Process a single auto-fetch filter configuration.
    Returns True if successful, False otherwise.
    """
    try:
        client = filter_config.get("client", "").strip()
        region = filter_config.get("region", "").strip()
        subject_hint = filter_config.get("subject_hint", "").strip()
        hours = filter_config.get("hours", 240)
        
        if not client:
            print(f"Skipping filter - missing client: {filter_config}")
            return False
        
        print(f"Auto-fetching for {client}/{region or 'N/A'} (subject: '{subject_hint}', hours: {hours})")
        
        sender_email = lookup_sender_for_pair(client, region)
        if not sender_email:
            print(f"Could not find a configured sender for {client}/{region}. Skipping.")
            return False
        
        res = fetch_save_for_pair(
            client=client,
            region=region,
            subject_hint=subject_hint,
            hours=hours,
            sender=sender_email
        )
        
        ok = bool(res.get("ok") or res.get("saved"))
        saved_path = res.get("final_path") or res.get("saved_path") or res.get("path")
        
        if not ok:
            print(f"No matching mail/attachment for {client}/{region}: {res.get('reason')}")
            return False
        
        if not saved_path or not os.path.exists(saved_path):
            print(f"Saved file not found after fetch: {saved_path}")
            return False
        
        best_key = _best_key(res)
        mail_subject = res.get("mail_subject") or res.get("subject") or ""
        received_iso = res.get("receivedDateTime") or res.get("received_iso") or None
        
        print(f"Fetched mail: '{mail_subject}'")
        print(f"Saved attachment to: {saved_path}")
        
        try:
            rows_written, uniq_params = ingest_excel(
                saved_path,
                client=client,
                region=region,
                message_id=best_key,
                received_iso=received_iso,
            )
            print(f"Ingested {rows_written} rows, {uniq_params} unique parameters for {client}/{region}")
            return True
            
        except Exception as e:
            print(f"DB ingest failed for {client}/{region}: {e}")
            return False
            
    except Exception as e:
        print(f"Error processing filter {filter_config}: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_auto_fetch_cycle():
    """Run one cycle of auto-fetching for all dynamically generated filters."""
    print(f"\n{'='*20}\nStarting auto-fetch cycle at {datetime.now().isoformat()}\n{'='*20}")
    
    filters = generate_auto_fetch_filters()
    
    if not filters:
        print("No valid clients/regions found in clients.json to generate filters. Ending cycle.")
        return
    
    print(f"Processing {len(filters)} dynamically generated filters.")
    
    success_count = 0
    for i, filter_config in enumerate(filters, 1):
        print(f"\n--- Processing filter {i}/{len(filters)} ---")
        if process_auto_fetch_filter(filter_config):
            success_count += 1
    
    print(f"\n{'='*20}\nAuto-fetch cycle completed: {success_count}/{len(filters)} successful\n{'='*20}")

def auto_fetch_loop():
    """Main auto-fetch loop that runs continuously."""
    print("Starting auto-fetcher service...")
    
    while True:
        try:
            run_auto_fetch_cycle()
        except Exception as e:
            print(f"CRITICAL Error in auto-fetch cycle: {e}")
        
        sleep_minutes = AUTO_FETCH_INTERVAL_MINUTES
        print(f"Waiting {sleep_minutes} minutes until next auto-fetch cycle...")
        time.sleep(sleep_minutes * 60)