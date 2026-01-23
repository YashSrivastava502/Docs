#!/usr/bin/env python3
"""
Standalone reconcile worker. Runs continuously and auto-reconciles pending events.

Start as:
  nohup python reconcile_worker.py &
"""
import time, logging
from logging.handlers import TimedRotatingFileHandler
from sqlalchemy import create_engine, text
from datetime import datetime
from config import DB_CONFIG

AUTO_RECONCILE_INTERVAL = 300  # seconds

logger = logging.getLogger("reconcile_worker")
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler("reconcile_worker.log", when="midnight", backupCount=7)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

ENGINE = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}",
    pool_pre_ping=True
)

def auto_reconcile_pending_events(thresholds=None):
    if thresholds is None:
        thresholds = {'cpu_abs':1, 'ram_mb_abs':512, 'storage_gb_abs':5, 'pct_tolerance':0.20}
    matched = []
    try:
        with ENGINE.connect() as conn:
            pending_rows = conn.execute(text("""
                SELECT id, assetuniquename, cpu_delta, ram_delta_gb, storage_delta_gb
                FROM capacity_events
                WHERE pending_inventory = true AND reconciled = false
                ORDER BY event_time
            """)).fetchall()
        for row in pending_rows:
            sid = int(row['id']); server = row['assetuniquename']
            with ENGINE.connect() as conn:
                inv = conn.execute(text("SELECT servercores, servermemory, totaldisk FROM inventory WHERE assetuniquename = :s LIMIT 1"), {"s": server}).fetchone()
            if not inv:
                continue
            try: inv_cpu = float(inv['servercores']) if inv['servercores'] not in (None,'') else 0.0
            except: inv_cpu = 0.0
            try: inv_ram_mb = float(inv['servermemory']) if inv['servermemory'] not in (None,'') else 0.0
            except: inv_ram_mb = 0.0
            try: inv_storage_gb = float(inv['totaldisk']) if inv['totaldisk'] not in (None,'') else 0.0
            except: inv_storage_gb = 0.0

            ev_cpu = float(row['cpu_delta'] or 0)
            ev_ram_raw = float(row['ram_delta_gb'] or 0)
            ev_storage = float(row['storage_delta_gb'] or 0)
            ev_ram_mb = ev_ram_raw if abs(ev_ram_raw) > 1024 else ev_ram_raw * 1024.0

            cpu_ok = (abs(ev_cpu - inv_cpu) <= thresholds['cpu_abs']) or (abs(ev_cpu - inv_cpu) <= thresholds['pct_tolerance'] * max(1, inv_cpu))
            ram_ok = (abs(ev_ram_mb - inv_ram_mb) <= thresholds['ram_mb_abs']) or (abs(ev_ram_mb - inv_ram_mb) <= thresholds['pct_tolerance'] * max(1, inv_ram_mb))
            storage_ok = (abs(ev_storage - inv_storage_gb) <= thresholds['storage_gb_abs']) or (abs(ev_storage - inv_storage_gb) <= thresholds['pct_tolerance'] * max(1, inv_storage_gb))

            if cpu_ok and ram_ok and storage_ok:
                with ENGINE.begin() as conn:
                    conn.execute(text("""
                        UPDATE capacity_events
                        SET reconciled = true, pending_inventory = false, reconciled_at = now(), reconciled_by = 'AUTO_WORKER'
                        WHERE id = :id
                    """), {"id": sid})
                matched.append(sid)
        logger.info("Auto-reconcile matched %d events", len(matched))
        return {"matched_count": len(matched), "matched_ids": matched}
    except Exception as e:
        logger.exception("Auto-reconcile error")
        return {"error": str(e)}

def main_loop():
    logger.info("Reconcile worker started (interval %s sec)", AUTO_RECONCILE_INTERVAL)
    while True:
        try:
            res = auto_reconcile_pending_events()
            if res and res.get("matched_count",0) > 0:
                logger.info("Matched IDs: %s", res.get("matched_ids"))
        except Exception:
            logger.exception("Worker loop error")
        time.sleep(AUTO_RECONCILE_INTERVAL)

if __name__ == "__main__":
    main_loop()