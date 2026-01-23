#!/usr/bin/env python3
"""
Idempotent migration for capacity_events (Python only).
Creates an in-DB backup table by default when applying.

Usage:
  python migrate_capacity_events.py --dry-run --db-url "<db_url>"
  python migrate_capacity_events.py --apply --db-url "<db_url>" --yes
"""
import argparse, datetime, logging, sys
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("migrate_capacity_events")

SQL_CHECK_COLUMN = """
SELECT 1 FROM information_schema.columns WHERE table_name='capacity_events' AND column_name = :col
"""
SQL_CHECK_PK = """
SELECT 1 FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name='capacity_events' AND tc.constraint_type='PRIMARY KEY' LIMIT 1
"""

def run_migration(engine, apply=False, do_backup=True):
    with engine.connect() as conn:
        col_id = conn.execute(text(SQL_CHECK_COLUMN), {"col": "id"}).fetchone() is not None
        col_pending = conn.execute(text(SQL_CHECK_COLUMN), {"col": "pending_inventory"}).fetchone() is not None
        col_reconciled = conn.execute(text(SQL_CHECK_COLUMN), {"col": "reconciled"}).fetchone() is not None
        col_reconciled_at = conn.execute(text(SQL_CHECK_COLUMN), {"col": "reconciled_at"}).fetchone() is not None
        col_reconciled_by = conn.execute(text(SQL_CHECK_COLUMN), {"col": "reconciled_by"}).fetchone() is not None
        pk_exists = conn.execute(text(SQL_CHECK_PK)).fetchone() is not None

        plan = []
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_table = f"capacity_events_backup_{ts}"

        if do_backup:
            plan.append(("backup_table", backup_table))
        if not col_id:
            plan.append(("add_column", "id", "BIGSERIAL"))
            plan.append(("set_pk_if_no_existing", None if pk_exists else "id"))
        else:
            plan.append(("skip", "id"))
        if not col_pending:
            plan.append(("add_column", "pending_inventory", "boolean NOT NULL DEFAULT false"))
        else:
            plan.append(("skip", "pending_inventory"))
        if not col_reconciled:
            plan.append(("add_column", "reconciled", "boolean NOT NULL DEFAULT false"))
        else:
            plan.append(("skip", "reconciled"))
        if not col_reconciled_at:
            plan.append(("add_column", "reconciled_at", "timestamp NULL"))
        else:
            plan.append(("skip", "reconciled_at"))
        if not col_reconciled_by:
            plan.append(("add_column", "reconciled_by", "text NULL"))
        else:
            plan.append(("skip", "reconciled_by"))
        plan.append(("ensure_index", "idx_capacity_events_pending_inventory", "pending_inventory"))
        plan.append(("ensure_index", "idx_capacity_events_reconciled", "reconciled"))

        logger.info("Planned actions:")
        for p in plan:
            logger.info("  %s", p)

        if not apply:
            return {"status": "dry-run", "plan": plan}

        logger.info("Applying migration...")
        if do_backup:
            logger.info("Creating backup table %s ...", backup_table)
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS {backup_table} AS TABLE capacity_events WITH NO DATA"))
            conn.execute(text(f"INSERT INTO {backup_table} SELECT * FROM capacity_events"))
            logger.info("Backup created.")

        if not col_id:
            conn.execute(text("ALTER TABLE capacity_events ADD COLUMN id BIGSERIAL"))
            if not pk_exists:
                try:
                    conn.execute(text("ALTER TABLE capacity_events ADD PRIMARY KEY (id)"))
                    logger.info("Set id as PRIMARY KEY")
                except Exception as e:
                    logger.warning("Could not set id as PK: %s", e)

        if not col_pending:
            conn.execute(text("ALTER TABLE capacity_events ADD COLUMN pending_inventory boolean NOT NULL DEFAULT false"))
        if not col_reconciled:
            conn.execute(text("ALTER TABLE capacity_events ADD COLUMN reconciled boolean NOT NULL DEFAULT false"))
        if not col_reconciled_at:
            conn.execute(text("ALTER TABLE capacity_events ADD COLUMN reconciled_at timestamp NULL"))
        if not col_reconciled_by:
            conn.execute(text("ALTER TABLE capacity_events ADD COLUMN reconciled_by text NULL"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_capacity_events_pending_inventory ON capacity_events (pending_inventory)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_capacity_events_reconciled ON capacity_events (reconciled)"))

        logger.info("Migration applied successfully.")
        return {"status": "applied", "plan": plan, "backup_table": (backup_table if do_backup else None)}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db-url", type=str, default=None)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--yes", action="store_true")
    p.add_argument("--no-backup", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    db_url = args.db_url
    if not db_url:
        try:
            from config import DB_CONFIG
            db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        except Exception:
            logger.error("Provide --db-url or create config.py with DB_CONFIG.")
            sys.exit(2)

    engine = create_engine(db_url, pool_pre_ping=True)

    if args.apply and not args.yes:
        confirm = input("Apply migration? Type 'yes' to continue: ").strip().lower()
        if confirm != "yes":
            print("Aborted.")
            sys.exit(0)

    result = run_migration(engine, apply=args.apply, do_backup=not args.no_backup)
    logger.info("Result: %s", result)

if __name__ == "__main__":
    main()