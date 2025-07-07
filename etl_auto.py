import psycopg2
import pandas as pd
from sqlalchemy import create_engine, inspect
from datetime import datetime, timedelta
import json
import yaml
import os

CONFIG = "config.yaml"
META = "etl_metadata.json"

# Load config
with open(CONFIG) as f:
    cfg = yaml.safe_load(f)

with open(META) as f:
    metadata = json.load(f)

# Build SQLAlchemy engine
def get_engine(host, port, user, password, db="postgres"):
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

src_base = get_engine(**cfg["source"])
tgt_base = get_engine(**cfg["target"])

def get_databases():
    with src_base.connect() as conn:
        rows = conn.execute("SELECT datname FROM pg_database WHERE datistemplate = false;").fetchall()
        return [row[0] for row in rows if row[0].startswith("app_db")]

def detect_time_column(columns):
    for col in columns:
        if any(key in col.lower() for key in ['date', 'time', 'created']):
            return col
    return None

def sync_table(db_name, table_name, src_engine, tgt_engine):
    inspector = inspect(src_engine)
    columns = [col["name"] for col in inspector.get_columns(table_name)]

    # Try to detect date/time column
    time_col = detect_time_column(columns)

    if time_col:
        since = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
        query = f"SELECT * FROM {table_name} WHERE {time_col} >= '{since}'"
    else:
        last_id = metadata.get(db_name, {}).get(table_name, 0)
        query = f"SELECT * FROM {table_name} WHERE id > {last_id}"

    df = pd.read_sql(query, src_engine)

    if df.empty:
        print(f"🟡 Skipped {db_name}.{table_name} (no new data)")
        return

    # Save max id for metadata
    if not time_col:
        max_id = df["id"].max()
        metadata.setdefault(db_name, {})[table_name] = int(max_id)

    df.to_sql(table_name, tgt_engine, if_exists="replace", index=False)
    print(f"✅ Synced {len(df)} rows -> {db_name}.{table_name}")

# Run
for db in get_databases():
    print(f"\n🔍 Processing DB: {db}")
    src = get_engine(cfg["source"]["host"], cfg["source"]["port"], cfg["source"]["user"], cfg["source"]["password"], db)
    try:
        with tgt_base.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(f"CREATE DATABASE {db}")
            print(f"✅ Created target DB: {db}")
    except:
        print(f"ℹ️ Target DB {db} already exists")

    tgt = get_engine(cfg["target"]["host"], cfg["target"]["port"], cfg["target"]["user"], cfg["target"]["password"], db)
    insp = inspect(src)
    tables = insp.get_table_names()
    for tbl in tables:
        try:
            sync_table(db, tbl, src, tgt)
        except Exception as e:
            print(f"❌ Error syncing {db}.{tbl}: {e}")

# Save metadata
with open(META, "w") as f:
    json.dump(metadata, f, indent=2)
print("\n✅ ETL Complete.")
