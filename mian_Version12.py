#!/usr/bin/env python3
"""
mian.py - Capacity Management Dashboard (single-file, integrated auto-reconcile)

Behavior:
- Dashboard UI (KPIs, Trends, Events, Inventory) — improved layout and usability.
- Events: manual form and CSV upload. RAM entered in GB, converted to MB before saving.
- Inventory filter applied: assetstatus IN ('Running','Running/Not in Production'),
  assetlocation = 'NJ Datacenter', assettype = 'Server/VM' (matches your SQL).
- KPI math: Uses inventory baseline + non-reconciled events. Reconciled events are excluded.
- Auto-reconcile: integrated function that runs periodically in a background thread
  and marks pending events reconciled when inventory later contains the same server.
  This is embedded in this file (no separate reconcile_worker.py).
- Control: To avoid multiple reconcile threads when running multiple web workers,
  set environment variable RUN_RECONCILER="false". Default is "true".

Deployment (quick):
1. Edit config.py (DB_CONFIG, TOTAL_CAPACITY, RESERVED_PERCENT).
2. Ensure required packages installed system-wide.
3. Run one-time migration script (migrate_capacity_events.py) to add reconciliation columns.
4. Start this dashboard: python mian.py
   - The reconcile thread will start automatically (unless RUN_RECONCILER="false").

Notes:
- If you run multiple app workers (gunicorn with >1 worker), set RUN_RECONCILER="false"
  and run a single reconcile process elsewhere (or leave it true if you accept duplicates).
"""

import os
import io
import base64
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

import logging
from logging.handlers import TimedRotatingFileHandler

from dash import Dash, dcc, html, Input, Output, State, callback_context, no_update
import dash_bootstrap_components as dbc

# Import configuration (must exist)
from config import DB_CONFIG, TOTAL_CAPACITY, RESERVED_PERCENT

# -------------------- Logging --------------------
logger = logging.getLogger("capacity_dashboard")
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler("capacity.log", when="midnight", backupCount=7)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
if not logger.handlers:
    logger.addHandler(handler)

# -------------------- DB Engine --------------------
ENGINE = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}",
    pool_pre_ping=True
)

# -------------------- Inventory filters --------------------
INVENTORY_STATUS = ("Running", "Running/Not in Production")
INVENTORY_LOCATION = "NJ Datacenter"
INVENTORY_TYPE = "Server/VM"

# -------------------- Utilities --------------------
def fmt_cpu(v):
    try:
        return f"{int(round(float(v))):,} cores"
    except Exception:
        return "0 cores"

def fmt_ram_gb(v):
    try:
        return f"{float(v):,.2f} GB"
    except Exception:
        return "0.00 GB"

def fmt_storage_tb_from_gb(v):
    try:
        tb = float(v) / 1024.0
        return f"{tb:,.2f} TB"
    except Exception:
        return "0.00 TB"

def parse_csv(contents, filename):
    if not contents:
        raise ValueError("No file uploaded")
    _, content_string = contents.split(",", 1)
    decoded = base64.b64decode(content_string)
    df = pd.read_csv(io.StringIO(decoded.decode("utf-8")))
    lc = [c.lower() for c in df.columns]
    required = {"date", "server", "cpu", "ram", "storage"}
    if not required.issubset(set(lc)):
        raise ValueError("CSV must contain headers: date,server,cpu,ram,storage")
    mapping = {orig: orig.lower() for orig in df.columns}
    df = df.rename(columns=mapping)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["server"] = df["server"].astype(str)
    for c in ["cpu", "ram", "storage"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df = df[["date", "server", "cpu", "ram", "storage"]].rename(columns={"ram": "ram_gb", "storage": "storage_gb"})
    return df

def detect_and_convert_ram_sum_to_gb(raw_sum, raw_max, baseline_gb):
    try:
        rs = float(raw_sum)
        rm = float(raw_max)
    except Exception:
        return 0.0
    if baseline_gb <= 0:
        if abs(rm) > 1024 * 5:
            return rs / 1024.0
        return rs
    if abs(rm) > abs(baseline_gb) * 10 or abs(rm) > 1024 * 5:
        return rs / 1024.0
    return rs

# -------------------- Capacity Calculation --------------------
def calculate_capacity():
    """
    Returns dict with CPU, RAM, STORAGE metrics:
    - used, total, reserved, usable, available, pct
    Units:
    - CPU: cores
    - RAM: GB
    - STORAGE: GB internally (TOTAL_CAPACITY storage given as TB in config -> converted)
    """
    try:
        inv_sql = """
            SELECT
                COALESCE(SUM(NULLIF(TRIM(servercores), '')::numeric), 0) AS cpu,
                COALESCE(SUM(NULLIF(TRIM(servermemory), '')::numeric)/1024.0, 0) AS ram_gb,
                COALESCE(SUM(NULLIF(TRIM(totaldisk), '')::numeric), 0) AS storage_gb
            FROM inventory
            WHERE assetstatus IN (:s1, :s2)
              AND assetlocation = :loc
              AND assettype = :atype
        """
        inv = pd.read_sql(text(inv_sql), ENGINE, params={
            "s1": INVENTORY_STATUS[0], "s2": INVENTORY_STATUS[1],
            "loc": INVENTORY_LOCATION, "atype": INVENTORY_TYPE
        })
        baseline_cpu = float(inv["cpu"].iloc[0])
        baseline_ram_gb = float(inv["ram_gb"].iloc[0])
        baseline_storage_gb = float(inv["storage_gb"].iloc[0])
    except Exception as e:
        logger.exception("Inventory read failed")
        # return zeroed results on error
        return {
            "CPU": {"used": 0, "total": 0, "reserved": 0, "usable": 0, "available": 0, "pct": 0},
            "RAM": {"used": 0, "total": 0, "reserved": 0, "usable": 0, "available": 0, "pct": 0},
            "STORAGE": {"used": 0, "total": 0, "reserved": 0, "usable": 0, "available": 0, "pct": 0}
        }

    try:
        # exclude reconciled events from math
        ev_sql = """
            SELECT
                COALESCE(SUM(cpu_delta) FILTER (WHERE reconciled = false), 0) AS cpu_sum,
                COALESCE(SUM(ram_delta_gb) FILTER (WHERE reconciled = false), 0) AS ram_sum_raw,
                COALESCE(MAX(ABS(ram_delta_gb)) FILTER (WHERE reconciled = false), 0) AS ram_max_raw,
                COALESCE(SUM(storage_delta_gb) FILTER (WHERE reconciled = false), 0) AS storage_sum
            FROM capacity_events
        """
        ev = pd.read_sql(text(ev_sql), ENGINE)
        cpu_sum = float(ev["cpu_sum"].iloc[0])
        ram_sum_raw = float(ev["ram_sum_raw"].iloc[0])
        ram_max_raw = float(ev["ram_max_raw"].iloc[0])
        storage_sum = float(ev["storage_sum"].iloc[0])
    except Exception as e:
        logger.exception("Events aggregation failed")
        cpu_sum = ram_sum_raw = ram_max_raw = storage_sum = 0.0

    # Normalize ram event sum to GB if needed
    ram_sum_gb = detect_and_convert_ram_sum_to_gb(ram_sum_raw, ram_max_raw, baseline_ram_gb)

    used_cpu = baseline_cpu + cpu_sum
    used_ram_gb = baseline_ram_gb + ram_sum_gb
    used_storage_gb = baseline_storage_gb + storage_sum

    # Convert storage TOTAL from TB (config) -> GB
    total_storage_gb = TOTAL_CAPACITY.get("STORAGE", 0) * 1024.0

    def make(k, used, total):
        reserved = total * RESERVED_PERCENT
        usable = max(total - reserved, 0)
        used_val = max(used, 0)
        pct = (used_val / usable * 100) if usable > 0 else 0
        available = max(usable - used_val, 0)
        return {"used": used_val, "total": total, "reserved": reserved, "usable": usable, "available": available, "pct": pct}

    results = {
        "CPU": make("CPU", used_cpu, TOTAL_CAPACITY.get("CPU", 0)),
        "RAM": make("RAM", used_ram_gb, TOTAL_CAPACITY.get("RAM", 0)),
        "STORAGE": make("STORAGE", used_storage_gb, total_storage_gb)
    }
    logger.debug("Calculated capacity: %s", results)
    return results

# -------------------- Trend builder --------------------
def build_trend(resource):
    """
    Returns a Plotly Figure showing baseline + cumulative used (last 30 days).
    STORAGE plotted in TB for readability.
    """
    try:
        # baseline
        inv_sql = """
            SELECT
                COALESCE(SUM(NULLIF(TRIM(servercores), '')::numeric), 0) AS cpu,
                COALESCE(SUM(NULLIF(TRIM(servermemory), '')::numeric)/1024.0, 0) AS ram_gb,
                COALESCE(SUM(NULLIF(TRIM(totaldisk), '')::numeric), 0) AS storage_gb
            FROM inventory
            WHERE assetstatus IN (:s1, :s2)
              AND assetlocation = :loc
              AND assettype = :atype
        """
        inv = pd.read_sql(text(inv_sql), ENGINE, params={
            "s1": INVENTORY_STATUS[0], "s2": INVENTORY_STATUS[1],
            "loc": INVENTORY_LOCATION, "atype": INVENTORY_TYPE
        })
        baseline = float(inv["cpu" if resource == "CPU" else ("ram_gb" if resource == "RAM" else "storage_gb")].iloc[0])
    except Exception as e:
        logger.exception("Trend: inventory read failed")
        return go.Figure()

    today = datetime.today().date()
    start = today - timedelta(days=30)

    try:
        df = pd.read_sql(text("""
            SELECT DATE(event_time) AS d,
                   SUM(
                       CASE
                           WHEN :r = 'CPU' THEN COALESCE(cpu_delta, 0)
                           WHEN :r = 'RAM' THEN COALESCE(ram_delta_gb, 0)
                           WHEN :r = 'STORAGE' THEN COALESCE(storage_delta_gb, 0)
                           ELSE 0
                       END
                   ) AS v
            FROM capacity_events
            WHERE event_time >= :start_date AND reconciled = false
            GROUP BY DATE(event_time)
            ORDER BY d
        """), ENGINE, params={"r": resource, "start_date": start})
    except Exception as e:
        logger.exception("Trend events read failed")
        df = pd.DataFrame({"d": pd.date_range(start, today), "v": 0})

    date_range = pd.date_range(start, today)
    if df.empty:
        df = pd.DataFrame({"d": date_range, "v": 0})
    else:
        df = df.set_index("d").reindex(date_range).fillna(0).reset_index()
        df.columns = ["d", "v"]

    if resource == "RAM":
        max_v = df["v"].abs().max()
        if max_v > (abs(baseline) * 10 if baseline > 0 else 1024 * 5):
            df["v"] = df["v"] / 1024.0

    df["used"] = baseline + df["v"].cumsum()

    if resource == "STORAGE":
        df["plot"] = df["used"] / 1024.0  # TB
        baseline_plot = baseline / 1024.0
        y_label = "Used (TB)"
    else:
        df["plot"] = df["used"]
        baseline_plot = baseline
        y_label = "Used (cores)" if resource == "CPU" else "Used (GB)"

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["d"], y=df["plot"], mode="lines", line=dict(color="#0b5ed7", width=2), fill="tozeroy", name="Used"))
    fig.add_trace(go.Scatter(x=[df["d"].iloc[0], df["d"].iloc[-1]], y=[baseline_plot, baseline_plot], mode="lines", line=dict(color="rgba(11,94,215,0.6)", dash="dot"), name="Baseline"))
    # usable line
    if resource == "STORAGE":
        total_gb = TOTAL_CAPACITY.get("STORAGE", 0) * 1024.0
        usable_plot = max(total_gb - total_gb * RESERVED_PERCENT, 0) / 1024.0
    else:
        total = TOTAL_CAPACITY.get(resource, 0)
        usable_plot = max(total - total * RESERVED_PERCENT, 0)

    if usable_plot > 0:
        fig.add_trace(go.Scatter(x=[df["d"].iloc[0], df["d"].iloc[-1]], y=[usable_plot, usable_plot], mode="lines", line=dict(color="rgba(220,53,69,0.6)", dash="dash"), name="Usable"))

    fig.update_layout(template="plotly_white", title=f"{resource} - Last 30 Days", xaxis_title="Date", yaxis_title=y_label, height=340, margin=dict(t=40, b=30))
    return fig

# -------------------- Auto-reconcile (integrated) --------------------
RECONCILE_INTERVAL = int(os.environ.get("RECONCILE_INTERVAL", "300"))  # seconds
RUN_RECONCILER = os.environ.get("RUN_RECONCILER", "true").lower() in ("true", "1", "yes")

_reconcile_lock = threading.Lock()

def auto_reconcile_pending_events(thresholds=None, dry_run=False):
    """
    Heuristic reconcile:
    - Finds pending events (pending_inventory=true and reconciled=false)
    - If inventory now contains assetuniquename and resource values match within thresholds,
      mark event reconciled and clear pending_inventory.
    """
    if thresholds is None:
        thresholds = {"cpu_abs": 1, "ram_mb_abs": 512, "storage_gb_abs": 5, "pct_tolerance": 0.20}
    matched = []
    try:
        if not _reconcile_lock.acquire(blocking=False):
            logger.debug("Reconcile: another run in progress, skipping")
            return {"status": "skipped", "reason": "busy"}
        pending_df = pd.read_sql(text("""
            SELECT id, assetuniquename, cpu_delta, ram_delta_gb, storage_delta_gb
            FROM capacity_events
            WHERE pending_inventory = true AND reconciled = false
            ORDER BY event_time
        """), ENGINE)
        for _, row in pending_df.iterrows():
            sid = int(row["id"])
            server = row["assetuniquename"]
            inv = pd.read_sql(text("SELECT servercores, servermemory, totaldisk FROM inventory WHERE assetuniquename = :s LIMIT 1"), ENGINE, params={"s": server})
            if inv.empty:
                continue
            # inventory values
            try:
                inv_cpu = float(inv["servercores"].iloc[0]) if inv["servercores"].iloc[0] not in (None, "") else 0.0
            except Exception:
                inv_cpu = 0.0
            try:
                inv_ram_mb = float(inv["servermemory"].iloc[0]) if inv["servermemory"].iloc[0] not in (None, "") else 0.0
            except Exception:
                inv_ram_mb = 0.0
            try:
                inv_storage_gb = float(inv["totaldisk"].iloc[0]) if inv["totaldisk"].iloc[0] not in (None, "") else 0.0
            except Exception:
                inv_storage_gb = 0.0

            ev_cpu = float(row["cpu_delta"] or 0)
            ev_ram_raw = float(row["ram_delta_gb"] or 0)
            ev_storage = float(row["storage_delta_gb"] or 0)

            # event RAM may be MB or GB; treat >1024 as MB already
            ev_ram_mb = ev_ram_raw if abs(ev_ram_raw) > 1024 else ev_ram_raw * 1024.0

            cpu_ok = (abs(ev_cpu - inv_cpu) <= thresholds["cpu_abs"]) or (abs(ev_cpu - inv_cpu) <= thresholds["pct_tolerance"] * max(1, inv_cpu))
            ram_ok = (abs(ev_ram_mb - inv_ram_mb) <= thresholds["ram_mb_abs"]) or (abs(ev_ram_mb - inv_ram_mb) <= thresholds["pct_tolerance"] * max(1, inv_ram_mb))
            storage_ok = (abs(ev_storage - inv_storage_gb) <= thresholds["storage_gb_abs"]) or (abs(ev_storage - inv_storage_gb) <= thresholds["pct_tolerance"] * max(1, inv_storage_gb))

            if cpu_ok and ram_ok and storage_ok:
                if not dry_run:
                    with ENGINE.begin() as conn:
                        conn.execute(text("""
                            UPDATE capacity_events
                            SET reconciled = true, pending_inventory = false, reconciled_at = now(), reconciled_by = 'AUTO'
                            WHERE id = :id
                        """), {"id": sid})
                matched.append(sid)
        logger.info("Auto-reconcile: matched %d events", len(matched))
        return {"matched_count": len(matched), "matched_ids": matched}
    except Exception as e:
        logger.exception("auto_reconcile error")
        return {"error": str(e)}
    finally:
        try:
            _reconcile_lock.release()
        except Exception:
            pass

def reconcile_loop(interval_seconds=RECONCILE_INTERVAL):
    # small startup delay
    time.sleep(2)
    while True:
        try:
            auto_reconcile_pending_events()
        except Exception:
            logger.exception("Background reconcile failed")
        time.sleep(interval_seconds)

# Start background reconcile thread (if enabled)
if RUN_RECONCILER:
    t = threading.Thread(target=reconcile_loop, daemon=True)
    t.start()
    logger.info("Started embedded reconcile thread (interval %s sec)", RECONCILE_INTERVAL)
else:
    logger.info("Embedded reconcile disabled (RUN_RECONCILER=%s)", os.environ.get("RUN_RECONCILER"))

# -------------------- Dash App --------------------
app = Dash(__name__, external_stylesheets=[dbc.themes.CERULEAN])
app.title = "Capacity Dashboard"

# Navbar / header
navbar = dbc.Navbar(
    dbc.Container([
        dbc.NavbarBrand("Capacity Dashboard", className="ms-2", style={"fontWeight": "700", "color": "#ffffff"}),
        dbc.Button("Refresh", id="refresh", color="light")
    ]),
    color="#0b5ed7",
    dark=True,
    className="mb-3"
)

# Small instruction card for events
events_instructions = dbc.Card(
    dbc.CardBody([
        html.H6("How to add events"),
        html.Ul([
            html.Li("Server: assetuniquename (required)"),
            html.Li("CPU Δ: cores (+/-)"),
            html.Li("RAM Δ: GB (entered in GB — converted to MB for DB)"),
            html.Li("Storage Δ: GB"),
        ], style={"fontSize": "0.9rem"}),
        html.P("CSV headers: date,server,cpu,ram,storage (ram in GB)", style={"fontSize": "0.9rem"})
    ]),
    className="mb-2"
)

app.layout = dbc.Container([
    navbar,
    html.H2("Capacity Management Dashboard", className="text-primary"),
    dcc.Interval(id="interval", interval=5 * 60 * 1000, n_intervals=0),  # 5 minutes

    dbc.Tabs([
        dbc.Tab(label="Dashboard", children=[
            html.H4("Current Capacity Overview", className="mt-3"),
            dbc.Row(id="kpi-row", className="g-4 mb-4"),
            html.H4("30-Day Trends", className="mt-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(id="cpu-trend"), md=4),
                dbc.Col(dcc.Graph(id="ram-trend"), md=4),
                dbc.Col(dcc.Graph(id="storage-trend"), md=4)
            ]),
            html.Div(id="alerts", className="mt-3")
        ]),

        dbc.Tab(label="Events", children=[
            dbc.Row([
                dbc.Col(events_instructions, md=3),

                dbc.Col([
                    dbc.Button("Toggle Manual Event Form", id="toggle-manual", color="primary", className="mb-2"),
                    dbc.Collapse(
                        dbc.Card(dbc.CardBody([
                            dbc.Label("Server (assetuniquename)"),
                            dbc.Input(id="server", placeholder="e.g. web-01", className="mb-2"),
                            dbc.Row([dbc.Col(dbc.Input(id="cpu", type="number", placeholder="CPU Δ (cores)")), dbc.Col(dbc.Input(id="ram", type="number", placeholder="RAM Δ (GB)"))], className="mb-2"),
                            dbc.Input(id="storage", type="number", placeholder="Storage Δ (GB)", className="mb-2"),
                            dbc.Button("Submit Event", id="submit", color="success"),
                            html.Div(id="msg", className="mt-2")
                        ])),
                        id="manual-collapse", is_open=False
                    ),
                    html.Hr(),
                    dbc.Button("Toggle CSV Upload", id="toggle-upload", color="secondary", className="mb-2"),
                    dbc.Collapse(
                        dbc.Card(dbc.CardBody([
                            html.P("CSV example: date,server,cpu,ram,storage"),
                            dcc.Upload(id="csv-upload", children=dbc.Button("Select & Upload CSV", color="secondary")),
                            html.Div(id="csv-msg", className="mt-2")
                        ])),
                        id="upload-collapse", is_open=False
                    )
                ], md=5),

                dbc.Col([
                    html.H5("Recent Events (filterable)"),
                    dcc.DatePickerRange(
                        id="recent-range",
                        min_date_allowed=datetime(2000, 1, 1).date(),
                        max_date_allowed=datetime.today().date(),
                        start_date=(datetime.today().date() - timedelta(days=30)),
                        end_date=datetime.today().date()
                    ),
                    dbc.Button("Apply", id="recent-apply", color="info", className="ms-2"),
                    html.Div(id="recent-events", className="mt-3")
                ], md=4)
            ], className="mt-3")
        ]),

        dbc.Tab(label="Inventory", children=[
            html.H4("Filtered Inventory", className="mt-3"),
            html.Div(id="inventory-table", className="mt-2")
        ])
    ])
], fluid=True)

# Toggle callbacks
@app.callback(Output("manual-collapse", "is_open"), Input("toggle-manual", "n_clicks"), State("manual-collapse", "is_open"))
def toggle_manual(n, is_open):
    if n:
        return not is_open
    return is_open

@app.callback(Output("upload-collapse", "is_open"), Input("toggle-upload", "n_clicks"), State("upload-collapse", "is_open"))
def toggle_upload(n, is_open):
    if n:
        return not is_open
    return is_open

# Main callback: handles submits, CSV, interval, recent filter
@app.callback(
    [
        Output("kpi-row", "children"),
        Output("alerts", "children"),
        Output("cpu-trend", "figure"),
        Output("ram-trend", "figure"),
        Output("storage-trend", "figure"),
        Output("recent-events", "children"),
        Output("inventory-table", "children"),
        Output("msg", "children"),
        Output("csv-msg", "children")
    ],
    [
        Input("submit", "n_clicks"),
        Input("csv-upload", "contents"),
        Input("interval", "n_intervals"),
        Input("refresh", "n_clicks"),
        Input("recent-apply", "n_clicks")
    ],
    [
        State("server", "value"),
        State("cpu", "value"),
        State("ram", "value"),
        State("storage", "value"),
        State("csv-upload", "filename"),
        State("recent-range", "start_date"),
        State("recent-range", "end_date")
    ],
    prevent_initial_call=False
)
def master_callback(submit_n, csv_contents, n_intervals, refresh_n, recent_apply_n,
                    server, cpu, ram, storage, filename, recent_start, recent_end):
    ctx = callback_context
    triggered = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else ""

    msg = no_update
    csv_msg = no_update
    refresh_needed = False

    # Manual event submission
    if triggered == "submit" and submit_n:
        if not server or str(server).strip() == "":
            msg = dbc.Alert("Server (assetuniquename) required", color="danger")
        elif (cpu is None or float(cpu) == 0) and (ram is None or float(ram) == 0) and (storage is None or float(storage) == 0):
            msg = dbc.Alert("Provide at least one non-zero delta (CPU, RAM, Storage)", color="danger")
        else:
            try:
                exists = pd.read_sql(text("SELECT 1 FROM inventory WHERE assetuniquename = :s LIMIT 1"), ENGINE, params={"s": server})
                pending = exists.empty
                ram_mb = float(ram) * 1024.0 if ram not in (None, "") else 0.0
                with ENGINE.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO capacity_events
                        (assetuniquename, cpu_delta, ram_delta_gb, storage_delta_gb, source, event_time, pending_inventory, reconciled)
                        VALUES (:s, :c, :r, :st, 'MANUAL', now(), :p, false)
                    """), {"s": server, "c": float(cpu or 0), "r": ram_mb, "st": float(storage or 0), "p": pending})
                msg = dbc.Alert(f"Event saved. Pending: {pending}", color="success")
                refresh_needed = True
            except Exception as e:
                logger.exception("Manual event insert failed")
                msg = dbc.Alert(f"Error saving event: {e}", color="danger")

    # CSV upload
    if triggered == "csv-upload" and csv_contents:
        try:
            df = parse_csv(csv_contents, filename)
            inserted = 0
            with ENGINE.begin() as conn:
                for _, row in df.iterrows():
                    server_name = row["server"]
                    exists = conn.execute(text("SELECT 1 FROM inventory WHERE assetuniquename = :s LIMIT 1"), {"s": server_name}).fetchone()
                    pending = exists is None
                    ram_mb = float(row["ram_gb"]) * 1024.0
                    conn.execute(text("""
                        INSERT INTO capacity_events
                        (assetuniquename, cpu_delta, ram_delta_gb, storage_delta_gb, source, event_time, pending_inventory, reconciled)
                        VALUES (:s, :c, :r, :st, 'CSV', :date, :p, false)
                    """), {
                        "s": server_name,
                        "c": float(row["cpu"]),
                        "r": ram_mb,
                        "st": float(row["storage_gb"]),
                        "date": row["date"],
                        "p": pending
                    })
                    inserted += 1
            csv_msg = dbc.Alert(f"Imported {inserted} rows (RAM GB→MB on save)", color="success")
            refresh_needed = True
        except Exception as e:
            logger.exception("CSV import failed")
            csv_msg = dbc.Alert(f"CSV error: {e}", color="danger")

    # Refresh data if needed or on interval/apply
    if refresh_needed or triggered in ("interval", "refresh", "") or triggered == "recent-apply":
        cap = calculate_capacity()

        # KPI cards
        kpi_cards = []
        alerts = []
        for k in ("CPU", "RAM", "STORAGE"):
            d = cap[k]
            pct = d.get("pct", 0)
            if pct > 100:
                color = "danger"
            elif pct > 90:
                color = "danger"
            elif pct > 80:
                color = "warning"
            else:
                color = "primary"

            if k == "CPU":
                used_text = fmt_cpu(d["used"]); total_text = fmt_cpu(d["total"]); reserved_text = fmt_cpu(d["reserved"]); usable_text = fmt_cpu(d["usable"]); available_text = fmt_cpu(d["available"])
            elif k == "RAM":
                used_text = fmt_ram_gb(d["used"]); total_text = fmt_ram_gb(d["total"]); reserved_text = fmt_ram_gb(d["reserved"]); usable_text = fmt_ram_gb(d["usable"]); available_text = fmt_ram_gb(d["available"])
            else:
                used_text = fmt_storage_tb_from_gb(d["used"]); total_text = fmt_storage_tb_from_gb(d["total"]); reserved_text = fmt_storage_tb_from_gb(d["reserved"]); usable_text = fmt_storage_tb_from_gb(d["usable"]); available_text = fmt_storage_tb_from_gb(d["available"])

            card = dbc.Card([
                dbc.CardBody([
                    html.H6(k, className="mb-1"),
                    html.H4(used_text, className="mb-2"),
                    html.Div([html.Small(f"Total: {total_text}", className="me-3"), html.Small(f"Reserved: {reserved_text}", className="me-3"), html.Small(f"Usable: {usable_text}")], className="d-flex flex-wrap mb-2"),
                    dbc.Progress(value=min(pct, 100), color=color, style={"height": "18px"}),
                    html.Div(className="d-flex justify-content-between mt-2", children=[html.Small(f"Available: {available_text}"), html.Small(f"{pct:.1f}%")])
                ])
            ], className="shadow-sm")
            kpi_cards.append(dbc.Col(card, md=4))

            if pct > 90:
                alerts.append(dbc.Alert(f"High {k} utilization ({pct:.1f}%)", color="warning", dismissable=True))

        # Trends
        cpu_trend = build_trend("CPU")
        ram_trend = build_trend("RAM")
        storage_trend = build_trend("STORAGE")

        # Recent events (default last 30 days or as selected)
        try:
            if recent_start:
                start_date = pd.to_datetime(recent_start).date()
            else:
                start_date = datetime.today().date() - timedelta(days=30)
            if recent_end:
                end_date = pd.to_datetime(recent_end).date()
            else:
                end_date = datetime.today().date()

            recent_df = pd.read_sql(text("""
                SELECT event_time, assetuniquename, cpu_delta, ram_delta_gb, storage_delta_gb
                FROM capacity_events
                WHERE DATE(event_time) BETWEEN :start_date AND :end_date
                ORDER BY event_time DESC
                LIMIT 200
            """), ENGINE, params={"start_date": start_date, "end_date": end_date})
        except Exception:
            logger.exception("Recent events query failed")
            recent_df = pd.DataFrame()

        if recent_df.empty:
            recent_table = html.P("No recent events", className="text-center text-muted p-4")
        else:
            def present_ram(v):
                try:
                    vv = float(v)
                except:
                    return v
                if abs(vv) > 1024 * 2:
                    return f"{vv/1024:.2f} GB"
                return f"{vv:.2f} GB"
            recent_df = recent_df.rename(columns={"event_time": "Event Time", "assetuniquename": "Server", "cpu_delta": "CPU Δ", "ram_delta_gb": "RAM Δ", "storage_delta_gb": "Storage Δ (GB)"})
            recent_df["RAM Δ"] = recent_df["RAM Δ"].apply(present_ram)
            recent_table = dbc.Table.from_dataframe(recent_df[["Event Time", "Server", "CPU Δ", "RAM Δ", "Storage Δ (GB)"]], striped=True, hover=True, bordered=True, responsive=True)

        # Inventory table with Server Name and ordered by assetuniquename
        try:
            inv_df = pd.read_sql(text("""
                SELECT assetuniquename AS "Server Name", assetipaddress AS "IP Address", servercores AS "CPU (cores)",
                       ROUND(COALESCE(NULLIF(TRIM(servermemory), '')::numeric / 1024.0, 0), 2) AS "RAM (GB)",
                       totaldisk AS "Storage (GB)", assetstatus AS "Status"
                FROM inventory
                WHERE assetstatus IN (:s1, :s2) AND assetlocation = :loc AND assettype = :atype
                ORDER BY assetuniquename
            """), ENGINE, params={"s1": INVENTORY_STATUS[0], "s2": INVENTORY_STATUS[1], "loc": INVENTORY_LOCATION, "atype": INVENTORY_TYPE}).fillna("N/A")
        except Exception:
            logger.exception("Inventory query failed")
            inv_df = pd.DataFrame()

        if inv_df.empty:
            inv_table = html.P("No inventory matching filters", className="text-center text-muted p-4")
        else:
            inv_table = dbc.Table.from_dataframe(inv_df[["Server Name", "IP Address", "CPU (cores)", "RAM (GB)", "Storage (GB)", "Status"]], striped=True, hover=True, bordered=True, responsive=True)

        return kpi_cards, alerts, cpu_trend, ram_trend, storage_trend, recent_table, inv_table, no_update, no_update

    # No change
    return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update

# -------------------- Run --------------------
if __name__ == "__main__":
    # Run initial reconcile once synchronously to reduce immediate duplicates
    if RUN_RECONCILER:
        try:
            logger.info("Running initial reconcile at startup")
            auto_reconcile_pending_events()
        except Exception:
            logger.exception("Initial reconcile failed")

    # Start web app
    app.run(host="0.0.0.0", port=8050, debug=False)