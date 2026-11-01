"""
Anime Streaming Dashboard (Jikan API -> Kafka -> Spark -> MongoDB -> Streamlit)

Page 1: Real-time view  (consumes Kafka directly, refreshes every N seconds)
Page 2: Historical view (queries MongoDB, filters, aggregation, export)
"""

import io
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

load_dotenv()

st.set_page_config(
    page_title="Anime Streaming Dashboard",
    page_icon="🎌",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    div[data-testid="stMetric"] {
        background: rgba(128,128,128,0.08);
        border-radius: 12px;
        padding: 12px 16px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

REQUIRED_KEYS = ["timestamp", "value", "metric_type", "sensor_id"]
LOWER_IS_BETTER = {"rank", "popularity"}
BUFFER_LIMIT = 3000
TIME_RANGES = {
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "all": None,
}


# ============================================================ sidebar / config
def setup_sidebar():
    st.sidebar.title("🎌 Dashboard Controls")

    st.sidebar.subheader("Kafka")
    kafka_broker = st.sidebar.text_input("Kafka Broker", os.getenv("KAFKA_BROKER", "localhost:9092"))
    kafka_topic = st.sidebar.text_input("Kafka Topic", os.getenv("KAFKA_TOPIC", "streaming-data"))

    st.sidebar.subheader("Storage (MongoDB)")
    mongo_uri = st.sidebar.text_input("Mongo URI", os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    mongo_db = st.sidebar.text_input("Database", os.getenv("MONGO_DB", "anime_db"))
    raw_col = st.sidebar.text_input("Raw collection", os.getenv("MONGO_RAW_COLLECTION", "anime_raw"))
    agg_col = st.sidebar.text_input("Aggregate collection", os.getenv("MONGO_AGG_COLLECTION", "anime_agg"))

    st.sidebar.subheader("Refresh Settings")
    auto_refresh = st.sidebar.checkbox("Enable Auto Refresh", value=True)
    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 15)
    if auto_refresh:
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

    if st.sidebar.button("🔄 Manual Refresh"):
        st.rerun()
    if st.sidebar.button("🗑️ Clear live buffer"):
        st.session_state.live_buffer = []

    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh (UTC)", datetime.utcnow().strftime("%H:%M:%S"))

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "mongo_uri": mongo_uri,
        "mongo_db": mongo_db,
        "raw_col": raw_col,
        "agg_col": agg_col,
        "auto_refresh": auto_refresh,
        "refresh_interval": refresh_interval,
    }


# ================================================================ Kafka (live)
@st.cache_resource(show_spinner=False)
def get_consumer(broker: str, topic: str):
    """One long-lived consumer, rewound to the most recent ~300 messages."""
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        request_timeout_ms=10000,
    )
    parts = consumer.partitions_for_topic(topic)
    if not parts:
        consumer.close()
        raise RuntimeError(f"Topic '{topic}' not found. Start producer.py first.")
    tps = [TopicPartition(topic, p) for p in parts]
    consumer.assign(tps)
    ends = consumer.end_offsets(tps)
    for tp in tps:
        consumer.seek(tp, max(0, ends[tp] - 300))
    return consumer


def consume_kafka_data(config) -> pd.DataFrame:
    """Poll new messages, append to a session buffer, return the buffer as DataFrame."""
    if "live_buffer" not in st.session_state:
        st.session_state.live_buffer = []

    try:
        consumer = get_consumer(config["kafka_broker"], config["kafka_topic"])
        batches = consumer.poll(timeout_ms=1500, max_records=1000)
    except Exception as e:
        st.error(f"Kafka error: {e}")
        get_consumer.clear()
        return pd.DataFrame(st.session_state.live_buffer)

    new_rows = 0
    for msgs in batches.values():
        for m in msgs:
            d = m.value
            if not all(k in d for k in REQUIRED_KEYS):
                continue
            try:
                d = dict(d)
                d["timestamp"] = pd.to_datetime(d["timestamp"], utc=True).tz_convert(None)
                d["value"] = float(d["value"])
            except (ValueError, TypeError):
                continue
            st.session_state.live_buffer.append(d)
            new_rows += 1

    st.session_state.live_buffer = st.session_state.live_buffer[-BUFFER_LIMIT:]
    st.session_state.last_new_rows = new_rows
    return pd.DataFrame(st.session_state.live_buffer)


# ============================================================ MongoDB (history)
@st.cache_resource(show_spinner=False)
def get_mongo(uri: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    return client


def query_historical_data(config, time_range="24h", metrics=None, limit=100000) -> pd.DataFrame:
    client = get_mongo(config["mongo_uri"])
    col = client[config["mongo_db"]][config["raw_col"]]

    query = {}
    delta = TIME_RANGES.get(time_range)
    if delta:
        query["timestamp"] = {"$gte": datetime.utcnow() - delta}
    if metrics:
        query["metric_type"] = {"$in": metrics}

    docs = list(col.find(query, {"_id": 0}).sort("timestamp", 1).limit(limit))
    df = pd.DataFrame(docs)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def query_spark_aggregates(config, kind="metric", limit=500) -> pd.DataFrame:
    client = get_mongo(config["mongo_uri"])
    col = client[config["mongo_db"]][config["agg_col"]]
    docs = list(col.find({"kind": kind}, {"_id": 0}).sort("batch_time", -1).limit(limit))
    return pd.DataFrame(docs)


def aggregate_data(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """Time aggregation of the history (raw / hourly / daily / weekly)."""
    if level == "raw" or df.empty:
        return df
    freq = {"hourly": "h", "daily": "D", "weekly": "W"}[level]
    out = (
        df.groupby([pd.Grouper(key="timestamp", freq=freq), "metric_type"])["value"]
        .agg(mean="mean", min="min", max="max", count="count")
        .reset_index()
    )
    return out.rename(columns={"mean": "value"})


# ================================================================== exports
def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Data", index=False)
        if "metric_type" in df.columns and "value" in df.columns:
            summary = df.groupby("metric_type")["value"].agg(["count", "mean", "min", "max", "std"]).round(3)
            summary.to_excel(writer, sheet_name="Summary")
    return buf.getvalue()


def to_pdf_bytes(df: pd.DataFrame, title: str, max_rows: int = 40) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4), title=title)
    styles = getSampleStyleSheet()
    story = [
        Paragraph(title, styles["Title"]),
        Paragraph(f"Generated {datetime.utcnow():%Y-%m-%d %H:%M} UTC &nbsp;|&nbsp; {len(df)} records", styles["Normal"]),
        Spacer(1, 12),
    ]

    if not df.empty and "metric_type" in df.columns:
        s = df.groupby("metric_type")["value"].agg(["count", "mean", "min", "max"]).round(2).reset_index()
        data = [list(s.columns)] + s.values.tolist()
        t = Table(data, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#3b4cca")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
        ]))
        story += [Paragraph("Summary by metric", styles["Heading2"]), t, Spacer(1, 12)]

    cols = [c for c in ["timestamp", "title", "metric_type", "value"] if c in df.columns]
    if cols and not df.empty:
        sample = df[cols].tail(max_rows).copy()
        sample["timestamp"] = sample["timestamp"].astype(str)
        sample["title"] = sample.get("title", "").astype(str).str.slice(0, 40) if "title" in sample else ""
        data = [cols] + sample.values.tolist()
        t = Table(data, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#3b4cca")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
        ]))
        story += [Paragraph(f"Latest {len(sample)} records", styles["Heading2"]), t]

    doc.build(story)
    return buf.getvalue()


def export_buttons(df: pd.DataFrame, prefix: str, title: str):
    if df.empty:
        return
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    c1, c2, c3 = st.columns(3)
    c1.download_button("⬇️ CSV", df.to_csv(index=False).encode("utf-8"),
                       f"{prefix}_{stamp}.csv", "text/csv", key=f"csv_{prefix}")
    c2.download_button("⬇️ Excel (.xlsx)", to_excel_bytes(df), f"{prefix}_{stamp}.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"xlsx_{prefix}")
    try:
        c3.download_button("⬇️ PDF report", to_pdf_bytes(df, title), f"{prefix}_{stamp}.pdf",
                           "application/pdf", key=f"pdf_{prefix}")
    except Exception as e:
        c3.caption(f"PDF unavailable: {e}")


# ============================================================== page 1: live
def display_real_time_view(config):
    st.header("📈 Real-time Streaming Dashboard")
    st.info(
        f"**Auto-refresh:** {'🟢 Enabled' if config['auto_refresh'] else '🔴 Disabled'}"
        f" — every {config['refresh_interval']} seconds · Source: Jikan API via Kafka topic "
        f"`{config['kafka_topic']}`"
    )

    with st.spinner("Fetching real-time data from Kafka..."):
        df = consume_kafka_data(config)

    if df.empty:
        st.warning("No messages yet. Make sure Kafka is running and `python producer.py` is started.")
        return

    age = (datetime.utcnow() - df["timestamp"].max()).total_seconds()
    icon = "🟢" if age < 30 else "🟡" if age < 90 else "🔴"
    st.success(f"{icon} Newest record is {age:.0f}s old · {st.session_state.get('last_new_rows', 0)} new this refresh")

    # latest snapshot per anime/metric
    latest = df.sort_values("timestamp").groupby(["sensor_id", "metric_type"]).tail(1)
    wide = latest.pivot_table(index=["sensor_id", "title"], columns="metric_type",
                              values="value", aggfunc="last").reset_index()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Records buffered", f"{len(df):,}")
    c2.metric("Anime tracked", df["sensor_id"].nunique())
    avg_score = latest.loc[latest["metric_type"] == "score", "value"].mean()
    c3.metric("Avg score", f"{avg_score:.2f}" if pd.notna(avg_score) else "—")
    c4.metric("Data range (UTC)", f"{df['timestamp'].min():%H:%M} - {df['timestamp'].max():%H:%M}")

    metrics_available = sorted(df["metric_type"].unique())
    colf1, colf2 = st.columns(2)
    metric = colf1.selectbox("Metric", metrics_available,
                             index=metrics_available.index("score") if "score" in metrics_available else 0,
                             key="live_metric")
    top_n = colf2.slider("Top N anime", 5, 30, 15, key="live_topn")

    # Chart 1: top N bar
    st.subheader(f"🏆 Top {top_n} anime by {metric}")
    snap = latest[latest["metric_type"] == metric].copy()
    snap = snap.sort_values("value", ascending=metric in LOWER_IS_BETTER).head(top_n)
    fig = px.bar(snap, x="value", y="title", orientation="h", color="value",
                 color_continuous_scale="Viridis", template="plotly_white",
                 labels={"value": metric, "title": ""})
    fig.update_layout(yaxis={"categoryorder": "total ascending" if metric not in LOWER_IS_BETTER else "total descending"},
                      height=max(350, top_n * 28), coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

    # Chart 2: score vs members scatter
    left, right = st.columns(2)
    with left:
        st.subheader("👥 Score vs Members")
        if {"score", "members"}.issubset(wide.columns):
            fig2 = px.scatter(wide, x="members", y="score", hover_name="title",
                              size="favorites" if "favorites" in wide.columns else None,
                              log_x=True, template="plotly_white")
            st.plotly_chart(fig2, width="stretch")
        else:
            st.caption("Waiting for score/members data...")
    with right:
        st.subheader("📉 Trend over time")
        titles = sorted(df["title"].dropna().unique())
        pick = st.selectbox("Anime", titles, key="live_title")
        trend = df[(df["title"] == pick) & (df["metric_type"] == metric)].sort_values("timestamp")
        if len(trend) > 1:
            fig3 = px.line(trend, x="timestamp", y="value", markers=True, template="plotly_white",
                           labels={"value": metric, "timestamp": "Time (UTC)"})
            st.plotly_chart(fig3, width="stretch")
        else:
            st.caption("Only one data point so far for this anime (the producer cycles through pages).")

    with st.expander("📋 View Raw Data"):
        st.dataframe(df.sort_values("timestamp", ascending=False), width="stretch", height=300)

    st.subheader("📦 Export live data")
    export_buttons(df, "live_anime", "Live Anime Stream Report")


# ========================================================== page 2: history
def display_historical_view(config):
    st.header("📊 Historical Data Analysis")
    st.caption("All data collected by the Spark job and stored in MongoDB.")

    col1, col2, col3 = st.columns(3)
    time_range = col1.selectbox("Time Range", list(TIME_RANGES.keys()), index=1)
    metric_choice = col2.selectbox(
        "Metric Type", ["all", "score", "scored_by", "rank", "popularity", "members", "favorites"])
    aggregation = col3.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"])

    try:
        with st.spinner("Querying MongoDB..."):
            df = query_historical_data(config, time_range, None if metric_choice == "all" else [metric_choice])
    except Exception as e:
        st.error(f"Could not query MongoDB: {e}")
        return

    if df.empty:
        st.warning("No historical data found. Is `spark_consumer.py` running?")
        return

    # optional anime filter
    titles = sorted(df["title"].dropna().unique()) if "title" in df.columns else []
    chosen = st.multiselect("Filter anime (optional)", titles)
    if chosen:
        df = df[df["title"].isin(chosen)]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Records", f"{len(df):,}")
    m2.metric("Anime", df["sensor_id"].nunique())
    m3.metric("Date Range", f"{df['timestamp'].min():%m-%d %H:%M} → {df['timestamp'].max():%m-%d %H:%M}")
    m4.metric("Avg Value", f"{df['value'].mean():,.2f}")

    # Trends
    st.subheader(f"Historical Trends ({aggregation})")
    plot_df = aggregate_data(df, aggregation)
    if aggregation == "raw":
        top_titles = df["title"].value_counts().head(8).index if "title" in df.columns else []
        plot_df = plot_df[plot_df["title"].isin(top_titles)] if len(top_titles) else plot_df
        fig = px.line(plot_df, x="timestamp", y="value", color="title",
                      facet_row="metric_type", template="plotly_white")
    else:
        fig = px.line(plot_df, x="timestamp", y="value", facet_row="metric_type",
                      markers=True, template="plotly_white")
    fig.update_yaxes(matches=None)
    fig.update_layout(height=max(350, 260 * plot_df["metric_type"].nunique()))
    st.plotly_chart(fig, width="stretch")

    # Aggregation tables
    st.subheader("📐 Aggregation Summary")
    summary = (df.groupby("metric_type")["value"]
               .agg(["count", "mean", "min", "max", "std"]).round(3).reset_index())
    st.dataframe(summary, width="stretch", hide_index=True)

    a, b = st.columns(2)
    with a:
        st.markdown("**Spark aggregates: genre average score**")
        try:
            g = query_spark_aggregates(config, "genre")
            if not g.empty:
                g = (g.groupby("genre").apply(lambda x: (x["avg_score"] * x["count"]).sum() / x["count"].sum(),
                                              include_groups=False)
                     .sort_values(ascending=False).head(15).reset_index(name="avg_score"))
                figg = px.bar(g, x="avg_score", y="genre", orientation="h", template="plotly_white")
                figg.update_layout(yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(figg, width="stretch")
            else:
                st.caption("No genre aggregates yet.")
        except Exception as e:
            st.caption(f"Aggregates unavailable: {e}")
    with b:
        st.markdown("**Spark aggregates: per-batch metric stats**")
        try:
            m = query_spark_aggregates(config, "metric", 100)
            if not m.empty:
                st.dataframe(m.drop(columns=["kind"], errors="ignore"), width="stretch", height=350, hide_index=True)
            else:
                st.caption("No metric aggregates yet.")
        except Exception as e:
            st.caption(f"Aggregates unavailable: {e}")

    st.subheader("Historical Data Table")
    st.dataframe(df.sort_values("timestamp", ascending=False), width="stretch", hide_index=True, height=350)

    st.subheader("📦 Export history")
    export_buttons(plot_df if aggregation != "raw" else df, "history_anime", "Historical Anime Report")


# ==================================================================== main
def main():
    st.title("🎌 Anime Streaming Data Dashboard")
    st.caption("Jikan API → Kafka → Spark Structured Streaming → MongoDB → Streamlit")

    if "live_buffer" not in st.session_state:
        st.session_state.live_buffer = []

    config = setup_sidebar()
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    with tab1:
        display_real_time_view(config)
    with tab2:
        display_historical_view(config)


if __name__ == "__main__":
    main()