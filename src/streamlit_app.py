import os
import time
from collections import defaultdict

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from psql_analytics import PSQLAnalytics
from psql_manager import PSQLManager
from redis_manager import RedisManager


load_dotenv()


@st.cache_resource
def get_psql_analytics():
    manager = PSQLManager()
    manager.connect()
    return PSQLAnalytics(manager)


@st.cache_resource
def get_redis_client():
    manager = RedisManager()
    manager.connect()
    return manager.client


@st.cache_data(ttl=20)
def get_postgres_snapshots(window_hours, top_limit, top_users_type):
    analytics = get_psql_analytics()

    events_df = analytics.gap_filled_time_series(window_hours)
    top_users_df = analytics.top_users_today(limit=top_limit, user_type=top_users_type)
    top_wikis_df = analytics.top_wikis_today(limit=top_limit)

    # prepared for future chart expansion in the granular workspace
    type_mix_df = analytics.event_type_distribution_today()
    event_size_df = analytics.event_size_distribution()
    patrolled_df = analytics.patrolled_bot_distribution_today()

    return events_df, top_users_df, top_wikis_df, type_mix_df, event_size_df, patrolled_df


def minute_bucket_now():
    return int(time.time() // 60)


def aggregate_redis_windows(client, window_minutes_list):
    current_minute = minute_bucket_now()
    windows = sorted(set(window_minutes_list))
    starts = {w: current_minute - w + 1 for w in windows}
    aggregates = {w: defaultdict(int) for w in windows}

    records = []
    for key in client.scan_iter(match="minute:*:*:*", count=1000):
        parts = key.split(":", 3)
        if len(parts) != 4:
            continue

        _, minute_str, metric_group, metric_name = parts
        try:
            minute = int(minute_str)
        except ValueError:
            continue

        # skip keys older than the largest requested window
        if minute < starts[max(windows)]:
            continue

        records.append((key, minute, metric_group, metric_name))

    if not records:
        return {w: {} for w in windows}

    pipe = client.pipeline()
    for key, _, _, _ in records:
        pipe.get(key)
    values = pipe.execute()

    for (_, minute, metric_group, metric_name), raw_val in zip(records, values):
        value = int(raw_val or 0)
        agg_key = f"{metric_group}:{metric_name}"
        for window in windows:
            if minute >= starts[window]:
                aggregates[window][agg_key] += value

    return {w: dict(aggregates[w]) for w in windows}


def aggregate_top_users_window(client, window_minutes):
    top_users = defaultdict(int)
    current_minute = minute_bucket_now()
    minute_keys = [
        f"top_users:minute:{minute}" for minute in range(current_minute - window_minutes + 1, current_minute + 1)
    ]

    pipe = client.pipeline()
    for key in minute_keys:
        pipe.zrevrange(key, 0, -1, withscores=True)
    results = pipe.execute()

    for entries in results:
        for user, score in entries:
            top_users[user] += int(score)

    rows = sorted(top_users.items(), key=lambda x: x[1], reverse=True)[:10]
    return pd.DataFrame(rows, columns=["user", "events"])


def render_postgres_section(window_hours, top_limit, top_users_type):
    st.subheader("PostgreSQL Analytics")

    events_df, top_users_df, top_wikis_df, type_mix_df, event_size_df, patrolled_df = get_postgres_snapshots(
        window_hours, top_limit, top_users_type
    )

    events_fig = px.line(events_df, x="minutes_ts", y="events", title=f"Events Per Minute ({window_hours}h)")
    events_fig.update_layout(height=380, margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(events_fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        top_users_label = top_users_type.capitalize()
        top_users_pg_fig = px.bar(
            top_users_df,
            x="user",
            y="event_count",
            title=f"Top Users Today ({top_users_label}, Postgres)",
        )
        top_users_pg_fig.update_xaxes(categoryorder="total descending")
        top_users_pg_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(top_users_pg_fig, use_container_width=True)

    with col2:
        top_wikis_pg_fig = px.bar(top_wikis_df, x="wiki", y="event_count", title="Top Wikis Today (Postgres)")
        top_wikis_pg_fig.update_xaxes(categoryorder="total descending")
        top_wikis_pg_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(top_wikis_pg_fig, use_container_width=True)

    st.markdown("### PostgreSQL Granular Metrics Workspace")
    st.caption("Use this section to add deeper analytics charts as you expand the SQL set.")

    tab1, tab2, tab3 = st.tabs(["Event Type Mix", "Event Size Preview", "Patrolled Preview"])

    with tab1:
        if not type_mix_df.empty:
            type_mix_fig = px.pie(type_mix_df, names="type", values="event_count", title="Event Type Mix (Today)")
            type_mix_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(type_mix_fig, use_container_width=True)
        else:
            st.info("No event type data for today yet.")

    with tab2:
        st.dataframe(event_size_df, use_container_width=True)
        st.caption("Placeholder: add histograms/percentiles for length distribution.")

    with tab3:
        st.dataframe(patrolled_df, use_container_width=True)
        st.caption("Placeholder: add stacked bars for patrolled vs unpatrolled by user type.")


@st.cache_data(ttl=10)
def get_redis_snapshots():
    client = get_redis_client()
    aggregate_windows = aggregate_redis_windows(client, [5, 60])
    aggregates_5m = aggregate_windows.get(5, {})
    aggregates_1h = aggregate_windows.get(60, {})
    top_users_5m = aggregate_top_users_window(client, 5)

    one_hour_total = aggregates_1h.get("events:total", 0)
    five_min_total = aggregates_5m.get("events:total", 0)
    spike_score = 0.0
    if one_hour_total > 0:
        baseline_five_min = one_hour_total / 12
        spike_score = five_min_total / baseline_five_min if baseline_five_min > 0 else 0.0

    return aggregates_5m, aggregates_1h, top_users_5m, spike_score


def render_redis_section():
    st.subheader("Redis Realtime Metrics")
    aggregates_5m, aggregates_1h, top_users_5m, spike_score = get_redis_snapshots()

    c1, c2, c3 = st.columns(3)
    c1.metric("Events (5m)", aggregates_5m.get("events:total", 0))
    c2.metric("Events (1h)", aggregates_1h.get("events:total", 0))
    c3.metric("Spike Score (5m vs 1h baseline)", f"{spike_score:.2f}x")

    if not top_users_5m.empty:
        top_users_redis_fig = px.bar(top_users_5m, x="user", y="events", title="Top Users (Last 5 Minutes)")
        top_users_redis_fig.update_xaxes(categoryorder="total descending")
        top_users_redis_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(top_users_redis_fig, use_container_width=True)
    else:
        st.info("No top-user data available in Redis for the last 5 minutes.")


def main():
    st.set_page_config(page_title="Wikipedia Edit Dashboard", layout="wide")
    st.title("Wikipedia Edit Pipeline Dashboard")

    st.caption("Basic local dashboard: PostgreSQL for historical analytics, Redis for realtime snapshots.")

    with st.sidebar:
        st.header("Controls")
        window_hours = st.selectbox("Postgres window (hours)", [1, 2, 4, 6, 8], index=0)
        top_users_type = st.selectbox("Top users type (Postgres)", ["all", "bot", "human"], index=0)
        top_limit = st.slider("Top N (today)", min_value=5, max_value=20, value=10, step=1)
        if st.button("Refresh now"):
            st.cache_data.clear()

    render_postgres_section(window_hours, top_limit, top_users_type)
    st.divider()
    render_redis_section()


if __name__ == "__main__":
    main()
