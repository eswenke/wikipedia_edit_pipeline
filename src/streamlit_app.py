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
    """Return a cached PSQLAnalytics instance backed by a connected manager."""
    manager = PSQLManager()
    manager.connect()
    return PSQLAnalytics(manager)


@st.cache_resource
def get_redis_client():
    """Return a cached Redis client from RedisManager."""
    manager = RedisManager()
    manager.connect()
    return manager.client


@st.cache_data(ttl=20)
def get_postgres_snapshots(window_hours, top_limit, top_users_type):
    """Fetch and cache PostgreSQL datasets used by dashboard charts."""
    # get a PSQLAnalytics instance
    analytics = get_psql_analytics()

    # fetch the datasets with respect to N limit / window hours / user type (sidebar options)
    events_df = analytics.gap_filled_time_series(window_hours)
    top_users_df = analytics.top_users_today(limit=top_limit, user_type=top_users_type)
    top_wikis_df = analytics.top_wikis_today(limit=top_limit)

    # prepared for future chart expansion in the granular workspace
    type_mix_df = analytics.event_type_distribution_today()
    event_size_df = analytics.event_size_distribution()
    patrolled_df = analytics.patrolled_bot_distribution_today()

    return events_df, top_users_df, top_wikis_df, type_mix_df, event_size_df, patrolled_df


def minute_bucket_now():
    """Return the current unix-minute bucket for rolling-window lookups."""
    # functionally same as redis_manager.py:_get_minute_bucket, but didn't want to import it
    return int(time.time() // 60)


def aggregate_redis_windows(client, window_minutes_list):
    """Aggregate minute-bucket Redis counters for multiple windows in one pass."""
    current_minute = minute_bucket_now()
    windows = sorted(set(window_minutes_list))  # [5, 60] for example (5 minute, 1 hr windows)
    starts = {w: current_minute - w + 1 for w in windows}  # grabs the unix minute that is 'w' minutes prior
    aggregates = {w: defaultdict(int) for w in windows}  # {5: defaultdict(int), 60: defaultdict(int)}

    records = []
    # scan for all minute-bucket keys (1000 internal per batch, can be tuned)
    for key in client.scan_iter(match="minute:*:*:*", count=1000):
        parts = key.split(":", 3)
        if len(parts) != 4:
            continue

        # extract components from key format: minute:<minute>:<metric_group>:<metric_name>
        _, minute_str, metric_group, metric_name = parts
        try:
            minute = int(minute_str)
        except ValueError:
            continue

        # skip keys older than the largest requested window
        if minute < starts[max(windows)]:
            continue

        records.append((key, minute, metric_group, metric_name))

    # if no records found, return empty aggregates
    if not records:
        return {w: {} for w in windows}

    # fetch all values in one pipeline (less network round trips)
    pipe = client.pipeline()
    for key, _, _, _ in records:
        pipe.get(key)
    values = pipe.execute()

    # aggregate values by metric group and name
    for (_, minute, metric_group, metric_name), raw_val in zip(records, values):
        # convert to int for python type safety
        value = int(raw_val or 0)

        # create aggregate key
        agg_key = f"{metric_group}:{metric_name}"

        # for each window, add the value to the aggregate
        for window in windows:
            if minute >= starts[window]:
                aggregates[window][agg_key] += value

    return {w: dict(aggregates[w]) for w in windows}


def aggregate_top_users_window(client, window_minutes):
    """Aggregate top-user scores across minute sorted sets for a rolling window."""
    # initialize defaultdict to prep minute keys by user for aggregation
    top_users = defaultdict(int)
    current_minute = minute_bucket_now()
    minute_keys = [
        f"top_users:minute:{minute}" for minute in range(current_minute - window_minutes + 1, current_minute + 1)
    ]

    # pipeline fetch all values for each user for each minute key
    pipe = client.pipeline()
    for key in minute_keys:
        pipe.zrevrange(key, 0, -1, withscores=True)
    results = pipe.execute()

    # loop over each entry, summing each user's score across all minutes to get top users in this window
    for entries in results:
        for user, score in entries:
            top_users[user] += int(score)

    # sort by score and take top 10
    rows = sorted(top_users.items(), key=lambda x: x[1], reverse=True)[:10]
    return pd.DataFrame(rows, columns=["user", "events"])


def render_postgres_section(window_hours, top_limit, top_users_type):
    """Render the PostgreSQL analytics section and related charts."""
    st.subheader("PostgreSQL Analytics")

    # get postgres dfs given window hours, top limit, and top users type (sidebar options)
    events_df, top_users_df, top_wikis_df, type_mix_df, event_size_df, patrolled_df = get_postgres_snapshots(
        window_hours, top_limit, top_users_type
    )

    # plot events per minute
    events_fig = px.line(events_df, x="minutes_ts", y="events", title=f"Events Per Minute ({window_hours}h)")
    events_fig.update_layout(height=380, margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(events_fig, width="stretch")

    # plot top users and top wikis side by side
    col1, col2 = st.columns(2)

    # plot top users
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
        st.plotly_chart(top_users_pg_fig, width="stretch")

    # plot top wikis
    with col2:
        top_wikis_pg_fig = px.bar(top_wikis_df, x="wiki", y="event_count", title="Top Wikis Today (Postgres)")
        top_wikis_pg_fig.update_xaxes(categoryorder="total descending")
        top_wikis_pg_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(top_wikis_pg_fig, width="stretch")

    # plot event type mix, event size preview, and patrolled preview
    st.markdown("### PostgreSQL Granular Metrics Workspace")
    st.caption("Use this section to add deeper analytics charts as we expand the SQL set.")

    # create tabs for each chart
    tab1, tab2, tab3 = st.tabs(["Event Type Mix", "Event Size Preview", "Patrolled Preview"])

    # event type mix
    with tab1:
        if not type_mix_df.empty:
            type_mix_fig = px.pie(type_mix_df, names="type", values="event_count", title="Event Type Mix (Today)")
            type_mix_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(type_mix_fig, width="stretch")
        else:
            st.info("No event type data for today yet.")

    # event size preview
    with tab2:
        if not event_size_df.empty:
            size_row = event_size_df.iloc[0]
            size_preview_df = pd.DataFrame(
                [
                    {"segment": "all", "avg_length": size_row.get("all_avg_length") or 0},
                    {"segment": "bot", "avg_length": size_row.get("bot_avg_length") or 0},
                    {"segment": "human", "avg_length": size_row.get("human_avg_length") or 0},
                ]
            )
            size_fig = px.bar(
                size_preview_df,
                x="segment",
                y="avg_length",
                title="Average Event Size (Length Delta)",
            )
            size_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(size_fig, width="stretch")
        else:
            st.info("No event-size data available yet.")

    # patrolled preview
    with tab3:
        if not patrolled_df.empty:
            patrolled_preview_df = patrolled_df.melt(
                id_vars=["user_type"],
                value_vars=["patrolled_count", "unpatrolled_count"],
                var_name="patrol_status",
                value_name="status_count",
            )
            patrolled_preview_df["patrol_status"] = patrolled_preview_df["patrol_status"].replace(
                {
                    "patrolled_count": "patrolled",
                    "unpatrolled_count": "unpatrolled",
                }
            )
            patrolled_fig = px.bar(
                patrolled_preview_df,
                x="user_type",
                y="status_count",
                color="patrol_status",
                barmode="stack",
                title="Patrolled vs Unpatrolled (Bots vs Humans)",
            )
            patrolled_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(patrolled_fig, width="stretch")
        else:
            st.info("No patrolled/unpatrolled data for today yet.")


@st.cache_data(ttl=10)
def get_redis_snapshots():
    """Fetch and cache Redis rolling-window metrics for realtime cards/charts."""
    # init redis client
    client = get_redis_client()

    # aggregate windows
    aggregate_windows = aggregate_redis_windows(client, [5, 60])
    aggregates_5m = aggregate_windows.get(5, {})
    aggregates_1h = aggregate_windows.get(60, {})

    # get top users
    top_users_5m = aggregate_top_users_window(client, 5)

    # calculate spike score
    one_hour_total = aggregates_1h.get("events:total", 0)
    five_min_total = aggregates_5m.get("events:total", 0)
    spike_score = 0.0
    if one_hour_total > 0:
        baseline_five_min = one_hour_total / 12
        spike_score = five_min_total / baseline_five_min if baseline_five_min > 0 else 0.0

    return aggregates_5m, aggregates_1h, top_users_5m, spike_score


def render_redis_section():
    """Render the Redis realtime metrics section."""
    # get redis snapshots
    st.subheader("Redis Realtime Metrics")
    aggregates_5m, aggregates_1h, top_users_5m, spike_score = get_redis_snapshots()

    # create columns for metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Events (5m)", aggregates_5m.get("events:total", 0))
    c2.metric("Events (1h)", aggregates_1h.get("events:total", 0))
    c3.metric("Spike Score (5m vs 1h baseline)", f"{spike_score:.2f}x")

    # plot top users
    if not top_users_5m.empty:
        top_users_redis_fig = px.bar(top_users_5m, x="user", y="events", title="Top Users (Last 5 Minutes)")
        top_users_redis_fig.update_xaxes(categoryorder="total descending")
        top_users_redis_fig.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(top_users_redis_fig, width="stretch")
    else:
        st.info("No top-user data available in Redis for the last 5 minutes.")


def main():
    """Configure and render the Streamlit dashboard layout and controls."""
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
