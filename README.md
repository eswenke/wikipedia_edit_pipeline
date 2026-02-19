# Wikipedia Edit Pipeline

Production-style data engineering project that ingests live event streams from the MediaWiki Recent Changes stream, persists raw data for historical analysis, computes low-latency real-time metrics, and presents insights through an interactive analytics dashboard.


## Project Summary

This project simulates a practical analytics stack used in event-driven systems:

- **Streaming ingestion** from Wikimedia's Server-Sent Events (SSE) feed
- **Dual storage strategy** for both real-time and historical analysis
  - **Redis** for rolling-window counters and leaderboard-style metrics
  - **PostgreSQL** for flexible SQL analytics over retained raw events
- **Interactive dashboarding** via Streamlit + Plotly
- **Operational concerns** handled in code (reconnect/backoff logic, retention pruning, lock-aware DB maintenance)

The goal is to move beyond simple event counting toward behavioral analytics (e.g., burst detection, actor mix, edit-type distribution, and quality-adjacent proxies).



## Why This Project Matters

This repository demonstrates:

- End-to-end data pipeline ownership (ingest -> store -> analyze -> visualize)
- Practical use of both cache/metrics stores and relational analytics stores
- Experience with failure-aware stream consumption (connection issues, retry policy)
- Thoughtful analytics design under real-world data constraints
- Iterative product thinking: building fast feedback loops first, then maturing architecture



## Architecture Overview

1. **Ingestion (`src/pipeline.py`)**
   - Connects to the Wikimedia recent changes stream
   - Parses and validates incoming events
   - Writes each event to:
     - Redis metrics aggregations
     - PostgreSQL raw events table

2. **Real-Time Metrics (`src/redis_manager.py`)**
   - Tracks total events, type mix, namespace/log-type counts
   - Tracks bot/human and minor/major edit slices
   - Maintains top-user sorted sets
   - Maintains minute-bucket keys for rolling windows (5m/1h)

3. **Historical Analytics (`src/psql_manager.py`, `src/psql_analytics.py`)**
   - Stores raw event-level rows in Postgres
   - Runs SQL-powered analytics (top users/wikis, time series, distributions)
   - Supports retention pruning and lock-aware maintenance workflows

4. **Dashboard (`src/streamlit_app.py`)**
   - PostgreSQL section for historical/deeper analytics
   - Redis section for low-latency operational snapshots
   - Optimized Redis fetch paths using scan + pipelined reads



## Core Tech Stack

- **Language**: Python
- **Streaming/HTTP**: asyncio, aiohttp
- **Datastores/Analytics**: Redis, PostgreSQL
- **DataFrames**: pandas
- **Visualization/UI**: Streamlit, Plotly
- **Containerization (In Progress)**: Docker, Docker Compose
- **Orchestration (TBD)**: Airflow
- **Pipeline Monitoring (TBD)**: Grafana
- **Full Stack (TBD)**: FastAPI, React


## Current Feature Highlights

- Live SSE ingest with retry/backoff handling
- Real-time rolling-window metrics (5m/1h) and spike scoring
- Top contributor tracking in Redis and Postgres
- Gap-filled SQL time-series query support for chart continuity
- Bot/human and patrolled/unpatrolled slices
- Streamlit controls for query windows and segmented views



## Getting Started (Local)

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Configure environment

Create a `.env` file provide:

- PostgreSQL connection settings
- Redis connection settings (local or Redis Cloud)

### 3) Run ingestion pipeline

```bash
python src/pipeline.py
```

### 4) Run dashboard

```bash
streamlit run src/streamlit_app.py
```



## Future Development Roadmap

### TBD Next

- Create redis_analytics file for further separation of concerns
- Harden Docker setup for long-running local/prod-like operation
- Add health checks and better service observability (structured logs + lightweight monitoring)
- Add dashboard and pipeline monitoring via Grafana
- Expand SQL analytics coverage (distribution, segmentation, and comparative metrics)
- Add orchestration with Apache Airflow for scheduled jobs and workflow management

### Full Stack

- Introduce a **FastAPI backend** to formalize data-access and analytics endpoints
- Move from Streamlit-only UX to a more established frontend/backend split:
  - **Backend**: FastAPI for API contracts, auth-ready architecture, and service boundaries
  - **Frontend**: dedicated web client (e.g., React/Next.js) for richer product-level UX
- Add background scheduling and queue-based processing where appropriate
