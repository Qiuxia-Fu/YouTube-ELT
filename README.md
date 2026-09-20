# YouTube-ELT

An end-to-end, orchestrated ELT pipeline that extracts YouTube channel/video data via the YouTube Data API, loads it into PostgreSQL, transforms it into an analytics-ready schema, and validates it with automated data quality checks — fully containerized and deployed via CI/CD.

---

## Overview

This pipeline pulls video-level metadata (views, likes, comments, duration, upload date) for a given YouTube channel, lands the raw data, transforms it into a clean dimensional model, and automatically checks the result for data quality issues before it's considered "done." The whole flow is orchestrated with Apache Airflow and runs in Docker containers, with tests and deployment automated through GitHub Actions.

## Architecture

```
YouTube Data API
      │
      ▼
┌─────────────────┐
│  produce_json    │  DAG 1 — scheduled daily @ 10:00 UTC
│  get_playlist_id │
│  → get_video_ids │
│  → extract_video_data
│  → save_to_json  │
└────────┬─────────┘
         │ triggers
         ▼
┌─────────────────┐
│   update_db      │  DAG 2 — triggered by DAG 1
│  staging_table   │  raw JSON → staging.yt_api
│  → core_table    │  staging → core.yt_api (+ Video_Type classification)
└────────┬─────────┘
         │ triggers
         ▼
┌─────────────────┐
│  data_quality    │  DAG 3 — triggered by DAG 2
│  SODA: staging   │
│  → SODA: core    │
└──────────────────┘
```

The three stages run as separate Airflow DAGs chained together with `TriggerDagRunOperator`, rather than one monolithic DAG — each stage (extract, load/transform, validate) can be run, tested and monitored independently.

## Tech Stack

| Layer | Tools |
|---|---|
| Extraction | Python, YouTube Data API v3 |
| Storage | PostgreSQL (staging + core schemas) |
| Orchestration | Apache Airflow 2.9.2 |
| Containerization | Docker, Docker Compose |
| Data Quality | SODA |
| Testing | pytest (unit + integration) |
| CI/CD | GitHub Actions |

## Data Model

Raw video metadata lands in `staging.yt_api`, then flows into `core.yt_api` with an added derived field:

| Column | Description |
|---|---|
| `Video_ID` | Unique video identifier |
| `Video_Title` | Video title |
| `Upload_Date` | Publish timestamp |
| `Duration` | Video length |
| `Video_Views` | View count |
| `Likes_Count` | Like count |
| `Comments_Count` | Comment count |
| `Video_Type` *(core only)* | Derived classification — Shorts vs. long-form, based on duration |

## Data Quality Checks

Every run validates both the `staging` and `core` schemas with SODA before the pipeline is considered successful:

- `missing_count("Video_ID") = 0` — no null video IDs
- `duplicate_count("Video_ID") = 0` — no duplicate videos
- `Likes_Count > Video_Views` → flagged (likes should never exceed views)
- `Comments_Count > Video_Views` → flagged (comments should never exceed views)

## Testing

- **Unit tests** (`tests/unit_test.py`) — isolated component tests using `pytest` fixtures (`tests/conftest.py`) to mock external dependencies like the API key
- **Integration tests** (`tests/integration_test.py`) — verify DAG structure and task wiring

## CI/CD

`.github/workflows/ci-cd_yt-elt.yaml` runs on every push:
1. Builds and pushes the Docker image to Docker Hub
2. Runs the full `pytest` suite against the pipeline code

## Project Structure

```
YouTube-ELT/
├── dags/
│   ├── api/              # YouTube API extraction logic
│   ├── datawarehouse/    # staging/core table load & transform logic
│   ├── dataquality/      # SODA check trigger logic
│   └── main.py           # DAG definitions (produce_json, update_db, data_quality)
├── docker/
│   └── postgres/         # Postgres init scripts (multi-database setup)
├── include/
│   └── soda/             # checks.yml, configuration.yml
├── tests/
│   ├── conftest.py
│   ├── unit_test.py
│   └── integration_test.py
├── .github/workflows/
│   └── ci-cd_yt-elt.yaml
└── docker-compose.yaml
```

## Running Locally

```bash
git clone https://github.com/Qiuxia-Fu/YouTube-ELT.git
cd YouTube-ELT
docker compose up -d
```

This spins up Postgres, Redis, and the Airflow webserver/scheduler/worker. Once healthy, trigger the `produce_json` DAG from the Airflow UI (`localhost:8080`) to run the full pipeline end-to-end.

## Acknowledgements

Built while completing [Start Your Data Engineering Journey](https://www.udemy.com/course/start-your-data-engineering-journey-project-based-learning/) on Udemy, extended with my own debugging, testing and CI/CD setup along the way.
