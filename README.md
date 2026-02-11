# Data Pipeline (Python + Postgres)

A production-style data pipeline that ingests data, transforms it, stores it in
PostgreSQL, and validates behavior with tests.

---

## Stack
- Python, pandas
- PostgreSQL (Docker)
- SQLAlchemy
- pytest

---

## Project structure
- `data/raw/` input data
- `pipeline/transform.py` schema + type casting + validation
- `pipeline/load.py` DB init + idempotent load
- `tests/` unit/integration tests

---

## Quickstart
1) Start database
```bash
docker compose up -d

---

## Run with Docker
```bash
docker compose up -d --build
docker compose run --rm pipeline
docker compose run --rm tests
```
