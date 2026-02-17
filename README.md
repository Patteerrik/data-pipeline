# Data Pipeline (Python + Postgres)

A production-style data pipeline that ingests data, transforms it,
stores it in PostgreSQL, and validates behavior with tests.

---

## Stack

- Python
- pandas
- PostgreSQL (Docker)
- SQLAlchemy
- pytest

---

## Project Structure

- `data/raw/` – input data
- `pipeline/transform.py` – validation + type casting
- `pipeline/load.py` – DB init + idempotent upsert
- `tests/` – unit & integration tests

---

## Quickstart (Recommended)

Make sure you have Docker and Docker Compose installed.

```bash
make reset   # fresh database + rebuild containers
make test    # run test suite
make run     # run the pipeline

---