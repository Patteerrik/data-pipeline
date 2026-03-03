# Data Pipeline (Python + PostgreSQL)

A small production-style data pipeline built to understand how data flows
from raw input to a validated and idempotent database load.

The goal of this project was not just to move data, but to understand:

- How to design a clean load process
- How to prevent duplicates using UPSERT logic
- How to test database behavior against a real PostgreSQL instance

---

## 🚀 Tech Stack

- Python
- pandas
- PostgreSQL (Docker)
- SQLAlchemy
- pytest
- Docker Compose
- Makefile

---

## 🧠 What This Project Demonstrates

This project focuses on practical data engineering fundamentals:

- Idempotent data loads using PostgreSQL `ON CONFLICT DO UPDATE`
- Basic data validation and type casting before insert
- Database constraints (primary key + uniqueness)
- Integration tests running against a real database (not mocks)
- A reproducible environment using Docker

The pipeline can be executed multiple times without creating duplicate records.

---

## 🏗 How It Works

Raw CSV → Transform → Load (UPSERT) → Core Table

1. Raw order data is read from CSV.
2. The transform layer validates types and cleans the data.
3. Cleaned data is inserted into PostgreSQL.
4. If an order already exists, it is updated instead of duplicated.

This makes the pipeline safe to run repeatedly.

---

## 📁 Project Structure

```
data/
└── raw/                # Input CSV data

pipeline/
├── transform.py        # Validation and type casting
├── load.py             # Database initialization and idempotent upsert
└── run.py              # Entry point to execute pipeline

db/
└── models.py           # SQLAlchemy models and constraints

tests/
├── test_load_idempotent.py
├── test_upsert_updates.py
└── test_source_column.py

Makefile                # Convenience commands
docker-compose.yml      # PostgreSQL container setup
```

---

## ⚡ Quickstart

Make sure Docker is installed.

```bash
make reset   # Rebuild database from scratch
make test    # Run full test suite
make run     # Execute pipeline
```

---

## 🧪 Testing Approach

The tests are designed to validate actual database behavior.

Each test run:

- Recreates tables
- Clears data between runs
- Verifies idempotency
- Verifies update behavior on conflict

The goal was to understand how integration testing works when a real database is involved.

---

## 🎯 Why I Built This

I wanted to better understand:

- How idempotent pipelines work
- How database constraints protect data integrity
- How to test real database behavior instead of mocking it
- How Docker can be used to create a reproducible setup

This project helped me connect SQL, Python, and database design into one coherent system.