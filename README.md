# timescale-access

[![Docs](https://img.shields.io/badge/docs-online-blue)](https://jpbeerhold.github.io/timescale-access/)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)
![CI](https://github.com/jpbeerhold/timescale-access/actions/workflows/ci.yaml/badge.svg)
![Docs Build](https://github.com/jpbeerhold/timescale-access/actions/workflows/docs.yaml/badge.svg)
![Docker](https://img.shields.io/badge/GHCR-image-blue?logo=docker)
![Lint](https://github.com/jpbeerhold/timescale-access/actions/workflows/lint.yaml/badge.svg)

A lightweight and user-friendly Python wrapper for TimescaleDB/PostgreSQL, designed for time‑series data ingestion, schema management, and convenient querying using SQLAlchemy.  
Comes with a full development environment (VS Code Devcontainer + TimescaleDB), automated tests, and deployable Docker images via GHCR.

---

## 🗄️ Key Features

- High‑level `TimescaleAccess` client for TimescaleDB  
- Automatic hypertable creation and column inference  
- Fast DataFrame inserts with `insert_hypertable()` and `insert_hypertable_on_conflict()`  
- Schema and table utilities (`ensure_schema_exists`, `get_table_names`, …)  
- Advanced SQL analysis helpers:
  - Missing/non‑consecutive sequence detection  
  - Duplicate row detection  
  - Null summary function generation  
  - Hypertable size calculations  
- Complete test suite using real TimescaleDB (via docker-compose)  
- Devcontainer with automated setup for reproducible development  
- GHCR-ready runtime Docker image  

---

## 📦 Project Structure

```
src/timescale_access/
    client.py
    engine.py
    read.py
    write.py
    analysis.py

tests/
    test_client.py
    config.py
    conftest.py

docs/
    source/
    build/

.devcontainer/
docker-compose.yaml
Dockerfile
pyproject.toml
```

---

## 📥 Installation

### From source  
```bash
pip install -e .[dev]
```

### From GitHub  
```bash
pip install git+https://github.com/jpbeerhold/timescale-access.git
```

### Pull runtime image from GHCR  
```bash
docker pull ghcr.io/jpbeerhold/timescale-access:latest
```

---

## ⚡ Quickstart Example

```python
from timescale_access.client import TimescaleAccess
import pandas as pd
from datetime import datetime

db = TimescaleAccess("postgresql://user:pass@localhost:5432/postgres")

# Ensure schema exists
db.ensure_schema_exists("raw_data")

# Insert time-series data
df = pd.DataFrame([{
    "instrument_name": "BTC-PERPETUAL",
    "trade_seq": 10001,
    "timestamp": datetime.utcnow(),
    "value": 42.0,
}])

db.insert_hypertable("raw_data", "btc_trades", df)

# Read table back
df_out = db.get_table("raw_data", "btc_trades")
print(df_out)
```

For the full API reference, visit the documentation:  
👉 **https://jpbeerhold.github.io/timescale-access/**

---

## 🧑‍💻 Development Environment (VS Code Devcontainer)

This project ships with a complete dev environment:

- VS Code Devcontainer (`.devcontainer/`)
- docker-compose launching a real TimescaleDB instance
- Automatic installation of development dependencies

Start the environment:

```
Dev Containers: Rebuild and Reopen in Container
```

---

## 🧪 Running Tests

```
pytest
```

Tests automatically connect to the TimescaleDB instance from docker-compose.

---

## 📚 Documentation

Build locally:

```bash
cd docs
make html
```

Online docs:

👉 **https://jpbeerhold.github.io/timescale-access/**

---

## 🐳 Docker / GHCR Usage

Pull the production image:

```bash
docker pull ghcr.io/jpbeerhold/timescale-access:latest
```

Run:

```bash
docker run --rm ghcr.io/jpbeerhold/timescale-access:latest
```

---

## 🔧 API Overview

Main entry point:

- `TimescaleAccess`
  - `insert_hypertable()`
  - `insert_hypertable_on_conflict()`
  - `get_table()`
  - `get_column_names()`
  - `get_schemas()`
  - `get_hypertable_size()`
  - `get_missing_trade_seq()`
  - `ensure_schema_exists()`
  - … and more

See the full reference in the documentation.

---

## 📄 License

MIT License  
© 2025 Jannis Philipp Beerhold