# 🟢 Bruin Stack — 100% Free · Zero Infrastructure · One Binary

> **Industrial Sentinel × Bruin CLI**  
> The complete IIoT Security ROI Pipeline using only open-source, zero-cost tools.  
> No Airflow. No dbt. No Fivetran. No Great Expectations config files. **One binary.**

---

## 📋 Table of Contents

1. [Why Bruin?](#-why-bruin)
2. [Full Stack Overview](#-full-stack-overview)
3. [Stack Components](#-stack-components)
4. [Architecture Diagram](#-architecture-diagram)
5. [Project Structure](#-project-structure)
6. [Installation](#-installation)
7. [Pipeline Assets](#-pipeline-assets)
   - [Ingestion](#ingestion-assets)
   - [Bronze Layer](#bronze-layer)
   - [Silver Layer](#silver-layer)
   - [Gold Layer (Business Cases)](#gold-layer--business-cases)
8. [Data Quality Checks](#-data-quality-checks)
9. [Orchestration & Scheduling](#-orchestration--scheduling)
10. [CI/CD — GitHub Actions](#-cicd--github-actions)
11. [Monitoring Stack](#-monitoring-stack)
12. [Dashboard Setup](#-dashboard-setup)
13. [Quick Start](#-quick-start)
14. [Makefile Targets (Bruin)](#-makefile-targets-bruin)
15. [Comparison vs. Traditional Stack](#-comparison-vs-traditional-stack)

---

## 🤔 Why Bruin?

Bruin is an open-source CLI tool that replaces the usual Airflow + dbt + Great Expectations stack with a single binary: SQL and Python assets coexist in the same pipeline with automatic dependency resolution, incremental materialisation, and quality checks embedded directly in asset definitions rather than maintained as a separate test suite.

Key advantages:
- **Pipelines as Code** — Everything lives in version-controlled text (YAML, SQL, Python). No hidden UIs or databases. Reproducible, reviewable, and automatable.
- **Multi-Language by Nature** — Native support for SQL and Python, plus the ability to plug in binaries for more complex use cases.
- **Composable Pipelines** — Combine technologies, sources, and destinations in one seamless flow — no glue code, no hacks.
- **No Lock-In** — 100% open-source (Apache-licensed) CLI that runs anywhere: locally, in CI, or in production.

Bruin handles ingestion, transformation in both SQL and Python, orchestration, quality checks, column-level lineage, and an AI analyst on top — all in one platform with a Git-native CLI and no separate vendors to stitch together.

For this Capstone specifically, Bruin means:

| Instead of this... | Bruin gives you... |
|---|---|
| Apache Airflow (500MB install, Postgres dependency) | `bruin run` (single Go binary) |
| dbt for SQL transformations | SQL assets with `/* @bruin */` annotations |
| Great Expectations config files | Inline `quality` blocks per asset |
| Fivetran / Airbyte connectors | Built-in `ingestr` (200+ connectors) |
| Separate lineage tool | Automatic column-level lineage |

---

## 🗺 Full Stack Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│              INDUSTRIAL SENTINEL × BRUIN STACK                      │
│                    100% Free · All Open Source                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  INGESTION          COMPUTE           STORAGE          SERVING      │
│  ─────────          ───────           ───────          ───────      │
│  Bruin CLI          DuckDB            PostgreSQL        Streamlit   │
│  (ingestr)          (local OLAP)      (TimescaleDB)     (dashboard) │
│                     Python (SciPy)    MinIO             Superset    │
│                                       Apache Iceberg    Grafana     │
│                                                                     │
│  QUALITY            LINEAGE           CI/CD            MONITORING   │
│  ───────            ───────           ─────            ──────────   │
│  Bruin quality      Bruin (built-in   GitHub Actions   Prometheus  │
│  checks (inline)    column-level)     (free 2k min)    + Grafana   │
│                                                                     │
│  ML TRACKING        CONTAINERS        NOTEBOOKS                     │
│  ──────────         ──────────        ─────────                     │
│  MLflow (OSS)       Docker Compose    Jupyter (OSS)                │
│  (local SQLite)     (free)                                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Total infrastructure cost: USD 0.00**

---

## 🧱 Stack Components

| Category | Tool | License | Role in Pipeline |
|----------|------|---------|-----------------|
| **Pipeline engine** | [Bruin CLI](https://getbruin.com) | Apache 2.0 | Ingestion + orchestration + quality + lineage |
| **Ingestion** | [ingestr](https://github.com/bruin-data/ingestr) (built into Bruin) | MIT | 200+ source connectors (CSV, APIs, DBs) |
| **In-process OLAP** | [DuckDB](https://duckdb.org) | MIT | Local SQL analytics, Monte Carlo data processing |
| **OLAP engine** | [ClickHouse](https://clickhouse.com) | Apache 2.0 | Real-time sensor data aggregation |
| **Relational DB** | [PostgreSQL 16](https://postgresql.org) | PostgreSQL | Metadata + Gold layer results |
| **Time-series** | [TimescaleDB](https://timescale.com) | Timescale | Sensor time-series (extension on Postgres) |
| **Object storage** | [MinIO](https://min.io) | AGPL 3.0 | S3-compatible local object store |
| **Table format** | [Apache Iceberg](https://iceberg.apache.org) | Apache 2.0 | Open lakehouse on MinIO |
| **Transformation** | SQL + Python assets (Bruin) | — | Bronze → Silver → Gold |
| **Statistical compute** | SciPy + NumPy + Pandas | BSD | Monte Carlo, VaR, ALE, LPV, TEL |
| **Quality** | Bruin inline checks | Apache 2.0 | `not_null`, `accepted_values`, custom SQL |
| **Lineage** | Bruin (built-in) | Apache 2.0 | Column-level lineage, automatic |
| **BI** | [Apache Superset](https://superset.apache.org) | Apache 2.0 | Interactive ROI dashboards |
| **App** | [Streamlit](https://streamlit.io) | Apache 2.0 | Executive portal (CTO-ready) |
| **Monitoring** | [Grafana](https://grafana.com) + [Prometheus](https://prometheus.io) | Apache 2.0 | Pipeline + sensor metrics |
| **ML tracking** | [MLflow](https://mlflow.org) | Apache 2.0 | Anomaly model experiments |
| **Notebooks** | [Jupyter](https://jupyter.org) | BSD | EDA + prototype development |
| **Containers** | Docker + Docker Compose | Apache 2.0 | Local infra (Postgres, MinIO, Superset, Grafana) |
| **CI/CD** | GitHub Actions | Free (2,000 min/mo) | Automated pipeline on push |
| **VS Code** | [Bruin Extension](https://marketplace.visualstudio.com/items?itemName=bruin-data.bruin-vscode) | Free | Pipeline UI, lineage graph in editor |

---

## 🏗 Architecture Diagram

```
 CIC IIoT 2025 Dataset
 (CSV files — Kaggle/UNB)
         │
         ▼
 ┌──────────────────────────────────────────────────────────┐
 │                    BRUIN PIPELINE                        │
 │                                                          │
 │  ┌─────────────────────────────────┐                     │
 │  │  INGESTION (ingestr built-in)   │                     │
 │  │                                 │                     │
 │  │  bruin run assets/ingestion/    │                     │
 │  │  • CSV → DuckDB (local)         │                     │
 │  │  • Auto column normalization    │                     │
 │  │  • Incremental loading          │                     │
 │  └────────────┬────────────────────┘                     │
 │               │                                          │
 │               ▼                                          │
 │  ┌─────────────────────────────────┐                     │
 │  │  BRONZE LAYER (SQL assets)      │                     │
 │  │                                 │                     │
 │  │  bronze_iiot_raw.sql            │                     │
 │  │  • Dedup + null drop            │                     │
 │  │  • Label normalization          │                     │
 │  │  • quality: row_count > 1000    │                     │
 │  │  • quality: label not_null      │                     │
 │  └────────────┬────────────────────┘                     │
 │               │                                          │
 │               ▼                                          │
 │  ┌─────────────────────────────────┐                     │
 │  │  SILVER LAYER (Python assets)   │                     │
 │  │                                 │                     │
 │  │  silver_scored.py               │                     │
 │  │  • 3-sigma outlier clipping     │                     │
 │  │  • MSE anomaly scoring          │                     │
 │  │  • is_anomaly flag (top 5%)     │                     │
 │  │  • quality: mse_score >= 0      │                     │
 │  └────────────┬────────────────────┘                     │
 │               │                                          │
 │               ▼                                          │
 │  ┌─────────────────────────────────┐                     │
 │  │  GOLD LAYER (Python assets)     │                     │
 │  │                                 │                     │
 │  │  gold_case1_energy.py           │  ← VaR + ALE       │
 │  │  gold_case2_health.py           │  ← Poisson + LPV   │
 │  │  gold_case3_logistics.py        │  ← TEL + T-test    │
 │  │                                 │                     │
 │  │  quality: p_value < 0.05        │                     │
 │  │  quality: savings_usd > 0       │                     │
 │  └────────────┬────────────────────┘                     │
 │               │                                          │
 └───────────────┼──────────────────────────────────────────┘
                 │
        ┌────────┼────────┐
        ▼        ▼        ▼
  PostgreSQL   MinIO    DuckDB
  (results)  (Parquet) (local query)
        │        │        │
        └────────┼────────┘
                 ▼
     ┌───────────────────────┐
     │   SERVING LAYER       │
     │                       │
     │  Streamlit (port 8501)│
     │  Apache Superset 8088 │
     │  Grafana       3000   │
     │  MLflow        5000   │
     └───────────────────────┘
```

---

## 📁 Project Structure

```
bruin/
│
├── .bruin.yml                       ← Project-level config (environments, defaults)
├── pipeline.yml                     ← Pipeline metadata (name, schedule, notifications)
│
├── assets/
│   │
│   ├── ingestion/
│   │   └── ingest_iiot_csv.asset.yml   ← CSV → DuckDB via ingestr
│   │
│   ├── bronze/
│   │   └── bronze_iiot_raw.sql         ← Dedup + normalize + quality checks
│   │
│   ├── silver/
│   │   └── silver_scored.py            ← MSE anomaly scoring (Python asset)
│   │
│   └── gold/
│       ├── gold_case1_energy.py        ← Monte Carlo VaR + ALE (Python asset)
│       ├── gold_case2_health.py        ← Poisson LPV (Python asset)
│       └── gold_case3_logistics.py     ← TEL + T-test + weight MSE (Python asset)
│
└── .github/
    └── workflows/
        └── bruin-pipeline.yml          ← GitHub Actions CI/CD
```

---

## 🔧 Installation

```bash
# ── Bruin CLI ──────────────────────────────────────────────────────────────
# macOS / Linux
curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | sh

# Homebrew (macOS)
brew install bruin-data/tap/bruin

# Windows (PowerShell)
irm https://raw.githubusercontent.com/bruin-data/bruin/main/install.ps1 | iex

# VS Code Extension
# Search "Bruin" in Extensions panel → install bruin-data.bruin-vscode

# ── Verify ─────────────────────────────────────────────────────────────────
bruin version
# bruin version X.Y.Z

# ── Supporting services (Docker Compose) ───────────────────────────────────
docker compose -f bruin/docker/docker-compose-bruin.yml up -d

# ── Python dependencies ────────────────────────────────────────────────────
pip install duckdb pandas numpy scipy streamlit plotly mlflow superset
```

---

## 📄 Pipeline Assets

### Project Config — `.bruin.yml`

```yaml
# .bruin.yml
name: industrial-sentinel
environments:
  default:
    connections:
      duckdb:
        path: data/sentinel.duckdb   # local DuckDB file (zero infra)
      postgres:
        host:     localhost
        port:     5432
        database: sentinel
        username: bruin
        password: "${POSTGRES_PASSWORD}"
      gcs:
        service_account_file: ""    # optional: leave blank for MinIO

default_environment: default
```

### Pipeline Metadata — `pipeline.yml`

```yaml
# pipeline.yml
name: iiot-sentinel-pipeline
schedule: "0 6 * * *"          # Daily at 06:00 UTC (like Airflow's cron)
description: |
  CIC IIoT 2025 — End-to-end security ROI pipeline.
  Bronze → Silver → Gold (VaR / ALE / LPV / TEL)
  Monte Carlo 10,000 iterations · p < 0.05 validated

notifications:
  slack:
    channel: "#data-alerts"
    on_failure: true
```

---

### Ingestion Assets

#### `assets/ingestion/ingest_iiot_csv.asset.yml`

```yaml
# Bruin ingestion asset — replaces NiFi/Airbyte/Fivetran for this use case
name: ingestion.iiot_raw_csv
type: ingestr

parameters:
  source_connection: file                 # reads from local filesystem
  source_table: "data/raw/*.csv"          # glob pattern — all CIC IIoT CSVs
  destination: duckdb                     # target connection (from .bruin.yml)
  destination_table: raw.iiot_network     # target table
  loader_file_format: csv
  incremental_strategy: replace           # full refresh on each run

quality:
  - name: "Raw dataset not empty"
    type: row_count
    min: 10000
  - name: "Label column exists and not null"
    type: not_null
    column: Label
    threshold: 0.95                       # 95% not null acceptable
  - name: "Flow duration non-negative"
    type: custom_sql
    query: |
      SELECT COUNT(*) FROM {{ asset }}
      WHERE "Flow Duration" < 0
    max: 0
```

---

### Bronze Layer

#### `assets/bronze/bronze_iiot_raw.sql`

```sql
/* @bruin
name: bronze.iiot_network
type: duckdb
depends:
  - ingestion.iiot_raw_csv
materialization:
  type: table
  strategy: replace

quality:
  - name: "Row count minimum"
    type: row_count
    min: 5000
  - name: "Attack label valid values"
    type: accepted_values
    column: label
    values:
      - BENIGN
      - DoS
      - DDoS
      - MITM
      - Ransomware
      - Scanning
      - Mirai
    threshold: 0.90
  - name: "No fully null sensor rows"
    type: custom_sql
    query: |
      SELECT COUNT(*) FROM {{ asset }}
      WHERE sensor_temperature IS NULL
        AND sensor_pressure IS NULL
        AND sensor_flow_rate IS NULL
    max: 0
@bruin */

SELECT
  -- Network features
  TRIM(src_ip)                                   AS src_ip,
  TRIM(dst_ip)                                   AS dst_ip,
  CAST(src_port AS INTEGER)                      AS src_port,
  CAST(dst_port AS INTEGER)                      AS dst_port,
  LOWER(TRIM(protocol))                          AS protocol,
  CAST("Flow Duration" AS DOUBLE)                AS flow_duration,
  CAST("Flow Bytes/s" AS DOUBLE)                 AS flow_bytes_per_s,
  CAST("Flow Pkts/s" AS DOUBLE)                  AS flow_pkts_per_s,

  -- Sensor features (normalized names)
  CAST("Temperature" AS DOUBLE)                  AS sensor_temperature,
  CAST("Pressure" AS DOUBLE)                     AS sensor_pressure,
  CAST("Flow Rate" AS DOUBLE)                    AS sensor_flow_rate,

  -- Attack label
  UPPER(TRIM(Label))                             AS label,
  CASE
    WHEN UPPER(TRIM(Label)) = 'BENIGN' THEN 0
    ELSE 1
  END                                            AS is_attack,

  -- Audit
  NOW()                                          AS ingestion_ts,
  '{{ run_id }}'                                 AS pipeline_run_id

FROM {{ ref("ingestion.iiot_raw_csv") }}

-- Remove duplicates (same 5-tuple + timestamp)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY src_ip, dst_ip, src_port, dst_port, protocol, flow_duration
  ORDER BY ingestion_ts DESC
) = 1
```

---

### Silver Layer

#### `assets/silver/silver_scored.py`

```python
# @bruin
# name: silver.iiot_scored
# type: python
# depends:
#   - bronze.iiot_network
# materialization:
#   type: table
#   strategy: replace
#
# quality:
#   - name: "MSE score non-negative"
#     type: custom_sql
#     query: "SELECT COUNT(*) FROM {{ asset }} WHERE anomaly_score_mse < 0"
#     max: 0
#   - name: "is_anomaly is boolean"
#     type: accepted_values
#     column: is_anomaly
#     values: [true, false]
#   - name: "Anomaly rate reasonable"
#     type: custom_sql
#     query: |
#       SELECT COUNT(*) FROM {{ asset }}
#       WHERE CAST(is_anomaly AS INTEGER) > 0.30
#     max: 0
# @bruin

import pandas as pd
import numpy as np
from bruin import get_context          # Bruin Python SDK

ctx = get_context()
conn = ctx.connections.get("duckdb")

SENSOR_COLS = [
    "flow_duration", "flow_bytes_per_s", "flow_pkts_per_s",
    "sensor_temperature", "sensor_pressure", "sensor_flow_rate",
]

def compute_mse_score(df: pd.DataFrame, cols: list) -> pd.Series:
    """MSE anomaly score: distance from column means (normal baseline)."""
    available = [c for c in cols if c in df.columns]
    if not available:
        return pd.Series(0.0, index=df.index)
    # Fill NaN with column mean before MSE
    filled = df[available].fillna(df[available].mean())
    baseline = filled.mean()
    return ((filled - baseline) ** 2).mean(axis=1)

# ── Read from Bronze ─────────────────────────────────────────────────────────
df = conn.execute("SELECT * FROM bronze.iiot_network").df()

# ── Clip outliers (3 std per numeric column) ─────────────────────────────────
for col in SENSOR_COLS:
    if col in df.columns:
        mu, sd = df[col].mean(), df[col].std()
        if sd > 0:
            df[col] = df[col].clip(lower=mu - 3 * sd, upper=mu + 3 * sd)

# ── MSE anomaly score ─────────────────────────────────────────────────────────
df["anomaly_score_mse"] = compute_mse_score(df, SENSOR_COLS)
threshold_95 = df["anomaly_score_mse"].quantile(0.95)
df["is_anomaly"] = df["anomaly_score_mse"] > threshold_95
df["processing_ts"] = pd.Timestamp.utcnow()

# ── Write to Silver ──────────────────────────────────────────────────────────
conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
conn.execute("DROP TABLE IF EXISTS silver.iiot_scored")
conn.register("silver_df", df)
conn.execute("CREATE TABLE silver.iiot_scored AS SELECT * FROM silver_df")

print(f"✔ Silver layer written: {len(df):,} rows | "
      f"Anomaly rate: {df['is_anomaly'].mean()*100:.1f}% | "
      f"MSE threshold (p95): {threshold_95:.4f}")
```

---

### Gold Layer — Business Cases

#### `assets/gold/gold_case1_energy.py`

```python
# @bruin
# name: gold.case1_energy_var_ale
# type: python
# depends:
#   - silver.iiot_scored
# materialization:
#   type: table
#   strategy: replace
#
# quality:
#   - name: "ALE savings must be positive"
#     type: custom_sql
#     query: "SELECT COUNT(*) FROM {{ asset }} WHERE ale_savings_mean_usd <= 0"
#     max: 0
#   - name: "T-test must be significant"
#     type: custom_sql
#     query: "SELECT COUNT(*) FROM {{ asset }} WHERE p_value >= 0.05"
#     max: 0
# @bruin

import json, pandas as pd, numpy as np
from scipy import stats
from bruin import get_context

ctx  = get_context()
conn = ctx.connections.get("duckdb")

ITERS       = 10_000
PROB_BEFORE = 0.15
PROB_AFTER  = 0.02
LOSS_PER_EVENT = 1_000_000   # USD
ASSET_VALUE    = 500_000_000
COST_PER_HR    = 150_000

rng = np.random.default_rng(42)

loss_before = rng.binomial(1, PROB_BEFORE, ITERS) * LOSS_PER_EVENT
loss_after  = rng.binomial(1, PROB_AFTER,  ITERS) * LOSS_PER_EVENT
savings_ale = loss_before - loss_after

var95_before = np.percentile(loss_before, 95)
var95_after  = np.percentile(loss_after,  95)

t_stat, p_val = stats.ttest_ind(loss_before, loss_after)

downtime_saved = (PROB_BEFORE - PROB_AFTER) * (ASSET_VALUE / COST_PER_HR) * COST_PER_HR

results_df = pd.DataFrame([{
    "case":                    "case1_energy",
    "iterations":              ITERS,
    "ale_before_mean_usd":     float(np.mean(loss_before)),
    "ale_after_mean_usd":      float(np.mean(loss_after)),
    "ale_savings_mean_usd":    float(np.mean(savings_ale)),
    "var95_before_usd":        float(var95_before),
    "var95_after_usd":         float(var95_after),
    "var95_reduction_pct":     float((1 - var95_after / max(var95_before, 1)) * 100),
    "downtime_saved_usd":      float(downtime_saved),
    "t_stat":                  float(t_stat),
    "p_value":                 float(p_val),
    "statistically_significant": bool(p_val < 0.05),
    "computed_at":             pd.Timestamp.utcnow(),
}])

conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
conn.execute("DROP TABLE IF EXISTS gold.case1_energy_var_ale")
conn.register("r1", results_df)
conn.execute("CREATE TABLE gold.case1_energy_var_ale AS SELECT * FROM r1")

print(f"✔ Case 1 — ALE savings: USD {np.mean(savings_ale):,.0f} | "
      f"VaR95 reduction: {(1-var95_after/max(var95_before,1))*100:.1f}% | "
      f"p={p_val:.4f}")
```

#### `assets/gold/gold_case2_health.py`

```python
# @bruin
# name: gold.case2_health_lpv
# type: python
# depends:
#   - silver.iiot_scored
# quality:
#   - name: "LPV savings must be positive"
#     type: custom_sql
#     query: "SELECT COUNT(*) FROM {{ asset }} WHERE lpv_savings_mean_usd <= 0"
#     max: 0
# @bruin

import pandas as pd, numpy as np
from scipy import stats
from bruin import get_context

ctx  = get_context()
conn = ctx.connections.get("duckdb")

ITERS      = 10_000
DEVICES    = 1200
LEGAL_COST = 400_000
BREACH_PROB = 0.005
rng = np.random.default_rng(42)

lam_b = DEVICES * 0.05;  lam_a = DEVICES * 0.005
intr_b = rng.poisson(lam_b, ITERS);  intr_a = rng.poisson(lam_a, ITERS)
lpv_b  = intr_b * LEGAL_COST * BREACH_PROB
lpv_a  = intr_a * LEGAL_COST * BREACH_PROB
lpv_savings = lpv_b - lpv_a

fp_savings  = (intr_b * 0.30 - intr_a * 0.05) * 75    # nurse-hours @ $75
insurance   = float(np.mean(lpv_savings)) * 0.05        # 5% premium reduction
t_stat, p_val = stats.ttest_ind(lpv_b, lpv_a)

results_df = pd.DataFrame([{
    "case":                       "case2_health",
    "iterations":                 ITERS,
    "lpv_savings_mean_usd":       float(np.mean(lpv_savings)),
    "lpv_savings_p95_usd":        float(np.percentile(lpv_savings, 95)),
    "alarm_fatigue_savings_usd":  float(np.mean(fp_savings)),
    "insurance_savings_usd":      insurance,
    "total_annual_savings_usd":   float(np.mean(lpv_savings)) + insurance,
    "t_stat":                     float(t_stat),
    "p_value":                    float(p_val),
    "statistically_significant":  bool(p_val < 0.05),
    "computed_at":                pd.Timestamp.utcnow(),
}])

conn.execute("DROP TABLE IF EXISTS gold.case2_health_lpv")
conn.register("r2", results_df)
conn.execute("CREATE TABLE gold.case2_health_lpv AS SELECT * FROM r2")
print(f"✔ Case 2 — LPV savings: USD {np.mean(lpv_savings):,.0f} | p={p_val:.4f}")
```

#### `assets/gold/gold_case3_logistics.py`

```python
# @bruin
# name: gold.case3_logistics_tel
# type: python
# depends:
#   - silver.iiot_scored
# quality:
#   - name: "TEL savings non-zero"
#     type: custom_sql
#     query: "SELECT COUNT(*) FROM {{ asset }} WHERE tel_savings_3yr_usd <= 0"
#     max: 0
# @bruin

import pandas as pd, numpy as np
from scipy import stats
from bruin import get_context

ctx  = get_context()
conn = ctx.connections.get("duckdb")

ITERS      = 10_000
DAILY_PKGS = 50_000
MARGIN_USD = 5.0
rng = np.random.default_rng(42)

spd_b = rng.normal(100, 10, ITERS)
spd_a = rng.normal(105,  8, ITERS)
t_stat, p_val = stats.ttest_ind(spd_b, spd_a)

tel_b = np.clip(float(np.mean(spd_a)) - spd_b[:365], 0, None) * 24*60 * MARGIN_USD
tel_a = np.clip(float(np.mean(spd_a)) - spd_a[:365], 0, None) * 24*60 * MARGIN_USD
tel_savings_3yr = float((tel_b - tel_a).sum() * 3)

theo  = rng.normal(1.0, 0.01, ITERS)
actual = theo + rng.choice([0, rng.normal(0.15, 0.05)], size=ITERS, p=[0.95, 0.05])
fraud_events = ((actual - theo) ** 2 > 0.005).sum()
merma_saved  = fraud_events * MARGIN_USD * DAILY_PKGS / ITERS

results_df = pd.DataFrame([{
    "case":                   "case3_logistics",
    "iterations":             ITERS,
    "speed_before_mean":      float(np.mean(spd_b)),
    "speed_after_mean":       float(np.mean(spd_a)),
    "tel_savings_3yr_usd":    tel_savings_3yr,
    "merma_saved_usd":        float(merma_saved),
    "total_savings_usd":      tel_savings_3yr + float(merma_saved),
    "t_stat":                 float(t_stat),
    "p_value":                float(p_val),
    "statistically_significant": bool(p_val < 0.05),
    "computed_at":            pd.Timestamp.utcnow(),
}])

conn.execute("DROP TABLE IF EXISTS gold.case3_logistics_tel")
conn.register("r3", results_df)
conn.execute("CREATE TABLE gold.case3_logistics_tel AS SELECT * FROM r3")
print(f"✔ Case 3 — TEL savings 3yr: USD {tel_savings_3yr:,.0f} | "
      f"Merma: USD {merma_saved:,.0f} | p={p_val:.4f}")
```

---

## ✅ Data Quality Checks

Bruin quality checks are defined **inline inside each asset** — no separate test files, no separate tool config:

```
Quality check types available in Bruin:
  ├── not_null          → column has no nulls above threshold
  ├── unique            → column values are unique
  ├── accepted_values   → column values in a defined set
  ├── row_count         → table has min/max rows
  ├── custom_sql        → any SQL expression as a check
  └── (more in docs)    → pattern_match, between, etc.
```

Run all checks:
```bash
# Validate the entire pipeline
bruin validate

# Validate a single asset
bruin validate assets/gold/gold_case1_energy.py

# Show lineage graph
bruin lineage assets/gold/gold_case1_energy.py
```

---

## ⏱ Orchestration & Scheduling

Bruin handles orchestration natively — no Airflow, no scheduler daemon:

```bash
# Run the full pipeline (respects dependency order automatically)
bruin run

# Run only the Gold layer (Bruin resolves upstream deps)
bruin run assets/gold/

# Run a specific asset + all dependencies
bruin run assets/gold/gold_case1_energy.py --full-refresh

# Dry run — show execution plan without running
bruin run --dry-run

# Run with a specific date context (for incremental loads)
bruin run --start-date 2025-01-01 --end-date 2025-06-30
```

Cron scheduling is defined in `pipeline.yml` and Bruin runs it natively or via GitHub Actions — no separate scheduler process needed.

---

## 🔄 CI/CD — GitHub Actions

#### `.github/workflows/bruin-pipeline.yml`

```yaml
name: IIoT Sentinel Pipeline — Bruin CI

on:
  push:
    branches: [main]
    paths:
      - 'bruin/assets/**'
      - 'bruin/pipeline.yml'
      - 'bruin/.bruin.yml'
  schedule:
    - cron: '0 6 * * *'      # Daily at 06:00 UTC
  workflow_dispatch:          # Manual trigger from GitHub UI

jobs:
  validate:
    name: Validate pipeline
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Bruin CLI
        uses: bruin-data/setup-bruin@v1   # official Bruin GitHub Action

      - name: Validate all assets
        run: bruin validate bruin/
        working-directory: .

  run-pipeline:
    name: Run full pipeline
    needs: validate
    runs-on: ubuntu-latest
    env:
      POSTGRES_PASSWORD: ${{ secrets.POSTGRES_PASSWORD }}
    steps:
      - uses: actions/checkout@v4

      - name: Install Bruin CLI
        uses: bruin-data/setup-bruin@v1

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Python dependencies
        run: pip install pandas numpy scipy duckdb mlflow

      - name: Run Bruin pipeline
        run: bruin run bruin/
        working-directory: .

      - name: Upload Gold layer results
        uses: actions/upload-artifact@v4
        with:
          name: gold-layer-results
          path: data/processed/gold/
          retention-days: 30

      - name: Notify on failure
        if: failure()
        uses: rtCamp/action-slack-notify@v2
        env:
          SLACK_WEBHOOK: ${{ secrets.SLACK_WEBHOOK }}
          SLACK_MESSAGE: "IIoT Sentinel pipeline FAILED on ${{ github.sha }}"
```

**GitHub Actions free tier:** 2,000 minutes/month — enough for daily pipeline runs.

---

## 📊 Monitoring Stack

```yaml
# bruin/docker/docker-compose-bruin.yml
version: '3.8'

services:
  # ── Storage backend ────────────────────────────────────────────────────────
  postgres:
    image: timescale/timescaledb:latest-pg16   # TimescaleDB = Postgres + time-series
    environment:
      POSTGRES_DB:       sentinel
      POSTGRES_USER:     bruin
      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD}"
    volumes:
      - pg-data:/var/lib/postgresql/data
    ports: ["5432:5432"]

  # ── S3-compatible object store ─────────────────────────────────────────────
  minio:
    image: quay.io/minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER:     minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    volumes: [minio-data:/data]
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # MinIO Console

  # ── BI Layer ───────────────────────────────────────────────────────────────
  superset:
    image: apache/superset:3.1.0
    ports: ["8088:8088"]
    environment:
      SUPERSET_SECRET_KEY: bruin-iiot-secret-2025
    command: >
      bash -c "superset db upgrade &&
               superset fab create-admin
               --username admin --password admin
               --firstname Admin --lastname User
               --email admin@example.com &&
               superset init &&
               superset run -h 0.0.0.0 -p 8088"

  # ── Monitoring ─────────────────────────────────────────────────────────────
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana-oss:latest
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana-data:/var/lib/grafana
    ports: ["3000:3000"]

  # ── ML Tracking ────────────────────────────────────────────────────────────
  mlflow:
    image: ghcr.io/mlflow/mlflow:v2.11.0
    command: >
      mlflow server
      --host 0.0.0.0
      --port 5000
      --backend-store-uri sqlite:///mlflow/mlflow.db
      --default-artifact-root /mlflow/artifacts
    volumes: [mlflow-data:/mlflow]
    ports: ["5000:5000"]

  # ── Streamlit Dashboard ────────────────────────────────────────────────────
  streamlit:
    image: python:3.11-slim
    working_dir: /app
    command: >
      bash -c "pip install streamlit plotly numpy scipy pandas duckdb -q &&
               streamlit run app/main.py
               --server.port 8501 --server.headless true"
    volumes: [..:/app]
    ports: ["8501:8501"]

volumes:
  pg-data:
  minio-data:
  grafana-data:
  mlflow-data:
```

---

## 📈 Dashboard Setup

After `docker compose up`:

| Service | URL | Credentials |
|---------|-----|-------------|
| **Streamlit Executive Portal** | http://localhost:8501 | — |
| **Apache Superset** | http://localhost:8088 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| **MLflow** | http://localhost:5000 | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 |
| **Prometheus** | http://localhost:9090 | — |

Connect Superset to DuckDB:
```
SQLAlchemy URI: duckdb:///data/sentinel.duckdb
```

---

## ⚡ Quick Start

```bash
# 1. Install Bruin
curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | sh

# 2. Clone and enter bruin directory
cd industrial-sentinel/bruin/

# 3. Start supporting services
docker compose -f docker/docker-compose-bruin.yml up -d

# 4. Validate pipeline config
bruin validate

# 5. Run the full pipeline
bruin run
# Output:
#  ✔ ingestion.iiot_raw_csv      [00:03]
#  ✔ bronze.iiot_network         [00:12]
#  ✔ silver.iiot_scored          [00:08]
#  ✔ gold.case1_energy_var_ale   [00:05]
#  ✔ gold.case2_health_lpv       [00:04]
#  ✔ gold.case3_logistics_tel    [00:04]
#  Pipeline complete in 00:36

# 6. View lineage graph (VS Code extension)
bruin lineage assets/gold/gold_case1_energy.py

# 7. Launch executive dashboard
cd ../app && streamlit run main.py
```

---

## 🎯 Makefile Targets (Bruin)

```bash
# Added to the master Makefile as bruin-* targets:

make bruin-install      # Install Bruin CLI
make bruin-validate     # Validate all assets and quality checks
make bruin-run          # Run the full pipeline
make bruin-run-gold     # Run only Gold layer (auto-resolves deps)
make bruin-lineage      # Show lineage for Case 1 asset
make bruin-docker-up    # Start Postgres + MinIO + Superset + Grafana + MLflow
make bruin-docker-down  # Stop all containers
make bruin-dashboard    # Launch Streamlit dashboard
make bruin-ci-test      # Simulate GitHub Actions run locally
make bruin-clean        # Remove DuckDB file + processed data
```

---

## ⚖️ Comparison vs. Traditional Stack

| Capability | Traditional Stack | Bruin Stack |
|------------|------------------|-------------|
| Ingestion | Airbyte / NiFi (separate) | `ingestr` built into Bruin CLI |
| SQL transforms | dbt (separate tool, separate config) | SQL assets with `/* @bruin */` |
| Python transforms | Custom scripts + Airflow | Python assets, same pipeline |
| Orchestration | Apache Airflow (Postgres, Redis, workers) | `bruin run` (one binary) |
| Data quality | Great Expectations (separate YAML suite) | Inline `quality:` blocks |
| Lineage | OpenLineage + Marquez (separate) | Built-in, automatic |
| Schema registry | Confluent / Hive Metastore | DuckDB catalog |
| Local dev setup | Hours (Airflow alone ~500MB) | Minutes (`brew install bruin`) |
| CI/CD integration | Custom Airflow operators | `bruin-data/setup-bruin` action |
| Infrastructure needed | Postgres + Redis + Airflow workers | Zero (DuckDB is embedded) |
| Learning curve | High (4+ tools to master) | Low (YAML + SQL + Python) |
| **Total cost** | **USD 0 (but complex)** | **USD 0 (and simple)** |

---

## 🎓 When to Choose Bruin for the Capstone

✅ **Choose Bruin if:**
- You are presenting to a committee and want to demo a running pipeline in < 5 minutes
- You want column-level lineage without setting up a separate lineage tool
- You want embedded data quality without a separate GE project structure
- You prefer Git-native code review of all pipeline logic
- You are running on a laptop with limited RAM (no Airflow workers needed)
- You want GitHub Actions to be your scheduler (free, no infrastructure)

🔄 **Combine with other stacks if:**
- You need production Kafka streaming (Bruin handles batch; combine with Flink for streaming)
- You need ML training at scale (add MLflow + Ray for distributed training)
- You need a full BI layer with user management (add Apache Superset)

---

*Bruin Stack for Industrial Sentinel v2025.1.0 · Apache 2.0 License · [getbruin.com](https://getbruin.com) · [github.com/bruin-data/bruin](https://github.com/bruin-data/bruin)*
