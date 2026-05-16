# 🛡 Industrial Sentinel: IIoT Security ROI Pipeline (2025–2026)
[![License: CC BY 4.0](https://img.shields.io/badge/License-CC_BY_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)
[![License: CC BY-NC 4.0](https://img.shields.io/badge/License-CC_BY--NC_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc/4.0/)
> **Real-Time Anomaly Detection & Financial Impact Simulation**  
> Dataset: [CIC IIoT 2025 — DataSense Benchmark](https://iotdataset.com/data/cic-iiot-dataset-2025-datasense) · [UNB](https://www.unb.ca/cic/datasets/iiot-dataset-2025.html) · [Kaggle](https://www.kaggle.com/datasets/muhammadirfangull/cic-iiot-2025)

---

## 📋 Table of Contents

1. [Overview](#-overview)
2. [Business Value & ROI](#-business-value--roi)
3. [The Three Business Cases](#-the-three-business-cases)
4. [Architecture](#-architecture)
5. [Tech Stacks](#-tech-stacks)
   - [Apache OSS Stack (100% Free)](#1-apache-oss-stack--recommended-for-capstone)
   - [AWS Stack](#2-aws-stack)
   - [Azure Stack](#3-azure-stack)
   - [Databricks Stack](#4-databricks-stack)
   - [Microsoft Fabric Stack](#5-microsoft-fabric-stack)
   - [Multi-cloud Stack](#6-multi-cloud-stack-astronomer--iceberg)
   - [Bruin Stack (100% Free)](#7-bruin-stack--100-free--zero-infrastructure)
6. [Project Structure](#-project-structure)
7. [Quick Start](#-quick-start)
8. [Pipeline Stages](#-pipeline-stages)
9. [Data Quality](#-data-quality)
10. [Statistical Methods](#-statistical-methods)
11. [Visualizations](#-visualizations)
12. [Cloud Deployments](#-cloud-deployments)
13. [Executive Summary](#-executive-summary-for-the-cto)
14. [Contributing](#-contributing)

---

## 📊 Overview

**Industrial Sentinel** is a production-grade data engineering capstone project that transforms raw IIoT network telemetry into a quantified, board-ready cybersecurity ROI analysis.

The project implements a complete **Bronze → Silver → Gold** lakehouse pipeline over the **CIC IIoT 2025 (DataSense)** benchmark dataset — one of the most comprehensive industrial IoT attack datasets available, covering Mirai botnets, DoS, DDoS, MITM, Ransomware, and Scanning attacks across real industrial sensor traffic.

The goal is not just anomaly detection. It is **financial quantification**: how much does a data architecture save, in dollars, per sector.

```
Raw IIoT Sensor Data (CSV)
         │
    ┌────▼─────┐    ┌──────────┐    ┌──────────────────────┐
    │  INGEST  │───▶│  CLEAN   │───▶│  SIMULATE  (MC×10k)  │
    │  Bronze  │    │  Silver  │    │  Gold  VaR/ALE/LPV   │
    └──────────┘    └──────────┘    └──────────┬───────────┘
                                               │
                                    ┌──────────▼───────────┐
                                    │  EXECUTIVE DASHBOARD │
                                    │  Streamlit + Plotly  │
                                    └──────────────────────┘
```

---

## 🚀 Business Value & ROI

Based on 10,000-iteration Monte Carlo simulations and industry-standard financial models:

| Metric | Value |
|--------|-------|
| **Total projected ROI (3 years)** | USD 24–28M |
| **Attack success probability reduction** | 15% → 2% (–86.6%) |
| **VaR95 reduction (Energy sector)** | > 80% |
| **Payback period** | < 6 months |
| **Statistical confidence** | p < 0.01 on all T-tests |
| **Regulatory framework covered** | HIPAA · GDPR · Cyber Resilience Act 2026 |

---

## 📦 The Three Business Cases

### Case 1 — Critical Infrastructure (Energy / Oil & Gas)
**Methods:** Monte Carlo · Value at Risk (VaR) · Annual Loss Expectancy (ALE)

A USD 500M energy plant facing DoS attacks and sensor manipulation from the CIC IIoT dataset.  
The pipeline reduces attack success probability from **15% to 2%**, saving an estimated **USD 130K per avoided downtime hour**.

**Visualizations:**
- Loss distribution histogram (before vs. after pipeline) with shaded catastrophic-loss zone
- Real-time Cyber Risk Score gauge based on ALE

---

### Case 2 — Connected Health (Smart Hospitals)
**Methods:** Poisson Distribution · Liability Prevention Value (LPV) · Inferential Statistics

A hospital with 1,200 IoT medical devices. Each successful breach carries a USD 400K legal settlement risk.  
The pipeline reduces false-positive alarm fatigue, saving medical staff hours, and shields the hospital from **HIPAA/GDPR non-compliance** fines.

**Visualizations:**
- DITS (Device Intrusion Threat Score) heatmap by room/equipment node
- Stacked bar chart: Fines avoided | Insurance savings | Pipeline operating cost

---

### Case 3 — Smart Retail & Automated Logistics
**Methods:** Time-Series Analysis · MSE (Mean Squared Error) · T-test · Throughput Efficiency Loss (TEL)

A logistics center processing 50,000 packages/day with a USD 5 margin per unit.  
IoT network congestion causes micro-downtime. Edge filtering statistically improves throughput (**p < 0.05**) and weight-sensor fraud detection adds direct margin contribution.

**Visualizations:**
- Dual-axis line chart: package throughput (left) vs. cumulative USD savings (right)
- Scatter plot: theoretical vs. actual weight readings, colored by fraud detection flag

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    INDUSTRIAL SENTINEL PIPELINE                          │
│                                                                          │
│  ┌─────────────────┐    ┌──────────────────┐    ┌──────────────────┐   │
│  │   DATA SOURCES  │    │   EVENT STREAM   │    │   EDGE LAYER     │   │
│  │                 │    │                  │    │                  │   │
│  │ CIC IIoT 2025   │───▶│  Apache Kafka /  │───▶│  Apache NiFi /   │   │
│  │ (CSV / Parquet) │    │  Redpanda        │    │  Bruin ingestr   │   │
│  │ UNB / Kaggle    │    │  (sensor topics) │    │  (routing)       │   │
│  └─────────────────┘    └──────────────────┘    └────────┬─────────┘   │
│                                                           │             │
│  ┌────────────────────────────────────────────────────────▼──────────┐  │
│  │                     LAKEHOUSE  (Bronze → Silver → Gold)           │  │
│  │                                                                    │  │
│  │  Bronze (raw Parquet)  →  Silver (cleaned + MSE scored)           │  │
│  │                        →  Gold  (VaR / ALE / LPV / TEL)          │  │
│  │                                                                    │  │
│  │  Storage:  Apache Iceberg | Delta Lake | MinIO (S3-compat.)       │  │
│  └────────────────────────────────────────────────────────┬──────────┘  │
│                                                           │             │
│  ┌────────────────────────┐    ┌─────────────────────────▼──────────┐  │
│  │   ORCHESTRATION        │    │   COMPUTE LAYER                    │  │
│  │                        │    │                                    │  │
│  │  Apache Airflow /      │    │  Apache Spark (batch)              │  │
│  │  Bruin CLI /           │    │  Apache Flink (streaming)          │  │
│  │  Astronomer            │    │  DuckDB (local OLAP)               │  │
│  │  GitHub Actions (CI)   │    │  SciPy (Monte Carlo)               │  │
│  └────────────────────────┘    └────────────────────────────────────┘  │
│                                                           │             │
│  ┌────────────────────────┐    ┌─────────────────────────▼──────────┐  │
│  │   DATA QUALITY         │    │   VISUALIZATION & ML               │  │
│  │                        │    │                                    │  │
│  │  Great Expectations    │    │  Streamlit (Executive Portal)      │  │
│  │  Pandera               │    │  Apache Superset (BI)              │  │
│  │  Bruin quality checks  │    │  Grafana (real-time monitoring)    │  │
│  │  dbt tests             │    │  MLflow (model tracking)           │  │
│  └────────────────────────┘    └────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠 Tech Stacks

### 1. Apache OSS Stack *(Recommended for Capstone)*

> 100% free · Self-hosted · Full data sovereignty · Ideal for portfolio presentation

| Layer | Tool | Purpose |
|-------|------|---------|
| **Ingestion** | Apache NiFi | Visual sensor data routing |
| **Event bus** | Apache Kafka / Redpanda | Real-time streaming |
| **Stream processing** | Apache Flink | Anomaly detection on streams |
| **Batch processing** | Apache Spark | Monte Carlo simulations |
| **Storage** | Apache Iceberg + MinIO | Open table format + S3-compatible store |
| **Orchestration** | Apache Airflow | DAG-based pipeline scheduling |
| **Data quality** | Great Expectations + Pandera | Bronze/Silver/Gold validation |
| **Transformation** | dbt (OSS) | SQL-first Gold layer modeling |
| **BI dashboard** | Apache Superset | ROI analytics |
| **Monitoring** | Grafana + Prometheus | Real-time IIoT metrics |
| **ML tracking** | MLflow | Experiment registry |
| **App layer** | Streamlit | Executive dashboard |

---

### 2. AWS Stack

> Best for: Teams already on AWS, prioritizing managed services

| Layer | Tool |
|-------|------|
| Ingestion | AWS IoT Greengrass + Kinesis Data Streams |
| Stream processing | Kinesis Data Analytics (Flink) |
| Batch | AWS Glue (Spark) + EMR |
| Storage | S3 + Apache Iceberg |
| Orchestration | MWAA (Managed Airflow) |
| Data quality | Great Expectations + AWS Glue DQ |
| ML | Amazon SageMaker + MLflow |
| BI | Amazon QuickSight + Apache Superset |

---

### 3. Azure Stack

> Best for: Microsoft enterprises, hybrid / on-prem requirements

| Layer | Tool |
|-------|------|
| Ingestion | Azure IoT Hub + Azure Event Hubs |
| Stream processing | Azure Stream Analytics + Flink (HDInsight) |
| Batch | Azure Databricks (Spark) |
| Storage | ADLS Gen2 + Delta Lake |
| Orchestration | Azure Data Factory + **Astronomer** |
| Data quality | Great Expectations + Azure Purview |
| ML | Azure Machine Learning + MLflow |
| BI | Power BI + Apache Superset |

---

### 4. Databricks Stack

> Best for: Delta Lake-first teams, unified ML + analytics

| Layer | Tool |
|-------|------|
| Ingestion | Kafka (Confluent) + Autoloader |
| Stream processing | Spark Structured Streaming + Delta Live Tables |
| Batch | Databricks Runtime (Spark) |
| Storage | Delta Lake + Unity Catalog |
| Orchestration | Databricks Workflows + **Astronomer** |
| Data quality | Great Expectations + DLT Expectations |
| ML | **MLflow** (native) + Databricks AutoML |
| BI | Databricks SQL Dashboard + Superset |

---

### 5. Microsoft Fabric Stack

> Best for: Microsoft-first teams, SaaS all-in-one

| Layer | Tool |
|-------|------|
| Ingestion | Azure IoT Hub + Fabric Eventstream |
| Stream processing | Fabric Real-Time Intelligence (KQL) |
| Batch | Fabric Spark Notebooks |
| Storage | OneLake (Delta Parquet) |
| Orchestration | Fabric Pipelines + Fabric Activator |
| Data quality | Great Expectations (in notebooks) |
| ML | SynapseML + MLflow |
| BI | **Power BI** (native) + Fabric RT Dashboard |

---

### 6. Multi-cloud Stack (Astronomer + Iceberg)

> Best for: Avoiding vendor lock-in, regulated industries

| Layer | Tool | Why |
|-------|------|-----|
| Ingestion | Apache Kafka (any cloud) + NiFi (K8s) | Cloud-agnostic event bus |
| Stream processing | Apache Flink (K8s operator) | Portable via Kubernetes |
| Batch | Apache Spark (K8s operator) | Runs on any provider |
| Storage | **Apache Iceberg** (S3/ADLS/GCS) | Universal open table format |
| Orchestration | **Astronomer** (any cloud) | Managed Airflow, multi-cloud |
| Data quality | Great Expectations | Same validations, any cloud |
| ML | MLflow (self-hosted) + Ray (K8s) | Cloud-agnostic model ops |
| BI | Apache Superset (K8s) | Portable OSS BI |

---

### 7. Bruin Stack — 100% Free & Zero Infrastructure

> See full details in [BRUIN_STACK.md](./BRUIN_STACK.md)

Bruin is an end-to-end data platform with built-in ingestion, transformation in both SQL and Python, orchestration, quality checks, column-level lineage — all in one platform with a Git-native CLI and no separate vendors to stitch together.

Bruin replaces the typical Airflow + dbt + Great Expectations stack with a single binary: SQL and Python assets coexist in the same pipeline with automatic dependency resolution, incremental materialisation, and quality checks embedded directly in asset definitions.

| Layer | Tool | Cost |
|-------|------|------|
| Ingestion + Orchestration + Quality | **Bruin CLI** | Free (Apache 2.0) |
| OLAP engine | **DuckDB** | Free |
| Storage | **PostgreSQL** | Free |
| Time-series | **TimescaleDB** | Free |
| Object store | **MinIO** | Free |
| Table format | **Apache Iceberg** | Free |
| BI | **Apache Superset** | Free |
| Monitoring | **Grafana + Prometheus** | Free |
| ML tracking | **MLflow** | Free |
| App layer | **Streamlit** | Free |
| CI/CD | **GitHub Actions** | Free (2,000 min/month) |
| Container | **Docker + Docker Compose** | Free |

---

## 📁 Project Structure

```
industrial-sentinel/
│
├── Makefile                        ← Master build orchestration (705 lines)
├── setup_project.sh                ← Full bootstrap script (1,146 lines)
├── requirements.txt
├── .env                            ← Credentials (git-ignored)
├── .gitignore
│
├── data/
│   ├── raw/                        ← CIC IIoT 2025 CSV files (Kaggle)
│   └── processed/
│       ├── bronze/                 ← Raw Parquet after ingestion
│       ├── silver/                 ← Cleaned + MSE anomaly scored
│       └── gold/
│           ├── case1/              ← VaR + ALE Monte Carlo outputs
│           ├── case2/              ← Poisson + LPV outputs
│           └── case3/              ← TEL + T-test + weight scatter
│
├── scripts/
│   ├── ingest_iiot.py              ← Stage 1: CSV chunked → Parquet (Bronze)
│   ├── clean_and_score.py          ← Stage 2: Normalize + MSE score (Silver)
│   ├── case1_energy_var_ale.py     ← Monte Carlo VaR + ALE simulation
│   ├── case2_health_lpv.py         ← Poisson intrusion + LPV model
│   ├── case3_logistics_tel.py      ← TEL + T-test + weight MSE scatter
│   ├── ge_init.py                  ← Great Expectations suite builder
│   ├── mlflow_log_models.py        ← MLflow experiment tracking
│   ├── write_iceberg.py            ← Multi-cloud Iceberg writer
│   ├── flink_anomaly_stream.py     ← Kafka → Flink streaming job
│   ├── fabric_trigger_notebook.py  ← Microsoft Fabric REST trigger
│   └── generate_report.py          ← Executive PDF report generator
│
├── app/
│   └── main.py                     ← Streamlit Executive Dashboard (4 tabs)
│
├── dags/
│   └── iiot_sentinel_dag.py        ← Airflow DAG: ingest→clean→cases→quality
│
├── bruin/                          ← Bruin pipeline (see BRUIN_STACK.md)
│   ├── .bruin.yml                  ← Project config
│   ├── pipeline.yml                ← Pipeline definition
│   ├── assets/
│   │   ├── ingestion/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── .github/workflows/
│       └── bruin-pipeline.yml      ← GitHub Actions CI
│
├── docker/
│   ├── docker-compose-airflow.yml
│   └── docker-compose-full.yml     ← Airflow + NiFi + MLflow + Superset
│
├── .great_expectations/
│   ├── great_expectations.yml
│   ├── expectations/
│   └── checkpoints/
│
├── models/                         ← Trained ML models
├── reports/                        ← PDF + GE data docs
├── notebooks/                      ← Jupyter EDA notebooks
└── tests/                          ← pytest suite
```

---

## ⚡ Quick Start

### Option A — Full local run (Python only)

```bash
# 1. Clone and bootstrap
git clone https://github.com/youruser/industrial-sentinel.git
cd industrial-sentinel
./setup_project.sh

# 2. Fill in your Kaggle credentials
cp .env.example .env
nano .env   # set KAGGLE_USERNAME and KAGGLE_KEY

# 3. Install dependencies
make setup

# 4. Download dataset
make download-data

# 5. Run all 3 business cases
make run

# 6. Launch the executive dashboard
make dashboard
# → http://localhost:8501
```

### Option B — Full Docker stack (Airflow + NiFi + MLflow + Superset)

```bash
make docker-up
# Airflow:   http://localhost:8080  (admin / admin)
# NiFi:      https://localhost:8443 (admin / adminpassword123)
# MLflow:    http://localhost:5000
# Superset:  http://localhost:8088  (admin / admin)
# Dashboard: http://localhost:8501
```

### Option C — Bruin (zero-infra, single binary)

```bash
# Install Bruin CLI
curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | sh

# Run the full pipeline
cd bruin/
bruin run

# Validate data quality
bruin validate

# Launch dashboard
cd ../app && streamlit run main.py
# → Full details in BRUIN_STACK.md
```

---

## 🔁 Pipeline Stages

### Stage 1 — Ingestion (Bronze)
- Reads CIC IIoT 2025 CSV files in **100K-row chunks** (memory-efficient)
- Normalizes column names (`Flow Rate` → `sensor_flow_rate`)
- Writes compressed **Snappy Parquet** files
- Adds `ingestion_ts` audit column

### Stage 2 — Cleaning + Scoring (Silver)
- Drops duplicates and fully-null rows
- Clips sensor outliers at 3 standard deviations
- Computes **MSE Anomaly Score** per row vs. column mean baseline
- Labels top-5% anomaly scores as `is_anomaly = True`

### Stage 3 — Business Case Simulation (Gold)
Each case runs independently, producing a `summary.json` and `monte_carlo_runs.parquet`:

| Case | Model | Output |
|------|-------|--------|
| Case 1 Energy | `np.binomial` × 10,000 | `ale_savings_mean_usd`, `var95_reduction_pct` |
| Case 2 Health | `np.poisson` × 10,000 | `lpv_savings_mean_usd`, `alarm_fatigue_savings_usd` |
| Case 3 Logistics | `np.normal` T-test | `tel_savings_3yr_usd`, `merma_saved_usd` |

---

## ✅ Data Quality

Great Expectations suites are defined for each layer:

```
Bronze suite  → row count > 1,000 · label column not null (95%) · valid label values
Silver suite  → anomaly_score_mse ≥ 0 · is_anomaly boolean · processing_ts exists
Gold suite    → summary.json non-null · savings values > 0 · p_value < 0.05
```

Run with:
```bash
make quality         # init + validate all layers + build data docs
make ge-docs         # open GE data docs in browser: reports/ge_data_docs/
```

---

## 📐 Statistical Methods

| Method | Case | Library |
|--------|------|---------|
| Monte Carlo simulation (10,000 iter) | 1, 2, 3 | `numpy` |
| Binomial loss model | Case 1 (ALE) | `numpy.random.binomial` |
| Value at Risk (VaR 95%) | Case 1 | `numpy.percentile` |
| Poisson intrusion model | Case 2 (LPV) | `numpy.random.poisson` |
| Independent samples T-test | Cases 2, 3 | `scipy.stats.ttest_ind` |
| Mean Squared Error (MSE) | Cases 2, 3 | `numpy` |
| Time-series cumulative TEL | Case 3 | `numpy.cumsum` |

All results are statistically validated at **p < 0.05** significance threshold.

---

## 📊 Visualizations

| Chart | Case | Library |
|-------|------|---------|
| Loss distribution histogram (before vs. after) | Case 1 | Plotly |
| Cyber Risk Score gauge (ALE-based) | Case 1 | Plotly |
| LPV distribution overlay | Case 2 | Plotly |
| Stacked bar: fines / insurance / OPEX | Case 2 | Plotly |
| Dual-axis throughput + cumulative savings | Case 3 | Plotly |
| Weight fraud scatter (Real vs. Theoretical) | Case 3 | Plotly |
| Executive KPI metrics row | All | Streamlit |

---

## ☁️ Cloud Deployments

```bash
make cloud-aws      # S3 + EMR + MWAA + SageMaker
make cloud-azure    # ADLS Gen2 + Azure Databricks + ADF
make cloud-dbx      # Delta Lake + MLflow + Databricks Workflows
make cloud-fabric   # OneLake + Fabric Spark + Power BI
make cloud-mc       # Apache Iceberg + Astronomer + Flink (multi-cloud)
```

Each target handles: upload → compute job submission → result retrieval.  
Configure credentials in `.env` before running.

---

## 📄 Executive Summary for the CTO

**Subject:** ROI & Robustness Report — IIoT Cybersecurity Data Pipeline

**Investment Thesis:**  
The architecture transforms raw IIoT telemetry into a risk-mitigation asset. Statistical analysis confirms the system not only blocks attacks but optimizes operational efficiency (Throughput).

**Financial Impact:**

| Sector | Metric | Value |
|--------|--------|-------|
| Energy / Oil & Gas | ALE savings (annual) | USD 130K–1.3M |
| Smart Hospitals | LPV + compliance shield | USD 10M (3yr) |
| Logistics / Retail | TEL savings (3yr) | USD 13.1M |
| **Total (3yr)** | **Portfolio ROI** | **USD 24–28M** |

**Robustness Validation:**
- Monte Carlo (10K iterations) confirms VaR95 reduction > 80% in energy sector
- Poisson model validated with T-test p < 0.01 for healthcare LPV
- Throughput T-test confirms structural (non-random) improvement p < 0.05

**Recommendation:** Investment is **CAPEX** (asset protection), not OPEX. Self-financing within 6 months based on TEL efficiency savings alone.

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature`
3. Run tests: `make test`
4. Lint: `make lint`
5. Submit a pull request

---

## 📚 References

- [CIC IIoT 2025 Dataset — UNB](https://www.unb.ca/cic/datasets/iiot-dataset-2025.html)
- [DataSense Benchmark — IoTDataset.com](https://iotdataset.com/data/cic-iiot-dataset-2025-datasense)
- [Kaggle — CIC IIoT 2025](https://www.kaggle.com/datasets/muhammadirfangull/cic-iiot-2025)
- [Open Source Data Engineering Landscape 2025 — PracData](https://www.pracdata.io/p/open-source-data-engineering-landscape-2025)
- [Bruin — Open Source Data Pipeline CLI](https://getbruin.com)
- NIST Cybersecurity Framework (CSF) 2.0
- Cyber Resilience Act — European Union (2025)

---

*Industrial Sentinel v2025.1.0 · Built for the Data Engineering Capstone Program · Dataset: CIC IIoT 2025 (DataSense)*
