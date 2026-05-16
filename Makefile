# =============================================================================
#  INDUSTRIAL SENTINEL — IIoT Security ROI Pipeline (2025-2026)
#  Master Makefile  |  Dataset: CIC IIoT 2025 (DataSense)
#  Stack: Apache OSS · AWS · Azure · Databricks · Fabric · Multi-cloud
#
#  Usage:
#    make help          → list all targets
#    make setup         → bootstrap local environment
#    make run           → run all 3 business cases (local)
#    make cloud-aws     → deploy & run on AWS stack
#    make cloud-azure   → deploy & run on Azure stack
#    make cloud-dbx     → deploy & run on Databricks stack
#    make cloud-fabric  → deploy & run on Microsoft Fabric stack
#    make cloud-mc      → deploy multi-cloud (Astronomer + Iceberg)
#    make quality       → run Great Expectations validation suite
#    make airflow-up    → spin up local Airflow with Docker Compose
#    make nifi-up       → spin up Apache NiFi with Docker Compose
#    make dashboard     → launch Streamlit executive dashboard
#    make clean         → remove generated artefacts
# =============================================================================

# ── Project metadata ──────────────────────────────────────────────────────────
PROJECT       := industrial-sentinel
VERSION       := 2025.1.0
DATASET_NAME  := CIC-IIoT-2025-DataSense
PYTHON        := python3
PIP           := pip3
VENV          := .venv
VENV_PYTHON   := $(VENV)/bin/python
VENV_PIP      := $(VENV)/bin/pip

# ── Directories ───────────────────────────────────────────────────────────────
DATA_RAW        := data/raw
DATA_PROCESSED  := data/processed
DATA_GE         := data/ge_validations
NOTEBOOKS_DIR   := notebooks
SCRIPTS_DIR     := scripts
APP_DIR         := app
DAGS_DIR        := dags
REPORTS_DIR     := reports
DOCKER_DIR      := docker

# ── Docker image tags ─────────────────────────────────────────────────────────
AIRFLOW_IMAGE := apache/airflow:2.9.1
NIFI_IMAGE    := apache/nifi:1.25.0
SUPERSET_IMG  := apache/superset:3.1.0

# ── Cloud config (override via env vars or .env file) ─────────────────────────
AWS_REGION        ?= us-east-1
AWS_S3_BUCKET     ?= s3://$(PROJECT)-lake
AWS_EMR_CLUSTER   ?= j-XXXXXXXXX
AZURE_RG          ?= rg-industrial-sentinel
AZURE_WORKSPACE   ?= adb-iiot-sentinel
DBX_HOST          ?= https://adb-xxxx.azuredatabricks.net
DBX_CLUSTER_ID    ?= xxxx-xxxxxx-xxxxxxxx
FABRIC_WORKSPACE  ?= iiot-sentinel-fabric
ASTRO_DEPLOYMENT  ?= iiot-sentinel-astro

# ── Colours for terminal output ───────────────────────────────────────────────
RED    := \033[0;31m
GREEN  := \033[0;32m
YELLOW := \033[1;33m
CYAN   := \033[0;36m
RESET  := \033[0m

# ── Default target ────────────────────────────────────────────────────────────
.DEFAULT_GOAL := help

# =============================================================================
#  PHONY declarations
# =============================================================================
.PHONY: help setup setup-dev dirs download-data \
        ingest clean-data \
        case1-energy case2-health case3-logistics run \
        quality ge-init ge-run ge-docs \
        airflow-up airflow-down airflow-trigger \
        nifi-up nifi-down \
        superset-up \
        dashboard \
        cloud-aws cloud-azure cloud-dbx cloud-fabric cloud-mc \
        aws-upload aws-emr-run aws-glue-run \
        azure-upload azure-databricks-run \
        dbx-upload dbx-run dbx-mlflow \
        fabric-upload fabric-spark-run \
        mc-iceberg mc-astronomer-deploy mc-flink-run \
        test lint format \
        docker-build docker-up docker-down \
        clean clean-data clean-docker clean-all \
        readme report

# =============================================================================
#  HELP
# =============================================================================
help:
	@echo ""
	@printf "$(CYAN)╔══════════════════════════════════════════════════════════════╗$(RESET)\n"
	@printf "$(CYAN)║  Industrial Sentinel — IIoT Security ROI Pipeline  v$(VERSION)  ║$(RESET)\n"
	@printf "$(CYAN)╚══════════════════════════════════════════════════════════════╝$(RESET)\n"
	@echo ""
	@printf "$(YELLOW)── Environment ──────────────────────────────────────────────────$(RESET)\n"
	@printf "  $(GREEN)make setup$(RESET)              Bootstrap local venv + install all dependencies\n"
	@printf "  $(GREEN)make setup-dev$(RESET)          Setup + dev tools (pytest, ruff, pre-commit)\n"
	@printf "  $(GREEN)make dirs$(RESET)               Create project directory structure\n"
	@printf "  $(GREEN)make download-data$(RESET)      Download CIC IIoT 2025 dataset via Kaggle API\n"
	@echo ""
	@printf "$(YELLOW)── Pipeline Stages ──────────────────────────────────────────────$(RESET)\n"
	@printf "  $(GREEN)make ingest$(RESET)             Run NiFi-style ingestion + Parquet conversion\n"
	@printf "  $(GREEN)make clean-data$(RESET)         Normalize columns + MSE anomaly scoring\n"
	@echo ""
	@printf "$(YELLOW)── Business Cases ───────────────────────────────────────────────$(RESET)\n"
	@printf "  $(GREEN)make case1-energy$(RESET)       Caso 1 — VaR + ALE Monte Carlo (Oil & Gas)\n"
	@printf "  $(GREEN)make case2-health$(RESET)       Caso 2 — Poisson + LPV (Smart Hospitals)\n"
	@printf "  $(GREEN)make case3-logistics$(RESET)    Caso 3 — TEL + T-test (Smart Retail)\n"
	@printf "  $(GREEN)make run$(RESET)                Run all 3 cases sequentially\n"
	@echo ""
	@printf "$(YELLOW)── Data Quality ─────────────────────────────────────────────────$(RESET)\n"
	@printf "  $(GREEN)make ge-init$(RESET)            Initialise Great Expectations project\n"
	@printf "  $(GREEN)make ge-run$(RESET)             Run all GE checkpoint validations\n"
	@printf "  $(GREEN)make ge-docs$(RESET)            Build GE data docs site\n"
	@printf "  $(GREEN)make quality$(RESET)            ge-init + ge-run + ge-docs\n"
	@echo ""
	@printf "$(YELLOW)── Orchestration (Local) ─────────────────────────────────────────$(RESET)\n"
	@printf "  $(GREEN)make airflow-up$(RESET)         Start Apache Airflow (Docker Compose)\n"
	@printf "  $(GREEN)make airflow-down$(RESET)       Stop Airflow containers\n"
	@printf "  $(GREEN)make airflow-trigger$(RESET)    Trigger the iiot_sentinel_dag manually\n"
	@printf "  $(GREEN)make nifi-up$(RESET)            Start Apache NiFi (Docker Compose)\n"
	@printf "  $(GREEN)make nifi-down$(RESET)          Stop NiFi container\n"
	@echo ""
	@printf "$(YELLOW)── Visualization ────────────────────────────────────────────────$(RESET)\n"
	@printf "  $(GREEN)make dashboard$(RESET)          Launch Streamlit Executive ROI Dashboard\n"
	@printf "  $(GREEN)make superset-up$(RESET)        Start Apache Superset (Docker Compose)\n"
	@echo ""
	@printf "$(YELLOW)── Cloud Deployments ─────────────────────────────────────────────$(RESET)\n"
	@printf "  $(GREEN)make cloud-aws$(RESET)          Upload + run pipeline on AWS (S3 + EMR + MWAA)\n"
	@printf "  $(GREEN)make cloud-azure$(RESET)        Upload + run pipeline on Azure (ADLS + Databricks)\n"
	@printf "  $(GREEN)make cloud-dbx$(RESET)          Upload + run on Databricks Lakehouse + MLflow\n"
	@printf "  $(GREEN)make cloud-fabric$(RESET)       Upload + run on Microsoft Fabric (OneLake + Spark)\n"
	@printf "  $(GREEN)make cloud-mc$(RESET)           Multi-cloud: Iceberg + Astronomer + Flink\n"
	@echo ""
	@printf "$(YELLOW)── Dev & CI ──────────────────────────────────────────────────────$(RESET)\n"
	@printf "  $(GREEN)make test$(RESET)               Run pytest test suite\n"
	@printf "  $(GREEN)make lint$(RESET)               Run ruff linter\n"
	@printf "  $(GREEN)make format$(RESET)             Auto-format with ruff + black\n"
	@printf "  $(GREEN)make report$(RESET)             Export executive PDF report\n"
	@printf "  $(GREEN)make clean$(RESET)              Remove __pycache__ + .pyc files\n"
	@printf "  $(GREEN)make clean-all$(RESET)          Remove everything (venv + data + docker)\n"
	@echo ""

# =============================================================================
#  ENVIRONMENT SETUP
# =============================================================================
setup: dirs
	@printf "$(CYAN)▶ Creating Python virtual environment...$(RESET)\n"
	@$(PYTHON) -m venv $(VENV)
	@printf "$(CYAN)▶ Upgrading pip...$(RESET)\n"
	@$(VENV_PIP) install --upgrade pip wheel setuptools
	@printf "$(CYAN)▶ Installing core dependencies...$(RESET)\n"
	@$(VENV_PIP) install \
		pandas>=2.2.0 \
		numpy>=1.26.0 \
		scipy>=1.12.0 \
		polars>=0.20.0 \
		pyarrow>=15.0.0 \
		fastparquet>=2024.2.0 \
		scikit-learn>=1.4.0 \
		streamlit>=1.32.0 \
		plotly>=5.20.0 \
		great_expectations>=0.18.0 \
		pandera>=0.18.0 \
		dbt-core>=1.7.0 \
		apache-airflow>=2.9.0 \
		mlflow>=2.11.0 \
		kaggle>=1.6.0 \
		boto3>=1.34.0 \
		azure-storage-blob>=12.19.0 \
		azure-databricks-sdk>=0.9.0 \
		delta-spark>=3.1.0 \
		pyspark>=3.5.0 \
		confluent-kafka>=2.4.0 \
		requests>=2.31.0 \
		python-dotenv>=1.0.0
	@printf "$(GREEN)✔ Environment ready. Activate with: source $(VENV)/bin/activate$(RESET)\n"

setup-dev: setup
	@printf "$(CYAN)▶ Installing dev tools...$(RESET)\n"
	@$(VENV_PIP) install pytest pytest-cov ruff black pre-commit ipykernel
	@pre-commit install
	@printf "$(GREEN)✔ Dev environment ready$(RESET)\n"

dirs:
	@printf "$(CYAN)▶ Creating project directory structure...$(RESET)\n"
	@mkdir -p \
		$(DATA_RAW) \
		$(DATA_PROCESSED)/bronze \
		$(DATA_PROCESSED)/silver \
		$(DATA_PROCESSED)/gold \
		$(DATA_GE) \
		$(NOTEBOOKS_DIR) \
		$(SCRIPTS_DIR) \
		$(APP_DIR) \
		$(DAGS_DIR) \
		$(REPORTS_DIR) \
		$(DOCKER_DIR) \
		logs \
		models \
		configs \
		.great_expectations
	@touch .env .gitignore
	@printf "$(GREEN)✔ Directory structure created$(RESET)\n"
	@tree -L 3 --dirsfirst 2>/dev/null || find . -type d | head -30

# =============================================================================
#  DATA ACQUISITION
# =============================================================================
download-data:
	@printf "$(CYAN)▶ Downloading CIC IIoT 2025 dataset from Kaggle...$(RESET)\n"
	@if [ -z "$$KAGGLE_USERNAME" ] || [ -z "$$KAGGLE_KEY" ]; then \
		echo "$(RED)✗ Set KAGGLE_USERNAME and KAGGLE_KEY in your .env file$(RESET)"; \
		exit 1; \
	fi
	@$(VENV_PYTHON) -c "\
import kaggle; \
kaggle.api.authenticate(); \
kaggle.api.dataset_download_files(\
    'muhammadirfangull/cic-iiot-2025', \
    path='$(DATA_RAW)', \
    unzip=True \
)"
	@printf "$(GREEN)✔ Dataset downloaded to $(DATA_RAW)/$(RESET)\n"
	@ls -lh $(DATA_RAW)/

# =============================================================================
#  INGESTION & CLEANING  (NiFi-style Python equivalent)
# =============================================================================
ingest:
	@printf "$(CYAN)▶ Stage 1 — Ingestion: CSV → Parquet (Bronze layer)...$(RESET)\n"
	@$(VENV_PYTHON) scripts/ingest_iiot.py \
		--input  $(DATA_RAW) \
		--output $(DATA_PROCESSED)/bronze \
		--chunk-size 100000
	@printf "$(GREEN)✔ Bronze layer ready$(RESET)\n"

clean-data:
	@printf "$(CYAN)▶ Stage 2 — Cleaning + MSE Anomaly Scoring (Silver layer)...$(RESET)\n"
	@$(VENV_PYTHON) scripts/clean_and_score.py \
		--input  $(DATA_PROCESSED)/bronze \
		--output $(DATA_PROCESSED)/silver
	@printf "$(GREEN)✔ Silver layer ready$(RESET)\n"

# =============================================================================
#  BUSINESS CASES
# =============================================================================
case1-energy:
	@printf "$(CYAN)▶ Caso 1 — Infraestructura Crítica: VaR + ALE Monte Carlo...$(RESET)\n"
	@$(VENV_PYTHON) scripts/case1_energy_var_ale.py \
		--input     $(DATA_PROCESSED)/silver \
		--output    $(DATA_PROCESSED)/gold/case1 \
		--iters     10000 \
		--asset-val 500000000 \
		--cost-hr   150000 \
		--prob-before 0.15 \
		--prob-after  0.02
	@printf "$(GREEN)✔ Caso 1 complete — results in data/processed/gold/case1/$(RESET)\n"

case2-health:
	@printf "$(CYAN)▶ Caso 2 — Salud Conectada: Poisson + LPV simulation...$(RESET)\n"
	@$(VENV_PYTHON) scripts/case2_health_lpv.py \
		--input       $(DATA_PROCESSED)/silver \
		--output      $(DATA_PROCESSED)/gold/case2 \
		--devices     1200 \
		--legal-cost  400000 \
		--breach-prob 0.005 \
		--iters       10000
	@printf "$(GREEN)✔ Caso 2 complete — results in data/processed/gold/case2/$(RESET)\n"

case3-logistics:
	@printf "$(CYAN)▶ Caso 3 — Logística: TEL + T-test + MSE scatter...$(RESET)\n"
	@$(VENV_PYTHON) scripts/case3_logistics_tel.py \
		--input       $(DATA_PROCESSED)/silver \
		--output      $(DATA_PROCESSED)/gold/case3 \
		--daily-pkgs  50000 \
		--margin-usd  5 \
		--iters       10000
	@printf "$(GREEN)✔ Caso 3 complete — results in data/processed/gold/case3/$(RESET)\n"

run: ingest clean-data case1-energy case2-health case3-logistics
	@printf "$(GREEN)══════════════════════════════════════════════$(RESET)\n"
	@printf "$(GREEN)✔ All 3 business cases completed successfully$(RESET)\n"
	@printf "$(GREEN)  Gold layer: data/processed/gold/$(RESET)\n"
	@printf "$(GREEN)  Run 'make dashboard' to launch the executive portal$(RESET)\n"
	@printf "$(GREEN)══════════════════════════════════════════════$(RESET)\n"

# =============================================================================
#  DATA QUALITY  — Great Expectations
# =============================================================================
ge-init:
	@printf "$(CYAN)▶ Initialising Great Expectations project...$(RESET)\n"
	@$(VENV_PYTHON) scripts/ge_init.py
	@printf "$(GREEN)✔ GE project initialised at .great_expectations/$(RESET)\n"

ge-run:
	@printf "$(CYAN)▶ Running GE checkpoints (Bronze + Silver + Gold)...$(RESET)\n"
	@$(VENV_PYTHON) -c "\
import great_expectations as gx; \
ctx = gx.get_context(); \
results = ctx.run_checkpoint(checkpoint_name='iiot_bronze_checkpoint'); \
print('Bronze:', 'PASS' if results.success else 'FAIL'); \
results2 = ctx.run_checkpoint(checkpoint_name='iiot_silver_checkpoint'); \
print('Silver:', 'PASS' if results2.success else 'FAIL'); \
results3 = ctx.run_checkpoint(checkpoint_name='iiot_gold_checkpoint'); \
print('Gold  :', 'PASS' if results3.success else 'FAIL'); \
"
	@printf "$(GREEN)✔ GE validation complete$(RESET)\n"

ge-docs:
	@printf "$(CYAN)▶ Building Great Expectations data docs...$(RESET)\n"
	@$(VENV_PYTHON) -c "\
import great_expectations as gx; \
ctx = gx.get_context(); \
ctx.build_data_docs(); \
ctx.open_data_docs(); \
"

quality: ge-init ge-run ge-docs
	@printf "$(GREEN)✔ Full data quality pipeline complete$(RESET)\n"

# =============================================================================
#  ORCHESTRATION — Apache Airflow (local Docker Compose)
# =============================================================================
airflow-up:
	@printf "$(CYAN)▶ Starting Apache Airflow (Docker Compose)...$(RESET)\n"
	@mkdir -p $(DOCKER_DIR)
	@cat > $(DOCKER_DIR)/docker-compose-airflow.yml << 'AIRFLOW_COMPOSE'
version: '3.8'
x-airflow-common: &airflow-common
  image: $(AIRFLOW_IMAGE)
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./scripts:/opt/airflow/scripts
    - ./data:/opt/airflow/data
    - airflow-logs:/opt/airflow/logs
  depends_on: [postgres]

services:
  postgres:
    image: postgres:15
    environment: {POSTGRES_USER: airflow, POSTGRES_PASSWORD: airflow, POSTGRES_DB: airflow}
    volumes: [postgres-db:/var/lib/postgresql/data]
  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports: ["8080:8080"]
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      retries: 5
  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
  airflow-init:
    <<: *airflow-common
    command: version
    environment:
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: admin
      _AIRFLOW_WWW_USER_PASSWORD: admin

volumes:
  postgres-db:
  airflow-logs:
AIRFLOW_COMPOSE
	@docker compose -f $(DOCKER_DIR)/docker-compose-airflow.yml up -d
	@printf "$(GREEN)✔ Airflow UI: http://localhost:8080  (admin/admin)$(RESET)\n"
	@printf "$(GREEN)  DAGs folder: ./dags/$(RESET)\n"

airflow-down:
	@docker compose -f $(DOCKER_DIR)/docker-compose-airflow.yml down -v
	@printf "$(GREEN)✔ Airflow stopped$(RESET)\n"

airflow-trigger:
	@printf "$(CYAN)▶ Triggering iiot_sentinel_dag...$(RESET)\n"
	@docker exec $$(docker ps -qf "name=airflow-scheduler") \
		airflow dags trigger iiot_sentinel_pipeline
	@printf "$(GREEN)✔ DAG triggered — check http://localhost:8080$(RESET)\n"

# =============================================================================
#  ORCHESTRATION — Apache NiFi
# =============================================================================
nifi-up:
	@printf "$(CYAN)▶ Starting Apache NiFi...$(RESET)\n"
	@docker run -d \
		--name nifi-iiot \
		-p 8443:8443 \
		-e SINGLE_USER_CREDENTIALS_USERNAME=admin \
		-e SINGLE_USER_CREDENTIALS_PASSWORD=adminpassword123 \
		-v $(PWD)/$(DATA_RAW):/opt/nifi/nifi-current/data_in \
		-v $(PWD)/$(DATA_PROCESSED)/bronze:/opt/nifi/nifi-current/data_out \
		$(NIFI_IMAGE)
	@printf "$(GREEN)✔ NiFi UI: https://localhost:8443/nifi  (admin / adminpassword123)$(RESET)\n"
	@printf "$(YELLOW)  Note: Import flows/iiot_sensor_flow.json via NiFi UI$(RESET)\n"

nifi-down:
	@docker stop nifi-iiot && docker rm nifi-iiot
	@printf "$(GREEN)✔ NiFi stopped$(RESET)\n"

# =============================================================================
#  VISUALIZATION
# =============================================================================
dashboard:
	@printf "$(CYAN)▶ Launching Streamlit Executive ROI Dashboard...$(RESET)\n"
	@$(VENV_PYTHON) -m streamlit run $(APP_DIR)/main.py \
		--server.port 8501 \
		--server.headless false \
		--browser.gatherUsageStats false
	@printf "$(GREEN)✔ Dashboard: http://localhost:8501$(RESET)\n"

superset-up:
	@printf "$(CYAN)▶ Starting Apache Superset...$(RESET)\n"
	@docker run -d \
		--name superset-iiot \
		-p 8088:8088 \
		-e SUPERSET_SECRET_KEY=iiot-sentinel-secret \
		$(SUPERSET_IMG)
	@sleep 15
	@docker exec superset-iiot superset fab create-admin \
		--username admin --firstname Admin --lastname User \
		--email admin@example.com --password admin
	@docker exec superset-iiot superset db upgrade
	@docker exec superset-iiot superset init
	@printf "$(GREEN)✔ Superset UI: http://localhost:8088  (admin/admin)$(RESET)\n"

# =============================================================================
#  CLOUD — AWS  (S3 + EMR Serverless + MWAA + SageMaker)
# =============================================================================
cloud-aws: aws-upload aws-glue-run aws-emr-run
	@printf "$(GREEN)✔ AWS pipeline complete$(RESET)\n"

aws-upload:
	@printf "$(CYAN)▶ [AWS] Uploading data + scripts to S3...$(RESET)\n"
	@aws s3 sync $(DATA_PROCESSED)/silver/ \
		$(AWS_S3_BUCKET)/silver/ \
		--exclude "*.DS_Store" \
		--sse AES256
	@aws s3 sync scripts/ \
		$(AWS_S3_BUCKET)/scripts/ \
		--exclude "*.pyc"
	@printf "$(GREEN)✔ [AWS] Uploaded to $(AWS_S3_BUCKET)$(RESET)\n"

aws-glue-run:
	@printf "$(CYAN)▶ [AWS] Triggering AWS Glue ETL job (Spark)...$(RESET)\n"
	@aws glue start-job-run \
		--job-name iiot-sentinel-etl \
		--arguments \
			'--S3_INPUT=$(AWS_S3_BUCKET)/silver/,--S3_OUTPUT=$(AWS_S3_BUCKET)/gold/,--ITERS=10000' \
		--region $(AWS_REGION)
	@printf "$(GREEN)✔ [AWS] Glue job started$(RESET)\n"

aws-emr-run:
	@printf "$(CYAN)▶ [AWS] Submitting Monte Carlo Spark job to EMR...$(RESET)\n"
	@aws emr add-steps \
		--cluster-id $(AWS_EMR_CLUSTER) \
		--steps Type=Spark,Name="IIoT Monte Carlo",\
ActionOnFailure=CONTINUE,\
Args=[\
--deploy-mode,cluster,\
--py-files,$(AWS_S3_BUCKET)/scripts/helpers.zip,\
$(AWS_S3_BUCKET)/scripts/case1_energy_var_ale.py,\
--input,$(AWS_S3_BUCKET)/silver/,\
--output,$(AWS_S3_BUCKET)/gold/case1/\
] \
		--region $(AWS_REGION)
	@printf "$(GREEN)✔ [AWS] EMR step submitted$(RESET)\n"

# =============================================================================
#  CLOUD — Azure  (ADLS Gen2 + Azure Databricks + ADF)
# =============================================================================
cloud-azure: azure-upload azure-databricks-run
	@printf "$(GREEN)✔ Azure pipeline complete$(RESET)\n"

azure-upload:
	@printf "$(CYAN)▶ [Azure] Uploading to ADLS Gen2...$(RESET)\n"
	@az storage fs directory upload \
		--account-name $$AZURE_STORAGE_ACCOUNT \
		--file-system iiot-sentinel \
		--source $(DATA_PROCESSED)/silver \
		--destination silver \
		--recursive \
		--auth-mode login
	@az storage fs directory upload \
		--account-name $$AZURE_STORAGE_ACCOUNT \
		--file-system iiot-sentinel \
		--source scripts \
		--destination scripts \
		--recursive \
		--auth-mode login
	@printf "$(GREEN)✔ [Azure] Data uploaded to ADLS Gen2$(RESET)\n"

azure-databricks-run:
	@printf "$(CYAN)▶ [Azure] Submitting Databricks Spark job...$(RESET)\n"
	@databricks jobs run-now \
		--job-id $$AZURE_DBX_JOB_ID \
		--python-params '["--input", "abfss://iiot-sentinel@$$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/silver/", "--output", "abfss://iiot-sentinel@$$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/gold/", "--iters", "10000"]'
	@printf "$(GREEN)✔ [Azure] Databricks job submitted$(RESET)\n"

# =============================================================================
#  CLOUD — Databricks  (Delta Lake + MLflow + Workflows)
# =============================================================================
cloud-dbx: dbx-upload dbx-run dbx-mlflow
	@printf "$(GREEN)✔ Databricks pipeline complete$(RESET)\n"

dbx-upload:
	@printf "$(CYAN)▶ [Databricks] Uploading notebooks + scripts to DBFS...$(RESET)\n"
	@databricks fs cp -r scripts/ dbfs:/iiot-sentinel/scripts/ --overwrite
	@databricks fs cp -r $(DATA_PROCESSED)/silver/ dbfs:/iiot-sentinel/silver/ --overwrite
	@printf "$(GREEN)✔ [Databricks] Uploaded to DBFS$(RESET)\n"

dbx-run:
	@printf "$(CYAN)▶ [Databricks] Running pipeline job on cluster $(DBX_CLUSTER_ID)...$(RESET)\n"
	@databricks runs submit \
		--cluster-id $(DBX_CLUSTER_ID) \
		--python-file dbfs:/iiot-sentinel/scripts/case1_energy_var_ale.py \
		--parameters '["--input","dbfs:/iiot-sentinel/silver/","--output","dbfs:/iiot-sentinel/gold/","--iters","10000"]'
	@printf "$(GREEN)✔ [Databricks] Job submitted — check $(DBX_HOST)/#job/list$(RESET)\n"

dbx-mlflow:
	@printf "$(CYAN)▶ [Databricks] Logging models to MLflow experiment...$(RESET)\n"
	@MLFLOW_TRACKING_URI=$(DBX_HOST)/mlflow \
	 $(VENV_PYTHON) scripts/mlflow_log_models.py \
		--experiment-name "iiot-sentinel-anomaly-detection" \
		--model-path models/
	@printf "$(GREEN)✔ [Databricks] MLflow run logged$(RESET)\n"

# =============================================================================
#  CLOUD — Microsoft Fabric  (OneLake + Fabric Spark + Power BI)
# =============================================================================
cloud-fabric: fabric-upload fabric-spark-run
	@printf "$(GREEN)✔ Microsoft Fabric pipeline complete$(RESET)\n"

fabric-upload:
	@printf "$(CYAN)▶ [Fabric] Uploading to OneLake via Azure CLI...$(RESET)\n"
	@az storage blob upload-batch \
		--account-name $$FABRIC_STORAGE_ACCOUNT \
		--destination "$$FABRIC_CONTAINER/silver" \
		--source $(DATA_PROCESSED)/silver \
		--pattern "*.parquet" \
		--auth-mode login
	@printf "$(GREEN)✔ [Fabric] Data uploaded to OneLake$(RESET)\n"

fabric-spark-run:
	@printf "$(CYAN)▶ [Fabric] Triggering Fabric Spark notebook via REST API...$(RESET)\n"
	@$(VENV_PYTHON) scripts/fabric_trigger_notebook.py \
		--workspace $(FABRIC_WORKSPACE) \
		--notebook  "iiot_monte_carlo_simulation"
	@printf "$(GREEN)✔ [Fabric] Notebook triggered — check Microsoft Fabric portal$(RESET)\n"

# =============================================================================
#  CLOUD — Multi-cloud  (Apache Iceberg + Astronomer + Apache Flink)
# =============================================================================
cloud-mc: mc-iceberg mc-astronomer-deploy mc-flink-run
	@printf "$(GREEN)✔ Multi-cloud pipeline complete$(RESET)\n"

mc-iceberg:
	@printf "$(CYAN)▶ [Multi-cloud] Writing Gold layer to Apache Iceberg (cloud-agnostic)...$(RESET)\n"
	@$(VENV_PYTHON) scripts/write_iceberg.py \
		--input    $(DATA_PROCESSED)/gold \
		--catalog  $$ICEBERG_CATALOG_URI \
		--warehouse $$ICEBERG_WAREHOUSE \
		--table    iiot_sentinel.gold_cases
	@printf "$(GREEN)✔ [Multi-cloud] Iceberg table written$(RESET)\n"

mc-astronomer-deploy:
	@printf "$(CYAN)▶ [Multi-cloud] Deploying DAGs to Astronomer...$(RESET)\n"
	@astro deploy $(ASTRO_DEPLOYMENT) --dags-only
	@printf "$(GREEN)✔ [Multi-cloud] DAGs deployed to Astronomer$(RESET)\n"

mc-flink-run:
	@printf "$(CYAN)▶ [Multi-cloud] Submitting Flink streaming anomaly detection job...$(RESET)\n"
	@$(VENV_PYTHON) scripts/flink_anomaly_stream.py \
		--kafka-bootstrap $$KAFKA_BOOTSTRAP_SERVERS \
		--topic           iiot.sensor.raw \
		--output-topic    iiot.anomalies.scored
	@printf "$(GREEN)✔ [Multi-cloud] Flink job submitted$(RESET)\n"

# =============================================================================
#  DEV / CI
# =============================================================================
test:
	@printf "$(CYAN)▶ Running test suite...$(RESET)\n"
	@$(VENV_PYTHON) -m pytest tests/ \
		-v \
		--cov=scripts \
		--cov-report=term-missing \
		--cov-report=html:reports/coverage
	@printf "$(GREEN)✔ Tests complete — coverage report: reports/coverage/index.html$(RESET)\n"

lint:
	@printf "$(CYAN)▶ Running ruff linter...$(RESET)\n"
	@$(VENV)/bin/ruff check scripts/ app/ dags/
	@printf "$(GREEN)✔ Lint passed$(RESET)\n"

format:
	@printf "$(CYAN)▶ Auto-formatting code...$(RESET)\n"
	@$(VENV)/bin/ruff format scripts/ app/ dags/
	@$(VENV)/bin/black scripts/ app/ dags/ --line-length 100
	@printf "$(GREEN)✔ Formatting done$(RESET)\n"

report:
	@printf "$(CYAN)▶ Generating executive PDF report...$(RESET)\n"
	@$(VENV_PYTHON) scripts/generate_report.py \
		--gold-dir  $(DATA_PROCESSED)/gold \
		--output    reports/executive_roi_report.pdf
	@printf "$(GREEN)✔ Report: reports/executive_roi_report.pdf$(RESET)\n"

# =============================================================================
#  DOCKER — Full stack (Airflow + NiFi + Superset + MLflow)
# =============================================================================
docker-build:
	@printf "$(CYAN)▶ Building custom project Docker image...$(RESET)\n"
	@docker build -t $(PROJECT):$(VERSION) .
	@printf "$(GREEN)✔ Image built: $(PROJECT):$(VERSION)$(RESET)\n"

docker-up:
	@printf "$(CYAN)▶ Starting full Docker Compose stack...$(RESET)\n"
	@docker compose -f $(DOCKER_DIR)/docker-compose-full.yml up -d
	@echo ""
	@printf "$(GREEN)Services running:$(RESET)\n"
	@printf "  Airflow:   http://localhost:8080  (admin/admin)\n"
	@printf "  NiFi:      https://localhost:8443 (admin/adminpassword123)\n"
	@printf "  Superset:  http://localhost:8088  (admin/admin)\n"
	@printf "  MLflow:    http://localhost:5000\n"
	@printf "  Dashboard: http://localhost:8501\n"

docker-down:
	@docker compose -f $(DOCKER_DIR)/docker-compose-full.yml down -v
	@printf "$(GREEN)✔ All containers stopped$(RESET)\n"

# =============================================================================
#  CLEAN
# =============================================================================
clean:
	@printf "$(CYAN)▶ Cleaning Python cache files...$(RESET)\n"
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	@find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null; true
	@printf "$(GREEN)✔ Cache cleaned$(RESET)\n"

clean-docker:
	@printf "$(YELLOW)▶ Removing project Docker containers and images...$(RESET)\n"
	@docker rm -f nifi-iiot superset-iiot 2>/dev/null; true
	@docker rmi $(PROJECT):$(VERSION) 2>/dev/null; true
	@printf "$(GREEN)✔ Docker resources cleaned$(RESET)\n"

clean-all: clean clean-docker
	@printf "$(RED)▶ WARNING: Removing venv, all data, and generated files...$(RESET)\n"
	@read -p "Are you sure? [y/N] " ans && [ "$$ans" = "y" ] || exit 1
	@rm -rf $(VENV) $(DATA_PROCESSED) $(DATA_GE) reports/coverage
	@printf "$(GREEN)✔ Full clean done. Run 'make setup' to restart$(RESET)\n"

# =============================================================================
#  README / DOCS
# =============================================================================
readme:
	@printf "$(CYAN)▶ Project structure:$(RESET)\n"
	@echo ""
	@echo "  $(PROJECT)/"
	@echo "  ├── Makefile                  ← You are here"
	@echo "  ├── .env                      ← Cloud credentials (git-ignored)"
	@echo "  ├── data/"
	@echo "  │   ├── raw/                  ← CIC IIoT 2025 CSV files"
	@echo "  │   ├── processed/"
	@echo "  │   │   ├── bronze/           ← Raw Parquet (ingest stage)"
	@echo "  │   │   ├── silver/           ← Cleaned + MSE scored"
	@echo "  │   │   └── gold/             ← Case outputs (VaR/ALE/TEL/LPV)"
	@echo "  │   └── ge_validations/       ← Great Expectations results"
	@echo "  ├── scripts/"
	@echo "  │   ├── ingest_iiot.py        ← CSV chunked ingestion → Parquet"
	@echo "  │   ├── clean_and_score.py    ← Normalize columns + MSE scorer"
	@echo "  │   ├── case1_energy_var_ale.py ← Monte Carlo (Caso 1)"
	@echo "  │   ├── case2_health_lpv.py   ← Poisson + LPV (Caso 2)"
	@echo "  │   ├── case3_logistics_tel.py ← TEL + T-test (Caso 3)"
	@echo "  │   ├── ge_init.py            ← Great Expectations setup"
	@echo "  │   ├── mlflow_log_models.py  ← MLflow model registry"
	@echo "  │   ├── write_iceberg.py      ← Multi-cloud Iceberg writer"
	@echo "  │   ├── flink_anomaly_stream.py ← Kafka→Flink streaming job"
	@echo "  │   ├── fabric_trigger_notebook.py ← Fabric REST trigger"
	@echo "  │   └── generate_report.py   ← Executive PDF generation"
	@echo "  ├── app/"
	@echo "  │   └── main.py               ← Streamlit Executive Dashboard"
	@echo "  ├── dags/"
	@echo "  │   └── iiot_sentinel_dag.py  ← Airflow DAG (full pipeline)"
	@echo "  ├── docker/"
	@echo "  │   ├── docker-compose-airflow.yml"
	@echo "  │   └── docker-compose-full.yml"
	@echo "  ├── models/                   ← Trained ML models"
	@echo "  ├── reports/                  ← PDF + coverage reports"
	@echo "  ├── tests/                    ← pytest test suite"
	@echo "  └── .great_expectations/      ← GE project config"
