.PHONY: install setup infra ingest analyze figures paper reproduce test lint format clean help

PROJECT_ID ?= kyivnotkiev-research
REGION ?= us-central1
DOCKER_IMAGE = $(REGION)-docker.pkg.dev/$(PROJECT_ID)/kyivnotkiev-pipeline/orchestrator

# ── Local Setup ─────────────────────────────────────────────────────────────

install:  ## Install Python dependencies locally
	uv sync

# ── GCP Infrastructure ──────────────────────────────────────────────────────

setup: infra docker-build docker-push  ## Full GCP setup: Terraform + Docker

infra:  ## Deploy GCP infrastructure with Terraform
	cd infrastructure && terraform init && terraform apply -auto-approve \
		-var="project_id=$(PROJECT_ID)" \
		-var="billing_account_id=$(BILLING_ACCOUNT_ID)"

infra-plan:  ## Preview infrastructure changes
	cd infrastructure && terraform plan \
		-var="project_id=$(PROJECT_ID)" \
		-var="billing_account_id=$(BILLING_ACCOUNT_ID)"

infra-destroy:  ## Tear down GCP infrastructure
	cd infrastructure && terraform destroy \
		-var="project_id=$(PROJECT_ID)" \
		-var="billing_account_id=$(BILLING_ACCOUNT_ID)"

docker-build:  ## Build pipeline Docker image
	docker build -t $(DOCKER_IMAGE):latest .

docker-push:  ## Push Docker image to Artifact Registry
	docker push $(DOCKER_IMAGE):latest

# ── Data Ingestion (incremental — only fetches new/changed pairs) ───────────

ingest:  ## Run incremental ingestion for ALL enabled pairs, ALL sources
	python -m pipeline.ingestion.orchestrator --all

ingest-pair:  ## Ingest one pair across all sources: make ingest-pair ID=1
	python -m pipeline.ingestion.orchestrator --pair-id $(ID)

ingest-source:  ## Ingest one source for all pairs: make ingest-source SOURCE=gdelt
	python -m pipeline.ingestion.orchestrator --source $(SOURCE)

ingest-gdelt:  ## GDELT: BigQuery public → our BigQuery
	python -m pipeline.ingestion.gdelt

ingest-common-crawl:  ## Common Crawl: Spark on Dataproc (TB-scale web crawl)
	gcloud dataproc jobs submit pyspark \
		--cluster=kyivnotkiev-spark \
		--region=$(REGION) \
		--project=$(PROJECT_ID) \
		pipeline/ingestion/common_crawl.py \
		-- --config config/pipeline.yaml

ingest-reddit:  ## Reddit: Arctic Shift bulk dump via Spark
	python -m pipeline.ingestion.reddit

ingest-wikipedia:  ## Wikipedia: pageviews + edit history
	python -m pipeline.ingestion.wikipedia

ingest-trends:  ## Google Trends: search interest
	python -m pipeline.ingestion.trends

ingest-ngrams:  ## Google Books Ngrams: book frequency (1800-2019)
	python -m pipeline.ingestion.ngrams

ingest-youtube:  ## YouTube: Data API v3 video metadata
	python -m pipeline.ingestion.youtube

# ── Legacy local collection (pre-GCP) ──────────────────────────────────────

collect:  ## [legacy] Run local data collection
	uv run python scripts/run_collect.py --source all

collect-gdelt:
	uv run python scripts/run_collect.py --source gdelt

collect-trends:
	uv run python scripts/run_collect.py --source trends

collect-ngrams:
	uv run python scripts/run_collect.py --source ngrams

# ── Processing ──────────────────────────────────────────────────────────────

process:  ## Process raw data into analysis-ready format
	uv run python scripts/process_all.py

preprocess:
	uv run python -m src.pipeline.preprocess

migrate:  ## Load existing local data into BigQuery
	python -m pipeline.transform.migrate_local

# ── Analysis ────────────────────────────────────────────────────────────────

analyze:  ## Run ALL analysis (adoption, changepoints, regression, categories)
	python -m pipeline.analysis.run_all

analyze-adoption:  ## Compute adoption ratios per pair per source
	python -m pipeline.analysis.adoption

analyze-changepoints:  ## Detect change points with bootstrap CIs
	python -m pipeline.analysis.changepoint

analyze-regression:  ## Run logistic regression model
	python -m pipeline.analysis.regression

analyze-categories:  ## Run Kruskal-Wallis + pairwise tests
	python -m pipeline.analysis.categories

analyze-legacy:  ## [legacy] Run local analysis
	uv run python scripts/run_analysis.py --source gdelt

# ── Visualization ───────────────────────────────────────────────────────────

figures:  ## Generate ALL figures from BigQuery data
	python -m pipeline.figures.generate_all

viz:  ## [legacy] Local visualization
	uv run python scripts/run_viz.py --source gdelt

viz-trends:
	uv run python scripts/run_viz.py --source trends

viz-modern:
	uv run python -m src.viz.modern

# ── Paper ───────────────────────────────────────────────────────────────────

paper:  ## Build publication-ready docx with embedded figures
	python scripts/build_docx.py

# ── Full Pipeline ───────────────────────────────────────────────────────────

reproduce: setup ingest analyze figures paper  ## Full end-to-end reproduction
	@echo "✓ Full reproduction complete"

all:  ## [legacy] Run full local pipeline
	uv run python scripts/run_pipeline.py --source all

# ── Quality & Utilities ─────────────────────────────────────────────────────

status:  ## Show pipeline status (watermarks per pair per source)
	python -m pipeline.transform.watermarks --status

validate:  ## Run data quality checks
	python -m pipeline.transform.validate

test:  ## Run tests
	uv run pytest tests/ -v

lint:  ## Run linter
	uv run ruff check src/ scripts/ pipeline/ tests/

format:  ## Auto-format code
	uv run ruff format src/ scripts/ pipeline/ tests/

clean:  ## Remove local processed files (does NOT touch BQ/GCS)
	rm -rf data/processed/*.csv data/processed/*.parquet
	rm -rf paper/figures/*.png paper/figures/*.html

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'
