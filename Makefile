.PHONY: install setup infra ingest analyze figures reproduce test lint format clean help

PROJECT_ID ?= kyivnotkiev-research
REGION ?= us-central1
MAX_PARALLEL ?= 3
DATASET_DIR ?= ./dataset
HF_REPO ?= IvanDobrovolsky/kyivnotkiev-dataset

# ── Local Setup ─────────────────────────────────────────────────────────────

install:  ## Install Python dependencies locally
	uv sync

# ── GCP Infrastructure ──────────────────────────────────────────────────────

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

# ── Data Ingestion ─────────────────────────────────────────────────────────

ingest:  ## Run incremental ingestion for ALL sources (max 3 concurrent)
	python -m pipeline.ingestion.orchestrator --all --max-parallel $(MAX_PARALLEL)

ingest-pair:  ## Ingest one pair across all sources: make ingest-pair ID=1
	python -m pipeline.ingestion.orchestrator --pair-id $(ID)

ingest-source:  ## Ingest one source for all pairs: make ingest-source SOURCE=gdelt
	python -m pipeline.ingestion.orchestrator --source $(SOURCE)

ingest-gdelt:  ## GDELT: BigQuery public → our BigQuery
	python -m pipeline.ingestion.gdelt

ingest-trends:  ## Google Trends: search interest (global + country-level)
	python -m pipeline.ingestion.trends

ingest-trends-countries:  ## Fill missing country-level Google Trends data
	python -m pipeline.ingestion.trends_countries_fill

ingest-wikipedia:  ## Wikipedia: pageviews API
	python -m pipeline.ingestion.wikipedia

ingest-reddit:  ## Reddit: Arctic Shift + Reddit search API
	python -m pipeline.ingestion.reddit

ingest-youtube:  ## YouTube: yt-dlp search + YouTube Data API
	python -m pipeline.ingestion.youtube

ingest-ngrams:  ## Google Books Ngrams: book frequency (1900-2019)
	python -m pipeline.ingestion.ngrams

ingest-openalex:  ## OpenAlex: academic paper title mentions (FREE API)
	python -m pipeline.ingestion.openalex

# ── Export & Publish ───────────────────────────────────────────────────────

export-site:  ## Export BigQuery → site JSON (manifest.json = single source of truth)
	python -m pipeline.export_site_data

export-dataset:  ## Export BigQuery → publishable Parquet dataset
	python -m pipeline.export_dataset --output-dir $(DATASET_DIR)

publish-dataset:  ## Upload dataset to Hugging Face Hub
	huggingface-cli upload $(HF_REPO) $(DATASET_DIR)

# ── Analysis ────────────────────────────────────────────────────────────────

analyze:  ## Run ALL statistical tests from fresh BQ data
	python -m pipeline.analysis.recompute_stats

analyze-errors:  ## Cross-source error analysis (disagreements between sources)
	python -m pipeline.analysis.error_analysis

analyze-categories:  ## Run Kruskal-Wallis + pairwise tests
	python -m pipeline.analysis.categories

# ── Site ────────────────────────────────────────────────────────────────────

site-build:  ## Build the Astro site
	cd site && npm run build

site-dev:  ## Run site dev server
	cd site && npm run dev

# ── Full Pipeline ───────────────────────────────────────────────────────────

reproduce: ingest export-site analyze site-build  ## Full end-to-end reproduction
	@echo "Full reproduction complete"

refresh: export-site site-build  ## Quick refresh: re-export BQ data + rebuild site
	@echo "Site refreshed from BigQuery"

# ── Quality & Utilities ─────────────────────────────────────────────────────

status:  ## Show pipeline status (watermarks per pair per source)
	python -m pipeline.ingestion.orchestrator --status

validate:  ## Run data quality checks
	python -m pipeline.transform.validate

test:  ## Run tests
	uv run pytest tests/ -v

lint:  ## Run linter
	uv run ruff check pipeline/ tests/

format:  ## Auto-format code
	uv run ruff format pipeline/ tests/

clean:  ## Remove local processed files (does NOT touch BQ/GCS)
	rm -rf data/processed/*.csv data/processed/*.parquet
	rm -rf figures/*.png figures/*.html
	rm -rf dataset/

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'
