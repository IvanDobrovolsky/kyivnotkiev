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

ingest-gdelt:  ## GDELT: BigQuery public dataset → local parquet
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

export-site:  ## Export data → site JSON (manifest.json = single source of truth)
	python -m pipeline.export_site_data

export-dataset:  ## Export data → publishable Parquet dataset
	python -m pipeline.export_dataset --output-dir $(DATASET_DIR)

publish-dataset:  ## Upload dataset to Hugging Face Hub
	huggingface-cli upload $(HF_REPO) $(DATASET_DIR)

# ── Analysis ────────────────────────────────────────────────────────────────

analyze:  ## Run ALL statistical tests
	python -m pipeline.analysis.recompute_stats

analyze-errors:  ## Cross-source error analysis (disagreements between sources)
	python -m pipeline.analysis.error_analysis

analyze-precision:  ## Regex precision evaluation (sample GDELT matches)
	python -m pipeline.analysis.regex_precision

ingest-dictionaries:  ## Dictionary audit (Wiktionary + FreeDictionary for all pairs)
	python -m pipeline.ingestion.dictionaries

ingest-enforcement:  ## Spellcheck/enforcement audit (Grammarly, Google, hunspell)
	python -m pipeline.ingestion.spellcheck_audit

audit-dictionaries:  ## Dictionary scraper (Oxford, Cambridge, MW, Britannica)
	python -m pipeline.ingestion.playwright_audits.dictionary_scraper

analyze-categories:  ## Run Kruskal-Wallis + pairwise tests
	python -m pipeline.analysis.categories

# ── Computational Linguistics Pipeline ─────────────────────────────────────

cl-extract:  ## CL: Extract texts (Reddit + YouTube + OpenAlex)
	python -m pipeline.cl.run --step extract

cl-gdelt:  ## CL: Fetch GDELT article bodies (async, ~30 min)
	python -m pipeline.cl.extract.gdelt_articles_async --concurrency 20

cl-openalex:  ## CL: Extract OpenAlex paper titles + abstracts
	python -m pipeline.cl.extract.openalex_texts

cl-balance:  ## CL: Balance corpus (stratified sampling)
	python -m pipeline.cl.run --step balance

cl-classify:  ## CL: Context + sentiment classification (needs --api-url)
	python -m pipeline.cl.run --step classify --api-url $(API_URL)

cl-embed:  ## CL: Sentence embeddings + collocations
	python -m pipeline.cl.run --step embed

cl-finetune:  ## CL: Fine-tune encoder benchmark (3 models)
	python -m pipeline.cl.run --step finetune

cl-evaluate:  ## CL: Ablation studies on fine-tuned models
	python -m pipeline.cl.run --step evaluate

cl-export:  ## CL: Export dataset + model to HF, site JSON
	python -m pipeline.cl.run --step export

cl-all:  ## CL: Full pipeline (extract → balance → classify → embed → finetune → export)
	python -m pipeline.cl.run --step all --api-url $(API_URL)

publish-cl-dataset:  ## Push CL dataset to Hugging Face
	python -m pipeline.cl.export.hf_dataset --repo $(HF_REPO)-cl

publish-cl-model:  ## Push fine-tuned model to Hugging Face
	python -m pipeline.cl.export.hf_model --repo IvanDobrovolsky/toponym-context-classifier

# ── Site ────────────────────────────────────────────────────────────────────

site-build:  ## Build the Astro site
	cd site && npm run build

site-dev:  ## Run site dev server
	cd site && npm run dev

# ── Full Pipeline ───────────────────────────────────────────────────────────

reproduce: export-site analyze site-build  ## Reproduce analysis from HuggingFace parquets (no API keys needed)
	@echo "Full reproduction complete"

reproduce-full: ingest export-site analyze site-build  ## Full pipeline including data ingestion (needs API keys)
	@echo "Full reproduction with fresh data complete"

refresh: export-site site-build  ## Quick refresh: re-export data + rebuild site
	@echo "Site refreshed"

# ── Quality & Utilities ─────────────────────────────────────────────────────

status:  ## Show pipeline status (watermarks per pair per source)
	python -m pipeline.ingestion.orchestrator --status

validate:  ## Run data quality checks
	python -m pipeline.analysis.recompute_stats

test:  ## Run tests
	uv run pytest tests/ -v

lint:  ## Run linter
	uv run ruff check pipeline/ tests/

format:  ## Auto-format code
	uv run ruff format pipeline/ tests/

clean:  ## Remove generated files (preserves dataset/ parquets)
	rm -rf data/processed/*.csv data/processed/*.parquet
	rm -rf figures/*.png figures/*.html

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'
