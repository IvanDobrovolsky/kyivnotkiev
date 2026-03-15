.PHONY: install collect analyze viz all clean test lint

install:
	uv sync

collect:
	uv run python scripts/run_collect.py --source all

collect-gdelt:
	uv run python scripts/run_collect.py --source gdelt

collect-trends:
	uv run python scripts/run_collect.py --source trends

collect-ngrams:
	uv run python scripts/run_collect.py --source ngrams

collect-dry:
	uv run python scripts/run_collect.py --source gdelt --dry-run

preprocess:
	uv run python -m src.pipeline.preprocess

analyze:
	uv run python scripts/run_analysis.py --source gdelt

analyze-trends:
	uv run python scripts/run_analysis.py --source trends

viz:
	uv run python scripts/run_viz.py --source gdelt

viz-trends:
	uv run python scripts/run_viz.py --source trends

all:
	uv run python scripts/run_pipeline.py --source all

all-gdelt:
	uv run python scripts/run_pipeline.py --source gdelt

all-trends:
	uv run python scripts/run_pipeline.py --source trends

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check src/ scripts/ tests/

format:
	uv run ruff format src/ scripts/ tests/

clean:
	rm -rf data/raw/gdelt/*.parquet
	rm -rf data/raw/trends/*.csv
	rm -rf data/raw/ngrams/*.csv
	rm -rf data/processed/*.parquet
	rm -rf paper/figures/*.png
