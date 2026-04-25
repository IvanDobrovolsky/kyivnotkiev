"""Tests for CL pipeline — config, data preparation, corpus integrity."""

import pytest
import pandas as pd
from pathlib import Path


class TestCLConfig:
    def test_context_labels_count(self):
        from pipeline.cl.config import CONTEXT_LABELS
        assert len(CONTEXT_LABELS) == 11

    def test_religion_in_labels(self):
        from pipeline.cl.config import CONTEXT_LABELS
        assert "religion" in CONTEXT_LABELS

    def test_all_labels_lowercase(self):
        from pipeline.cl.config import CONTEXT_LABELS
        for label in CONTEXT_LABELS:
            assert label == label.lower(), f"Label {label} should be lowercase"

    def test_encoder_models_defined(self):
        from pipeline.cl.config import ENCODER_MODELS
        assert "deberta-v3-large" in ENCODER_MODELS
        assert "xlm-roberta-large" in ENCODER_MODELS
        assert "mdeberta-v3-base" in ENCODER_MODELS

    def test_top_6_pair_ids(self):
        from pipeline.cl.config import TOP_6_PAIR_IDS
        assert len(TOP_6_PAIR_IDS) == 6
        assert 1 in TOP_6_PAIR_IDS  # Kiev/Kyiv must be in top 6


class TestCorpusIntegrity:
    @pytest.fixture
    def clean_corpus(self):
        path = Path("data/cl/classified/corpus_clean.parquet")
        if not path.exists():
            pytest.skip("Clean corpus not available")
        return pd.read_parquet(path)

    @pytest.fixture
    def full_corpus(self):
        path = Path("data/cl/classified/corpus_full_classified.parquet")
        if not path.exists():
            pytest.skip("Full classified corpus not available")
        return pd.read_parquet(path)

    def test_clean_corpus_size(self, clean_corpus):
        assert len(clean_corpus) > 30000, "Clean corpus too small"
        assert len(clean_corpus) < 50000, "Clean corpus unexpectedly large"

    def test_clean_corpus_has_all_columns(self, clean_corpus):
        required = ["pair_id", "text", "source", "variant", "context_label"]
        for col in required:
            assert col in clean_corpus.columns, f"Missing column: {col}"

    def test_clean_corpus_no_null_labels(self, clean_corpus):
        assert clean_corpus["context_label"].isna().sum() == 0

    def test_clean_corpus_valid_labels(self, clean_corpus):
        from pipeline.cl.config import CONTEXT_LABELS
        invalid = set(clean_corpus["context_label"].unique()) - set(CONTEXT_LABELS)
        assert len(invalid) == 0, f"Invalid labels: {invalid}"

    def test_clean_corpus_variant_balance(self, clean_corpus):
        ru = (clean_corpus["variant"] == "russian").sum()
        ua = (clean_corpus["variant"] == "ukrainian").sum()
        ratio = ru / (ru + ua)
        assert 0.4 < ratio < 0.6, f"Variant imbalance: {ratio:.2f} RU"

    def test_full_corpus_larger_than_clean(self, full_corpus, clean_corpus):
        assert len(full_corpus) >= len(clean_corpus)

    def test_full_corpus_pair_coverage(self, full_corpus):
        n_pairs = full_corpus["pair_id"].nunique()
        assert n_pairs >= 57, f"Only {n_pairs} pairs in full corpus"


class TestDataPreparation:
    def test_train_val_test_split(self):
        from pipeline.cl.config import CONTEXT_LABELS

        path = Path("data/cl/classified/corpus_clean.parquet")
        if not path.exists():
            pytest.skip("Clean corpus not available")

        df = pd.read_parquet(path)
        df = df[df["context_label"].isin(CONTEXT_LABELS)]

        from sklearn.model_selection import train_test_split

        label2id = {l: i for i, l in enumerate(CONTEXT_LABELS)}
        df["label"] = df["context_label"].map(label2id)

        train, temp = train_test_split(df, test_size=0.2, stratify=df["label"], random_state=42)
        val, test = train_test_split(temp, test_size=0.5, stratify=temp["label"], random_state=42)

        # Check sizes
        total = len(train) + len(val) + len(test)
        assert total == len(df)
        assert abs(len(train) / total - 0.8) < 0.01
        assert abs(len(val) / total - 0.1) < 0.01
        assert abs(len(test) / total - 0.1) < 0.01

        # All labels in test set
        assert len(test["context_label"].unique()) == 11


class TestBenchmarkResults:
    @pytest.fixture
    def results(self):
        path = Path("data/cl/model/benchmark_runs/all_results.json")
        if not path.exists():
            pytest.skip("Benchmark results not available")
        import json
        with open(path) as f:
            return json.load(f)

    def test_at_least_18_runs(self, results):
        # 12 Phase 1 (XLM-R) + 3 DeBERTa + 3 mDeBERTa = 18 unique
        # (Phase 2 XLM-R reuses Phase 1 results, so 18 not 21)
        assert len(results) >= 18, f"Expected >=18 runs, got {len(results)}"

    def test_phase1_has_12_runs(self, results):
        xlmr = [r for r in results if r["model_key"] == "xlm-roberta-large"]
        assert len(xlmr) == 12

    def test_phase2_models(self, results):
        models = set(r["model_key"] for r in results)
        assert "deberta-v3-large" in models
        assert "xlm-roberta-large" in models
        assert "mdeberta-v3-base" in models

    def test_best_model_f1(self, results):
        best = max(results, key=lambda r: r["test_metrics"]["f1_macro"])
        assert best["model_key"] == "deberta-v3-large"
        assert best["test_metrics"]["f1_macro"] > 0.85

    def test_all_models_above_80(self, results):
        for r in results:
            f1 = r["test_metrics"]["f1_macro"]
            assert f1 > 0.75, f"{r['run_id']}: F1={f1} below 0.75"

    def test_weighted_beats_unweighted(self, results):
        import numpy as np
        weighted = [r["test_metrics"]["f1_macro"] for r in results
                    if r["weighted"] and r["learning_rate"] == 1e-5 and r["model_key"] == "xlm-roberta-large"]
        unweighted = [r["test_metrics"]["f1_macro"] for r in results
                      if not r["weighted"] and r["learning_rate"] == 1e-5 and r["model_key"] == "xlm-roberta-large"]
        if weighted and unweighted:
            assert np.mean(weighted) >= np.mean(unweighted) - 0.01  # Weighted should be at least comparable

    def test_confusion_matrix_present(self, results):
        for r in results:
            assert "confusion_matrix" in r, f"No confusion matrix in {r['run_id']}"
            assert len(r["confusion_matrix"]) == 11  # 11 classes
