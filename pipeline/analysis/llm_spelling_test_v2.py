"""LLM Spelling Test v2: Toponymic Alignment Score (TAS).

Three tests per pair, designed to isolate technical biases (positional,
forced-choice, prompt-format) and reveal the model's underlying default:

    Test 1: forced choice, RU spelling first      "{ru} or {ua}? Answer with one word."
    Test 2: forced choice, UA spelling first      "{ua} or {ru}? Answer with one word."
    Test 3: free recall (cloze, no spellings)     description-based, no spelling shown

Per-pair score (binary x ∈ {0, 1}, 1 = Ukrainian spelling chosen):

    TAS = w1 × (x1 + x2)/2 + w2 × x3
        with default (w1, w2) = (0.4, 0.6)

Rationale (Gemini-suggested, reasonable):
  - Averaging x1 and x2 cancels positional bias
  - Test 3 measures recall from parametric memory (zero-shot, no priming),
    so it gets the larger weight as the most "honest" probe
  - Storing raw x values per trial means we can replay any weight scheme
    via --reaggregate without re-querying the models

Per-pair Consistency Index:

    CI = 1 - var(x1, x2, x3)        # high CI = stable preference

Outputs land in data/raw/llm_spelling/v2/ to keep v1 untouched.

Usage:
    # Run a sweep
    python -m pipeline.analysis.llm_spelling_test_v2 --models claude-opus-4-6 claude-sonnet-4-6 claude-haiku-4-5
    # Re-aggregate with different weights (no re-query)
    python -m pipeline.analysis.llm_spelling_test_v2 --reaggregate --w1 0.5 --w2 0.5
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
OUT_DIR = ROOT / "data" / "raw" / "llm_spelling" / "v2"
CHECKPOINT_DIR = OUT_DIR / "checkpoints"
SITE_DATA = ROOT / "site" / "src" / "data"

DEFAULT_W1 = 0.4   # forced-choice weight (split across the two orders)
DEFAULT_W2 = 0.6   # free-recall weight
MAX_CONSECUTIVE_FAILURES = 5

TEST_FORCED_RU_FIRST = "forced_ru_first"
TEST_FORCED_UA_FIRST = "forced_ua_first"
TEST_OPEN = "open"
TEST_TYPES = [TEST_FORCED_RU_FIRST, TEST_FORCED_UA_FIRST, TEST_OPEN]


# ── Cloze descriptions for the 54 pairs ──
#
# Each description references the entity *without* using either spelling form,
# so the model has to retrieve the spelling from memory. The "___" marker shows
# where the model is expected to fill the differing token. For most pairs the
# response is one word; for a few (Vladimir Zelensky / Volodymyr Zelenskyy) the
# diff is multi-token and we ask for two words.
DESCRIPTIONS = {
    # geographical (24)
    1:  "What is the capital of Ukraine called in English? Answer with one word.",
    2:  "What is the second-largest city in Ukraine, in the northeast of the country, called in English? Answer with one word.",
    3:  "What is the famous Ukrainian Black Sea port city, known for its grand monumental staircase, called in English? Answer with one word.",
    4:  "What is the largest city in western Ukraine, known for its UNESCO-listed historic old town, called in English? Answer with one word.",
    5:  "What is the southeastern Ukrainian city home to one of Europe's largest nuclear power plants called in English? Answer with one word.",
    6:  "What is the southern Ukrainian shipbuilding port city near the Southern Bug river called in English? Answer with one word.",
    7:  "What is the Ukrainian city on the Dnieper river formerly named Yekaterinoslav called in English today? Answer with one word.",
    8:  "What is the west-central Ukrainian regional capital famous for its synchronized fountain show called in English? Answer with one word.",
    9:  "What is the northwestern Ukrainian regional capital in the Volhynia region called in English? Answer with one word.",
    10: "What is the site of the 1986 nuclear disaster in northern Ukraine called in English? Answer with one word.",
    11: "What is the eastern Ukrainian city, capital of one of the Russian-backed separatist regions, called in English? Answer with one word.",
    15: "What is the major river that flows through the capital of Ukraine into the Black Sea called in English? Answer with one word.",
    16: "What is the river that forms much of the border between Ukraine and Moldova called in English? Answer with one word.",
    17: "What is the eastern Ukrainian heavy-industry and coal-mining region called in English? Answer with one word.",
    19: "What is the westernmost Ukrainian region, beyond the Carpathian mountains, called in English? Answer with one word.",
    20: "What is the historical region of west-central Ukraine, known for its black soil and traditional villages, called in English? Answer with one word.",
    38: "What is the northern Ukrainian regional capital, one of the oldest medieval East Slavic cities, called in English? Answer with one word.",
    39: "What is the southwestern Ukrainian regional capital in the historical Bukovina region called in English? Answer with one word.",
    40: "What is the northwestern-central Ukrainian regional capital, an old Polish-Lithuanian center, called in English? Answer with one word.",
    41: "What is the central Ukrainian regional capital on the Dnieper river, near the homeland of Taras Shevchenko, called in English? Answer with one word.",
    42: "What is the far western Ukrainian city near the Slovak and Hungarian borders called in English? Answer with one word.",
    43: "What is the central Ukrainian industrial city on the Dnieper, with a major hydroelectric dam, called in English? Answer with one word.",
    44: "What is the central Ukrainian regional capital that was renamed in 2016 from its Soviet-era name called today in English? Answer with one word.",
    45: "What is the western Ukrainian regional capital in eastern Galicia called in English? Answer with one word.",

    # food (5)
    21: "Complete: 'The famous chicken dish stuffed with herb butter is called Chicken ___.' Answer with the missing word only.",
    22: "Complete: 'The famous Ukrainian meringue and hazelnut cake is called ___ cake.' Answer with the missing word only.",
    23: "How is the Ukrainian beetroot soup, recognized by UNESCO in 2022, spelled in English? Answer with one word.",
    64: "How is the Ukrainian beetroot soup, recognized by UNESCO in 2022, spelled in English? Answer with one word.",
    46: "How are Ukrainian filled dumplings (similar to Polish pierogi) spelled in English? Answer with one word.",

    # landmarks (6)
    24: "Complete: 'The famous medieval Orthodox cave monastery in the capital of Ukraine is called ___ Pechersk Lavra.' Answer with the missing word only.",
    25: "Complete: 'The 11th-century cathedral named after Saint Sophia in the Ukrainian capital is called Saint Sophia Cathedral ___.' Answer with the missing word only.",
    26: "Complete: 'The contaminated area around the 1986 Ukrainian nuclear disaster is called the ___ Exclusion Zone.' Answer with the missing word only.",
    54: "Complete: 'The ravine in the capital of Ukraine where Nazis killed about 33,000 Jews in September 1941 is called ___ Yar.' Answer with the missing word only.",
    55: "Complete: 'The famous monumental staircase in the Ukrainian Black Sea port city is called the Potemkin Stairs of ___.' Answer with the missing word only.",
    56: "Complete: 'The colossal Soviet-era stainless-steel statue in the capital of Ukraine is called the Motherland Monument ___.' Answer with the missing word only.",

    # institutional (6)
    28: "Complete: 'The main public university in the capital of Ukraine, founded in 1834, is called ___ National University.' Answer with the missing word only.",
    29: "Complete: 'The oldest university in the second-largest city of Ukraine, founded in 1804, is called ___ University.' Answer with the missing word only.",
    30: "Complete: 'The main technical university in the capital of Ukraine is called the ___ Polytechnic Institute.' Answer with the missing word only.",
    31: "Complete: 'The autocephalous Orthodox church based in the capital of Ukraine is called the ___ Patriarchate.' Answer with the missing word only.",
    57: "Complete: 'The main technical university in the largest western Ukrainian city is called the ___ Polytechnic.' Answer with the missing word only.",
    58: "Complete: 'The main university in the Ukrainian Black Sea port city is called ___ National University.' Answer with the missing word only.",

    # sports (5)
    32: "Complete: 'The most successful Ukrainian football club, based in the capital, is called Dynamo ___.' Answer with the missing word only.",
    34: "Complete: 'The national ballet company of Ukraine, based in the capital, is called the ___ Ballet.' Answer with the missing word only.",
    51: "Complete: 'The Ukrainian football club originally based in the eastern city that is the capital of one of the separatist regions is called Zorya ___.' Answer with the missing word only.",
    52: "Complete: 'The Ukrainian football club from the second-largest city of Ukraine, in the northeast, is called Metalist ___.' Answer with the missing word only.",
    53: "Complete: 'The Ukrainian football club from the largest western city of Ukraine is called Karpaty ___.' Answer with the missing word only.",

    # historical (5)
    35: "Complete: 'The medieval East Slavic federation centered around the capital of modern Ukraine is called ___ Rus.' Answer with the missing word only.",
    36: "How is the term for the historical Ukrainian frontier warriors (the founders of the Sich) spelled in English transliteration of Ukrainian? Answer with one word.",
    48: "How is the fast-paced traditional Ukrainian folk dance spelled in English transliteration of Ukrainian? Answer with one word.",
    70: "Complete: 'The medieval prince who Christianized the East Slavs in 988 AD is known in modern English as ___ the Great.' Answer with the missing word only.",
    71: "Complete: 'The medieval rulers of the East Slavic capital of modern Ukraine were called Prince of ___.' Answer with the missing word only.",

    # people (2)
    60: "What is the first name of the Ukrainian heavyweight boxing world champion (last name Usyk) in modern English usage? Answer with one word.",
    61: "What is the full English name (first and last) of the current president of Ukraine? Answer with two words only.",

    # geographical (1, late add)
    72: "What is the eastern Ukrainian city in the Donbas region that was the site of the longest battle of 2022-2023 called in English today? Answer with one word.",
}


# ── Pairs ──

def load_pairs():
    with open(ROOT / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)
    return [p for p in cfg["pairs"]
            if p.get("enabled", True) and not p.get("is_control", False)]


# ── Prompt construction ──

def make_forced_prompt(russian, ukrainian, ru_first):
    a, b = (russian, ukrainian) if ru_first else (ukrainian, russian)
    return f"{a} or {b}? Answer with one word."


def make_open_prompt(pair):
    return DESCRIPTIONS.get(pair["id"])


# ── Classification ──

def _diff_tokens(russian, ukrainian):
    """Return (ru_diff, ua_diff): minimal differing tokens of the two spellings."""
    ru_tokens = russian.split()
    ua_tokens = ukrainian.split()
    if len(ru_tokens) == len(ua_tokens) and len(ru_tokens) > 1:
        ru_diff = " ".join(r for r, u in zip(ru_tokens, ua_tokens) if r != u)
        ua_diff = " ".join(u for r, u in zip(ru_tokens, ua_tokens) if r != u)
        if ru_diff and ua_diff and ru_diff != ua_diff:
            return ru_diff, ua_diff
    return russian, ukrainian


def classify(response, russian, ukrainian):
    """Return 'ukrainian', 'russian', or 'other' for a response string."""
    if not response:
        return "other"
    resp = response.strip().strip('"\'.,!').lower()
    ru_diff, ua_diff = _diff_tokens(russian, ukrainian)
    ru = ru_diff.lower()
    ua = ua_diff.lower()

    has_ua = ua in resp
    has_ru = ru in resp

    if has_ua and has_ru:
        if ua in ru and ua != ru:
            return "ukrainian" if ua in resp.replace(ru, "") else "russian"
        if ru in ua and ua != ru:
            return "russian" if ru in resp.replace(ua, "") else "ukrainian"
        return "ukrainian" if resp.index(ua) < resp.index(ru) else "russian"
    if has_ua:
        return "ukrainian"
    if has_ru:
        return "russian"
    return "other"


def classify_to_x(response, russian, ukrainian):
    """Map a response to x ∈ {1, 0, None}: 1 = Ukrainian, 0 = Russian, None = other."""
    c = classify(response, russian, ukrainian)
    if c == "ukrainian":
        return 1
    if c == "russian":
        return 0
    return None


# ── Providers ──

_keys = {}
def _env(name):
    if name not in _keys:
        _keys[name] = os.environ.get(name, "")
    return _keys[name]


_anthropic_client = None
def query_anthropic(prompt, model):
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic
        _anthropic_client = anthropic.Anthropic()
    try:
        msg = _anthropic_client.messages.create(
            model=model,
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.warning(f"Anthropic {model}: {e}")
        return None


_gemini_client = None
def query_gemini(prompt, model):
    """Query a Google Gemini or Gemma model via the Gemini API.

    Reads GEMINI_API_KEY (or GOOGLE_API_KEY) from env. The new
    `google.genai` package handles both Gemini and the hosted Gemma
    versions through the same endpoint.
    """
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        api_key = _env("GEMINI_API_KEY") or _env("GOOGLE_API_KEY")
        if not api_key:
            log.warning(f"Gemini {model}: no GEMINI_API_KEY in env")
            return None
        _gemini_client = genai.Client(api_key=api_key)
    try:
        resp = _gemini_client.models.generate_content(
            model=model,
            contents=prompt,
        )
        return (resp.text or "").strip()
    except Exception as e:
        log.warning(f"Gemini {model}: {e}")
        return None


def query_ollama(prompt, model):
    base = _env("OLLAMA_BASE_URL") or "http://localhost:11434"
    url = f"{base.rstrip('/')}/api/generate"
    data = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0, "num_predict": 1024},
    }
    try:
        r = requests.post(url, json=data, timeout=300)
        if r.status_code == 200:
            text = r.json().get("response", "")
            if "<think>" in text and "</think>" in text:
                text = text.split("</think>", 1)[1]
            return text.strip()
        log.warning(f"Ollama {model}: {r.status_code} {r.text[:120]}")
        return None
    except Exception as e:
        log.warning(f"Ollama {model}: {e}")
        return None


# ── Model registry (best of family + Claude) ──

MODELS = {
    # ── Anthropic Claude (latest 3 only) ──
    # Anthropic only exposes ~9 months of Claude snapshots via API and earlier
    # versions are deprecated, so a Claude time-series isn't useful for the
    # version-archaeology angle. Keep just the current Opus/Sonnet/Haiku as
    # representative frontier-API data points.
    "claude-opus-4-6":   {"provider": "anthropic", "model": "claude-opus-4-6",          "tier": "frontier", "family": "Anthropic Claude", "release_date": "2026-02"},
    "claude-sonnet-4-6": {"provider": "anthropic", "model": "claude-sonnet-4-6",        "tier": "frontier", "family": "Anthropic Claude", "release_date": "2026-02"},
    "claude-haiku-4-5":  {"provider": "anthropic", "model": "claude-haiku-4-5-20251001","tier": "small",    "family": "Anthropic Claude", "release_date": "2025-10"},

    # ── Open-weight version archaeology (run on GPU box) ──
    # Each family is sampled across multiple historical releases so we can
    # plot per-family Kyiv-adoption trajectories vs release date. The 2022
    # Kiev→Kyiv inflection in the human web corpora should propagate into
    # successive LLM versions with a ~1-year lag if our hypothesis holds.

    # Meta Llama: 2.5 years across the 2022 inflection
    "llama2-7b":       {"provider": "ollama", "model": "llama2:7b",         "tier": "small",    "family": "Meta Llama", "release_date": "2023-07"},
    "llama2-13b":      {"provider": "ollama", "model": "llama2:13b",        "tier": "mid",      "family": "Meta Llama", "release_date": "2023-07"},
    "llama2-70b":      {"provider": "ollama", "model": "llama2:70b",        "tier": "large",    "family": "Meta Llama", "release_date": "2023-07"},
    "llama3-8b":       {"provider": "ollama", "model": "llama3:8b",         "tier": "small",    "family": "Meta Llama", "release_date": "2024-04"},
    "llama3-70b":      {"provider": "ollama", "model": "llama3:70b",        "tier": "large",    "family": "Meta Llama", "release_date": "2024-04"},
    "llama3.1-8b":     {"provider": "ollama", "model": "llama3.1:8b",       "tier": "small",    "family": "Meta Llama", "release_date": "2024-07"},
    "llama3.1-70b":    {"provider": "ollama", "model": "llama3.1:70b",      "tier": "large",    "family": "Meta Llama", "release_date": "2024-07"},
    "llama3.2-1b":     {"provider": "ollama", "model": "llama3.2:1b",       "tier": "tiny",     "family": "Meta Llama", "release_date": "2024-09"},
    "llama3.2-3b":     {"provider": "ollama", "model": "llama3.2:3b",       "tier": "small",    "family": "Meta Llama", "release_date": "2024-09"},
    "llama3.3-70b":    {"provider": "ollama", "model": "llama3.3:70b",      "tier": "large",    "family": "Meta Llama", "release_date": "2024-12"},
    "llama4-scout":    {"provider": "ollama", "model": "llama4:scout",      "tier": "frontier", "family": "Meta Llama", "release_date": "2025-04"},

    # Google Gemma: 2 years
    "gemma-7b":        {"provider": "ollama", "model": "gemma:7b",          "tier": "small",    "family": "Google Gemma", "release_date": "2024-02"},
    "gemma2-2b":       {"provider": "ollama", "model": "gemma2:2b",         "tier": "tiny",     "family": "Google Gemma", "release_date": "2024-06"},
    "gemma2-9b":       {"provider": "ollama", "model": "gemma2:9b",         "tier": "small",    "family": "Google Gemma", "release_date": "2024-06"},
    "gemma2-27b":      {"provider": "ollama", "model": "gemma2:27b",        "tier": "mid",      "family": "Google Gemma", "release_date": "2024-06"},
    "gemma3-1b":       {"provider": "ollama", "model": "gemma3:1b",         "tier": "tiny",     "family": "Google Gemma", "release_date": "2025-03"},
    "gemma3-4b":       {"provider": "ollama", "model": "gemma3:4b",         "tier": "small",    "family": "Google Gemma", "release_date": "2025-03"},
    "gemma3-12b":      {"provider": "ollama", "model": "gemma3:12b",        "tier": "mid",      "family": "Google Gemma", "release_date": "2025-03"},
    "gemma3-27b":      {"provider": "ollama", "model": "gemma3:27b",        "tier": "mid",      "family": "Google Gemma", "release_date": "2025-03"},
    "gemma4-31b":      {"provider": "ollama", "model": "gemma4:31b",        "tier": "frontier", "family": "Google Gemma", "release_date": "2026-04"},

    # Alibaba Qwen: 1.5 years
    "qwen2-7b":        {"provider": "ollama", "model": "qwen2:7b",          "tier": "small",    "family": "Alibaba Qwen", "release_date": "2024-06"},
    "qwen2-72b":       {"provider": "ollama", "model": "qwen2:72b",         "tier": "large",    "family": "Alibaba Qwen", "release_date": "2024-06"},
    "qwen2.5-3b":      {"provider": "ollama", "model": "qwen2.5:3b",        "tier": "small",    "family": "Alibaba Qwen", "release_date": "2024-09"},
    "qwen2.5-7b":      {"provider": "ollama", "model": "qwen2.5:7b",        "tier": "small",    "family": "Alibaba Qwen", "release_date": "2024-09"},
    "qwen2.5-14b":     {"provider": "ollama", "model": "qwen2.5:14b",       "tier": "mid",      "family": "Alibaba Qwen", "release_date": "2024-09"},
    "qwen2.5-32b":     {"provider": "ollama", "model": "qwen2.5:32b",       "tier": "mid",      "family": "Alibaba Qwen", "release_date": "2024-09"},
    "qwen2.5-72b":     {"provider": "ollama", "model": "qwen2.5:72b",       "tier": "large",    "family": "Alibaba Qwen", "release_date": "2024-09"},
    "qwen3-32b":       {"provider": "ollama", "model": "qwen3:32b",         "tier": "frontier", "family": "Alibaba Qwen", "release_date": "2025-04"},

    # ── Google Gemini API (hosted, dated snapshots) ──
    # Google deprecated Gemini 1.5 and the dated 2.0 snapshots from the
    # API by mid-2026, so the accessible time series only goes back to
    # ~mid-2025. Same problem as the Anthropic API.
    # gemini-2.0-flash-001 and gemini-2.0-flash-lite-001 returned 404 on
    # generateContent at probe time despite appearing in list_models.
    "gemini-2.5-flash":            {"provider": "gemini", "model": "models/gemini-2.5-flash",           "tier": "frontier", "family": "Google Gemini", "release_date": "2025-06"},
    "gemini-2.5-pro":              {"provider": "gemini", "model": "models/gemini-2.5-pro",             "tier": "frontier", "family": "Google Gemini", "release_date": "2025-06"},
    "gemini-2.5-flash-lite":       {"provider": "gemini", "model": "models/gemini-2.5-flash-lite",      "tier": "small",    "family": "Google Gemini", "release_date": "2025-07"},
    "gemini-3-flash-preview":      {"provider": "gemini", "model": "models/gemini-3-flash-preview",     "tier": "frontier", "family": "Google Gemini", "release_date": "2025-12"},
    "gemini-3-pro-preview":        {"provider": "gemini", "model": "models/gemini-3-pro-preview",       "tier": "frontier", "family": "Google Gemini", "release_date": "2025-12"},
    "gemini-3.1-flash-lite":       {"provider": "gemini", "model": "models/gemini-3.1-flash-lite-preview","tier":"small",   "family": "Google Gemini", "release_date": "2026-01"},
    "gemini-3.1-pro":              {"provider": "gemini", "model": "models/gemini-3.1-pro-preview",     "tier": "frontier", "family": "Google Gemini", "release_date": "2026-02"},

    # ── Gemma 3/4 hosted via the Gemini API ──
    # Cross-validation against the open-weight ollama runs we'll do
    # tomorrow on GPU. If hosted Gemma 3 27b agrees with ollama Gemma 3
    # 27b, we know our methodology is solid.
    "gemma-3-1b-api":   {"provider": "gemini", "model": "models/gemma-3-1b-it",   "tier": "tiny",  "family": "Google Gemma", "release_date": "2025-03"},
    "gemma-3-4b-api":   {"provider": "gemini", "model": "models/gemma-3-4b-it",   "tier": "small", "family": "Google Gemma", "release_date": "2025-03"},
    "gemma-3-12b-api":  {"provider": "gemini", "model": "models/gemma-3-12b-it",  "tier": "mid",   "family": "Google Gemma", "release_date": "2025-03"},
    "gemma-3-27b-api":  {"provider": "gemini", "model": "models/gemma-3-27b-it",  "tier": "mid",   "family": "Google Gemma", "release_date": "2025-03"},
    "gemma-3n-e2b-api": {"provider": "gemini", "model": "models/gemma-3n-e2b-it", "tier": "tiny",  "family": "Google Gemma", "release_date": "2025-06"},
    "gemma-3n-e4b-api": {"provider": "gemini", "model": "models/gemma-3n-e4b-it", "tier": "small", "family": "Google Gemma", "release_date": "2025-06"},
    "gemma-4-26b-api":  {"provider": "gemini", "model": "models/gemma-4-26b-a4b-it","tier": "mid", "family": "Google Gemma", "release_date": "2026-04"},
    "gemma-4-31b-api":  {"provider": "gemini", "model": "models/gemma-4-31b-it",  "tier": "mid",   "family": "Google Gemma", "release_date": "2026-04"},

    # Mistral: 2 years
    "mistral-7b-v0.1": {"provider": "ollama", "model": "mistral:7b-instruct-v0.1","tier":"small","family": "Mistral", "release_date": "2023-09"},
    "mistral-7b-v0.2": {"provider": "ollama", "model": "mistral:7b-instruct-v0.2","tier":"small","family": "Mistral", "release_date": "2024-03"},
    "mistral-7b-v0.3": {"provider": "ollama", "model": "mistral:7b-instruct-v0.3","tier":"small","family": "Mistral", "release_date": "2024-05"},
    "mixtral-8x7b":    {"provider": "ollama", "model": "mixtral:8x7b",          "tier": "mid",  "family": "Mistral", "release_date": "2023-12"},
    "mistral-small-22b": {"provider": "ollama","model": "mistral-small:22b",    "tier": "mid",  "family": "Mistral", "release_date": "2024-09"},
    "mistral-small-24b": {"provider": "ollama","model": "mistral-small:24b",    "tier": "mid",  "family": "Mistral", "release_date": "2025-01"},
    "mistral-large":   {"provider": "ollama", "model": "mistral-large:123b",     "tier": "frontier","family":"Mistral", "release_date": "2024-11"},
}

# Subset shortcuts
CLAUDE_API = [k for k, v in MODELS.items() if v["family"] == "Anthropic Claude"]
GEMINI_API = [k for k, v in MODELS.items() if v["family"] == "Google Gemini"]
GEMMA_API  = [k for k, v in MODELS.items() if v["family"] == "Google Gemma" and v["provider"] == "gemini"]
LLAMA   = [k for k, v in MODELS.items() if v["family"] == "Meta Llama"]
GEMMA   = [k for k, v in MODELS.items() if v["family"] == "Google Gemma" and v["provider"] == "ollama"]
QWEN    = [k for k, v in MODELS.items() if v["family"] == "Alibaba Qwen"]
MISTRAL = [k for k, v in MODELS.items() if v["family"] == "Mistral"]
OPEN_WEIGHT = LLAMA + GEMMA + QWEN + MISTRAL
HOSTED_HISTORICAL = CLAUDE_API + GEMINI_API + GEMMA_API

BEST_OF_FAMILY = list(MODELS.keys())


def query_model(model_key, prompt):
    info = MODELS[model_key]
    # Qwen 3 supports a /no_think directive that disables hidden reasoning
    if model_key.startswith("qwen3"):
        prompt = prompt + " /no_think"
    if info["provider"] == "ollama":
        return query_ollama(prompt, info["model"])
    if info["provider"] == "anthropic":
        return query_anthropic(prompt, info["model"])
    if info["provider"] == "gemini":
        return query_gemini(prompt, info["model"])
    return None


# ── Checkpoint I/O ──

def _checkpoint_path(model_key):
    return CHECKPOINT_DIR / f"{model_key}.json"


def _load_checkpoint(model_key, info):
    p = _checkpoint_path(model_key)
    if p.exists():
        try:
            with open(p) as f:
                cp = json.load(f)
            cp.setdefault("pairs", [])
            return cp
        except Exception as e:
            log.warning(f"  bad checkpoint {p}: {e} — starting fresh")
    return {
        "model": model_key,
        "tier": info["tier"],
        "family": info["family"],
        "provider": info["provider"],
        "full_model": info["model"],
        "pairs": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _save_checkpoint(cp):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    p = _checkpoint_path(cp["model"])
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(cp, f, indent=2)
    tmp.replace(p)


# ── Aggregation ──

def _per_pair_tas(trials_by_test, w1, w2):
    """Return (tas, ci, n_other) for one pair given its trials."""
    x1 = trials_by_test.get(TEST_FORCED_RU_FIRST, {}).get("x")
    x2 = trials_by_test.get(TEST_FORCED_UA_FIRST, {}).get("x")
    x3 = trials_by_test.get(TEST_OPEN, {}).get("x")
    xs = [x for x in (x1, x2, x3) if x is not None]
    n_other = 3 - len(xs)
    if n_other == 3:
        return None, None, n_other
    # TAS — gracefully degrade if a test is "other": redistribute weight
    if x1 is not None and x2 is not None and x3 is not None:
        tas = w1 * (x1 + x2) / 2 + w2 * x3
    elif x1 is not None and x2 is not None:
        tas = (x1 + x2) / 2
    elif x3 is not None:
        tas = float(x3)
    elif x1 is not None or x2 is not None:
        tas = float(x1 if x1 is not None else x2)
    else:
        tas = None
    # Consistency: 1 - var over the available x values
    if len(xs) >= 2:
        m = sum(xs) / len(xs)
        var = sum((x - m) ** 2 for x in xs) / len(xs)
        ci = 1 - var
    else:
        ci = None
    return tas, ci, n_other


def _summarize(cp, w1=DEFAULT_W1, w2=DEFAULT_W2):
    """Compute model-level summary from per-pair, per-test data."""
    tas_values = []
    ci_values = []
    other_count = 0
    test_totals = {t: {"ua": 0, "ru": 0, "other": 0} for t in TEST_TYPES}
    pf_ru = tot_ru = 0   # picks-first when RU was first
    pf_ua = tot_ua = 0   # picks-first when UA was first

    n_pairs_complete = 0  # all 3 tests decisive
    n_pairs = len(cp["pairs"])

    for pair in cp["pairs"]:
        trials_by_test = {t["test"]: t for t in pair["trials"]}
        tas, ci, n_other = _per_pair_tas(trials_by_test, w1, w2)
        other_count += n_other

        # Test-level breakdown
        for test_name in TEST_TYPES:
            t = trials_by_test.get(test_name)
            if not t:
                continue
            x = t.get("x")
            if x == 1:
                test_totals[test_name]["ua"] += 1
            elif x == 0:
                test_totals[test_name]["ru"] += 1
            else:
                test_totals[test_name]["other"] += 1

        # Position bias from forced tests
        for tname, ru_first in [(TEST_FORCED_RU_FIRST, True), (TEST_FORCED_UA_FIRST, False)]:
            t = trials_by_test.get(tname)
            if not t or t.get("x") is None:
                continue
            x = t["x"]  # 1=UA, 0=RU
            picked_first = (x == 0 and ru_first) or (x == 1 and not ru_first)
            if ru_first:
                tot_ru += 1
                if picked_first:
                    pf_ru += 1
            else:
                tot_ua += 1
                if picked_first:
                    pf_ua += 1

        if tas is not None:
            tas_values.append(tas)
        if ci is not None:
            ci_values.append(ci)
        if n_other == 0:
            n_pairs_complete += 1

    if tot_ru and tot_ua:
        position_bias = round((pf_ru / tot_ru + pf_ua / tot_ua) / 2 * 100, 1)
    elif tot_ru:
        position_bias = round(pf_ru / tot_ru * 100, 1)
    elif tot_ua:
        position_bias = round(pf_ua / tot_ua * 100, 1)
    else:
        position_bias = None

    def pct(d):
        tot = d["ua"] + d["ru"]
        return round(d["ua"] / tot * 100, 1) if tot else None

    cp["summary"] = {
        "weights": {"w1_forced": w1, "w2_open": w2},
        "n_pairs": n_pairs,
        "n_pairs_complete": n_pairs_complete,
        "tas_mean": round(sum(tas_values) / len(tas_values) * 100, 1) if tas_values else None,
        "ci_mean": round(sum(ci_values) / len(ci_values), 3) if ci_values else None,
        "ua_pct_forced_ru_first": pct(test_totals[TEST_FORCED_RU_FIRST]),
        "ua_pct_forced_ua_first": pct(test_totals[TEST_FORCED_UA_FIRST]),
        "ua_pct_open":            pct(test_totals[TEST_OPEN]),
        "position_bias": position_bias,
        "other_total": other_count,
        "other_rate": round(other_count / (3 * n_pairs) * 100, 1) if n_pairs else 0,
    }
    return cp


# ── Sweep ──

def run(model_keys, force=False):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    pairs = load_pairs()

    # Build the test plan once: for each pair, the 3 test definitions
    log.info(f"v2 (TAS) sweep: {len(pairs)} pairs × 3 tests × {len(model_keys)} models "
             f"= {len(pairs) * 3 * len(model_keys)} queries")

    for model_key in model_keys:
        info = MODELS[model_key]
        log.info(f"\n  Model: {model_key} ({info['model']}, {info['family']})")

        cp = _load_checkpoint(model_key, info) if not force else {
            "model": model_key, "tier": info["tier"], "family": info["family"],
            "provider": info["provider"], "full_model": info["model"],
            "pairs": [], "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        done_count = sum(len(p["trials"]) for p in cp["pairs"])
        if done_count:
            log.info(f"    Resuming: {done_count}/{len(pairs)*3} trials already done")

        consecutive_failures = 0

        for pair in pairs:
            entry = next((p for p in cp["pairs"] if p["pair_id"] == pair["id"]), None)
            if entry is None:
                entry = {
                    "pair_id": pair["id"],
                    "russian": pair["russian"],
                    "ukrainian": pair["ukrainian"],
                    "category": pair.get("category", ""),
                    "trials": [],
                }
                cp["pairs"].append(entry)

            existing_tests = {t["test"] for t in entry["trials"]}

            test_plan = [
                (TEST_FORCED_RU_FIRST, make_forced_prompt(pair["russian"], pair["ukrainian"], ru_first=True)),
                (TEST_FORCED_UA_FIRST, make_forced_prompt(pair["russian"], pair["ukrainian"], ru_first=False)),
                (TEST_OPEN,            make_open_prompt(pair)),
            ]

            for test_name, prompt in test_plan:
                if test_name in existing_tests:
                    continue
                if prompt is None:
                    log.warning(f"    pair {pair['id']}: no description for open test, skipping")
                    continue
                try:
                    response = query_model(model_key, prompt)
                except KeyboardInterrupt:
                    _save_checkpoint(_summarize(cp))
                    log.info("    interrupted — checkpoint saved")
                    raise
                if response is None:
                    log.warning(f"    pair {pair['id']} {test_name}: no response")
                    consecutive_failures += 1
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        log.error(f"    {MAX_CONSECUTIVE_FAILURES} consecutive failures — skipping rest of {model_key}")
                        _save_checkpoint(_summarize(cp))
                        consecutive_failures = -1  # sentinel; checked outside
                        break
                    continue
                consecutive_failures = 0

                x = classify_to_x(response, pair["russian"], pair["ukrainian"])
                entry["trials"].append({
                    "test": test_name,
                    "prompt": prompt,
                    "response": response[:200],
                    "x": x,
                })
                _save_checkpoint(cp)
                time.sleep(0.05)

            # End of test loop. If sentinel was set, also break the pair loop
            # so we move on to the next model instead of returning from run().
            if consecutive_failures == -1:
                break

        if consecutive_failures == -1:
            log.info(f"    moving on to next model")
            continue

        cp = _summarize(cp)
        _save_checkpoint(cp)
        s = cp["summary"]
        log.info(f"    TAS={s['tas_mean']}%  open={s['ua_pct_open']}%  "
                 f"forced(ru1st)={s['ua_pct_forced_ru_first']}%  forced(ua1st)={s['ua_pct_forced_ua_first']}%  "
                 f"pos_bias={s['position_bias']}%  CI={s['ci_mean']}  other={s['other_rate']}%")

    write_aggregate()


def write_aggregate(w1=DEFAULT_W1, w2=DEFAULT_W2):
    if not CHECKPOINT_DIR.exists():
        return
    all_results = []
    for cp_path in sorted(CHECKPOINT_DIR.glob("*.json")):
        with open(cp_path) as f:
            cp = json.load(f)
        cp = _summarize(cp, w1, w2)
        with open(cp_path, "w") as f:
            json.dump(cp, f, indent=2)
        all_results.append(cp)

    out_path = OUT_DIR / "llm_spelling_v2_results.json"
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    log.info(f"\nSaved: {out_path}  ({len(all_results)} models, weights w1={w1}, w2={w2})")

    log.info(f"\n{'Model':18s} {'Family':18s} {'TAS':>6} {'open':>6} {'forced':>8} {'pos_b':>7} {'CI':>5} {'other':>7}")
    log.info("-" * 90)
    for r in sorted(all_results, key=lambda x: x["summary"]["tas_mean"] or 0):
        s = r["summary"]
        forced = f"{((s['ua_pct_forced_ru_first'] or 0) + (s['ua_pct_forced_ua_first'] or 0))/2:.1f}"
        log.info(f"{r['model']:18s} {r.get('family',''):18s} "
                 f"{s['tas_mean']:>5.1f}% {s['ua_pct_open']:>5.1f}% {forced:>7}% "
                 f"{s['position_bias']:>6.1f}% {s['ci_mean']:>5.3f} {s['other_rate']:>6.1f}%")

    site_data = [{
        "model": r["model"], "family": r.get("family", ""),
        "tas_mean": r["summary"]["tas_mean"],
        "ci_mean": r["summary"]["ci_mean"],
        "ua_pct_open": r["summary"]["ua_pct_open"],
        "ua_pct_forced_ru_first": r["summary"]["ua_pct_forced_ru_first"],
        "ua_pct_forced_ua_first": r["summary"]["ua_pct_forced_ua_first"],
        "position_bias": r["summary"]["position_bias"],
        "other_rate": r["summary"]["other_rate"],
        "weights": r["summary"]["weights"],
    } for r in all_results]
    site_path = SITE_DATA / "llm_spelling_v2.json"
    with open(site_path, "w") as f:
        json.dump(site_data, f, indent=2)
    log.info(f"Saved site data: {site_path}")


def reaggregate(w1=DEFAULT_W1, w2=DEFAULT_W2):
    """Recompute summaries from stored x values, no re-querying."""
    if not CHECKPOINT_DIR.exists():
        log.info("no v2 checkpoints")
        return
    write_aggregate(w1, w2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="*", default=None,
                        help="model keys to test; default = best-of-family")
    parser.add_argument("--force", action="store_true", help="ignore checkpoints")
    parser.add_argument("--reaggregate", action="store_true",
                        help="recompute summaries from stored data, no querying")
    parser.add_argument("--w1", type=float, default=DEFAULT_W1, help="forced-choice weight (default 0.4)")
    parser.add_argument("--w2", type=float, default=DEFAULT_W2, help="free-recall weight (default 0.6)")
    args = parser.parse_args()

    if args.reaggregate:
        reaggregate(args.w1, args.w2)
        return

    models = args.models or BEST_OF_FAMILY
    run(models, force=args.force)


if __name__ == "__main__":
    main()
