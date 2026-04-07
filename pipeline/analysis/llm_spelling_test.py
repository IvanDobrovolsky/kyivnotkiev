"""LLM Spelling Test: Do language models perpetuate Russian or Ukrainian toponyms?

Tests multiple LLMs with neutral prompts to reveal their default spelling
preferences for Ukrainian place names. Each model answers 54 toponym pairs.

Usage:
    python -m pipeline.analysis.llm_spelling_test [--models all|free|claude]
"""

import argparse
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
OUT_DIR = ROOT / "data" / "raw" / "llm_spelling"
CHECKPOINT_DIR = OUT_DIR / "checkpoints"
SITE_DATA = ROOT / "site" / "src" / "data"


def load_pairs():
    with open(ROOT / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)
    return [p for p in cfg["pairs"]
            if p.get("enabled", True) and not p.get("is_control", False)]


def make_prompt(pair):
    """Create a neutral prompt that doesn't hint at the answer."""
    pid = pair["id"]
    ru = pair["russian"]
    ua = pair["ukrainian"]
    cat = pair.get("category", "")

    # Neutral descriptions — don't mention either spelling
    descriptions = {
        1: "the capital city of Ukraine",
        2: "the second-largest city in Ukraine, in the northeast",
        3: "the Ukrainian port city on the Black Sea, famous for its steps",
        4: "the major city in western Ukraine, known for its architecture",
        5: "the Ukrainian city on the Dnieper, home to a large nuclear power plant",
        6: "the Ukrainian port city in the south, near the Bug river",
        7: "the large Ukrainian city on the Dnieper, formerly known as Yekaterinoslav",
        8: "the Ukrainian city in the west-central region, a regional capital",
        9: "the Ukrainian city in the northwest, a regional capital in Volhynia",
        10: "the site of the 1986 nuclear disaster in Ukraine",
        11: "the city in eastern Ukraine, capital of one of the separatist regions",
        61: "Ukraine's current president (full name, first and last)",
        70: "the medieval prince who Christianized Rus in 988 AD (English name)",
        72: "the Ukrainian city that was the site of the longest battle in 2022-2023",
    }

    if pid in descriptions:
        return f"How do you spell {descriptions[pid]} in English? Answer with just the name, nothing else."

    if cat == "food":
        return f"Which spelling is standard in English: '{ru}' or '{ua}'? Answer with just the spelling."
    elif cat == "sports":
        return f"Which spelling is standard in English: '{ru}' or '{ua}'? Answer with just the name."
    elif cat == "historical":
        return f"Which spelling is standard in English: '{ru}' or '{ua}'? Answer with just the spelling."
    else:
        return f"Which English spelling is standard for the Ukrainian place: '{ru}' or '{ua}'? Answer with just the spelling."


def _diff_tokens(russian, ukrainian):
    """Return (ru_diff, ua_diff): the minimal differing tokens of the two spellings.

    For pairs like ('Saint Sophia Cathedral Kiev', 'Saint Sophia Cathedral Kyiv'),
    returns ('Kiev', 'Kyiv') so the classifier can match a response of just "Kyiv".
    Falls back to the full strings for non-token-aligned pairs.
    """
    ru_tokens = russian.split()
    ua_tokens = ukrainian.split()
    if len(ru_tokens) == len(ua_tokens) and len(ru_tokens) > 1:
        ru_diff = " ".join(r for r, u in zip(ru_tokens, ua_tokens) if r != u)
        ua_diff = " ".join(u for r, u in zip(ru_tokens, ua_tokens) if r != u)
        if ru_diff and ua_diff and ru_diff != ua_diff:
            return ru_diff, ua_diff
    return russian, ukrainian


def classify_response(response, russian, ukrainian):
    """Classify LLM response as russian, ukrainian, or other.

    Compares against the *differing substring* of the pair so that responses
    like "Kyiv" correctly resolve "Saint Sophia Cathedral Kiev/Kyiv".
    """
    resp = response.strip().strip('"\'.,!').lower()
    ru_diff, ua_diff = _diff_tokens(russian, ukrainian)
    ru_lower = ru_diff.lower()
    ua_lower = ua_diff.lower()

    has_ua = ua_lower in resp
    has_ru = ru_lower in resp

    # Disambiguate when one diff is a substring of the other (e.g. Odesa ⊂ Odessa).
    if has_ua and has_ru:
        if ua_lower in ru_lower and ua_lower != ru_lower:
            # ua is substring of ru, so a real ua match must occur outside any ru match
            stripped = resp.replace(ru_lower, "")
            if ua_lower in stripped:
                return "ukrainian"
            return "russian"
        if ru_lower in ua_lower and ua_lower != ru_lower:
            stripped = resp.replace(ua_lower, "")
            if ru_lower in stripped:
                return "russian"
            return "ukrainian"
        # Both mentioned independently — first occurrence wins
        return "ukrainian" if resp.index(ua_lower) < resp.index(ru_lower) else "russian"
    if has_ua:
        return "ukrainian"
    if has_ru:
        return "russian"
    return "other"


def reclassify_checkpoints():
    """Re-run classification on stored responses without re-querying models."""
    if not CHECKPOINT_DIR.exists():
        log.info("No checkpoints to reclassify")
        return
    for cp_path in sorted(CHECKPOINT_DIR.glob("*.json")):
        with open(cp_path) as f:
            cp = json.load(f)
        changed = 0
        for p in cp["pairs"]:
            new = classify_response(p["response"], p["russian"], p["ukrainian"])
            if new != p["classified"]:
                p["classified"] = new
                changed += 1
        cp = _summarize(cp)
        with open(cp_path, "w") as f:
            json.dump(cp, f, indent=2)
        s = cp["summary"]
        log.info(f"  {cp['model']:18s}  reclassified {changed:>3} pairs  →  RU={s['russian']} UA={s['ukrainian']} other={s['other']}  {s['adoption']:.1f}% UA")


# ── Model Providers ──

def query_anthropic(prompt, model="claude-sonnet-4-20250514"):
    """Query Claude via direct API."""
    import anthropic
    client = anthropic.Anthropic()
    msg = client.messages.create(
        model=model,
        max_tokens=50,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()


def query_groq(prompt, model="llama-3.1-70b-versatile"):
    """Query open models via Groq (free tier)."""
    url = "https://api.groq.com/openai/v1/chat/completions"
    api_key = _get_key("GROQ_API_KEY")
    if not api_key:
        return None
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    data = {"model": model, "messages": [{"role": "user", "content": prompt}], "max_tokens": 50, "temperature": 0}
    try:
        r = requests.post(url, headers=headers, json=data, timeout=15)
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"].strip()
        log.warning(f"Groq {model}: {r.status_code}")
        return None
    except Exception as e:
        log.warning(f"Groq {model}: {e}")
        return None


def query_ollama(prompt, model="llama3.2:3b"):
    """Query a model running on an Ollama server (local or remote via OLLAMA_BASE_URL).

    num_predict=200 because some models (e.g. gemma4) emit chat/thinking tokens
    before the visible answer and need a bigger budget than the obvious ~10 chars.
    """
    base = _get_key("OLLAMA_BASE_URL") or "http://localhost:11434"
    url = f"{base.rstrip('/')}/api/generate"
    data = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0, "num_predict": 200},
    }
    try:
        r = requests.post(url, json=data, timeout=120)
        if r.status_code == 200:
            return r.json().get("response", "").strip()
        log.warning(f"Ollama {model}: {r.status_code} {r.text[:120]}")
        return None
    except Exception as e:
        log.warning(f"Ollama {model}: {e}")
        return None


def query_openrouter(prompt, model="deepseek/deepseek-chat-v3-0324:free"):
    """Query models via OpenRouter (many free models)."""
    url = "https://openrouter.ai/api/v1/chat/completions"
    api_key = _get_key("OPENROUTER_API_KEY")
    if not api_key:
        return None
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    data = {"model": model, "messages": [{"role": "user", "content": prompt}], "max_tokens": 50, "temperature": 0}
    try:
        r = requests.post(url, headers=headers, json=data, timeout=30)
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"].strip()
        log.warning(f"OpenRouter {model}: {r.status_code} {r.text[:100]}")
        return None
    except Exception as e:
        log.warning(f"OpenRouter {model}: {e}")
        return None


_keys = {}
def _get_key(name):
    if name not in _keys:
        import os
        _keys[name] = os.environ.get(name, "")
    return _keys[name]


# Model registry
MODELS = {
    # Claude (via anthropic SDK)
    "claude-sonnet": {"provider": "anthropic", "model": "claude-sonnet-4-20250514", "tier": "frontier"},
    "claude-haiku": {"provider": "anthropic", "model": "claude-haiku-4-5-20251001", "tier": "small"},

    # Groq (free, fast)
    "llama-70b": {"provider": "groq", "model": "llama-3.1-70b-versatile", "tier": "open"},
    "llama-8b": {"provider": "groq", "model": "llama-3.1-8b-instant", "tier": "small"},
    "mixtral": {"provider": "groq", "model": "mixtral-8x7b-32768", "tier": "open"},
    "gemma2-9b": {"provider": "groq", "model": "gemma2-9b-it", "tier": "small"},

    # OpenRouter (free tiers)
    "deepseek-v3": {"provider": "openrouter", "model": "deepseek/deepseek-chat-v3-0324:free", "tier": "frontier"},
    "deepseek-r1": {"provider": "openrouter", "model": "deepseek/deepseek-r1:free", "tier": "frontier"},
    "qwen-72b": {"provider": "openrouter", "model": "qwen/qwen-2.5-72b-instruct:free", "tier": "open"},
    "mistral-small": {"provider": "openrouter", "model": "mistralai/mistral-small-3.1-24b-instruct:free", "tier": "open"},

    # Ollama small (local or remote GPU box via OLLAMA_BASE_URL)
    "llama3.2-1b":  {"provider": "ollama", "model": "llama3.2:1b",  "tier": "tiny"},
    "llama3.2-3b":  {"provider": "ollama", "model": "llama3.2:3b",  "tier": "small"},
    "qwen2.5-3b":   {"provider": "ollama", "model": "qwen2.5:3b",   "tier": "small"},
    "gemma2-2b":    {"provider": "ollama", "model": "gemma2:2b",    "tier": "tiny"},
    "phi3-mini":    {"provider": "ollama", "model": "phi3:mini",    "tier": "small"},

    # Ollama mid/large (GPU box only)
    "qwen2.5-14b":  {"provider": "ollama", "model": "qwen2.5:14b",  "tier": "mid"},
    "gemma2-27b":   {"provider": "ollama", "model": "gemma2:27b",   "tier": "mid"},
    "qwen2.5-32b":  {"provider": "ollama", "model": "qwen2.5:32b",  "tier": "mid"},
    "llama3.3-70b": {"provider": "ollama", "model": "llama3.3:70b", "tier": "large"},
    "qwen2.5-72b":  {"provider": "ollama", "model": "qwen2.5:72b",  "tier": "large"},

    # Google Gemma 3 (released March 2025)
    "gemma3-1b":    {"provider": "ollama", "model": "gemma3:1b",    "tier": "tiny"},
    "gemma3-4b":    {"provider": "ollama", "model": "gemma3:4b",    "tier": "small"},
    "gemma3-12b":   {"provider": "ollama", "model": "gemma3:12b",   "tier": "mid"},
    "gemma3-27b":   {"provider": "ollama", "model": "gemma3:27b",   "tier": "mid"},

    # Google Gemma 4 (released April 2026)
    "gemma4-e2b":   {"provider": "ollama", "model": "gemma4:e2b",   "tier": "tiny"},
    "gemma4-e4b":   {"provider": "ollama", "model": "gemma4:e4b",   "tier": "small"},
    "gemma4-26b":   {"provider": "ollama", "model": "gemma4:26b",   "tier": "mid"},
    "gemma4-31b":   {"provider": "ollama", "model": "gemma4:31b",   "tier": "mid"},
}

SMALL_OLLAMA = ["llama3.2-1b", "llama3.2-3b", "qwen2.5-3b", "gemma2-2b", "phi3-mini"]
LARGE_OLLAMA = ["qwen2.5-14b", "gemma2-27b", "qwen2.5-32b", "llama3.3-70b", "qwen2.5-72b"]
GEMMA3_OLLAMA = ["gemma3-1b", "gemma3-4b", "gemma3-12b", "gemma3-27b"]
GEMMA4_OLLAMA = ["gemma4-e2b", "gemma4-e4b", "gemma4-26b", "gemma4-31b"]
ALL_OLLAMA = SMALL_OLLAMA + LARGE_OLLAMA + GEMMA3_OLLAMA + GEMMA4_OLLAMA


def query_model(model_key, prompt):
    """Route to the right provider."""
    info = MODELS[model_key]
    provider = info["provider"]
    model = info["model"]

    if provider == "anthropic":
        return query_anthropic(prompt, model)
    elif provider == "groq":
        return query_groq(prompt, model)
    elif provider == "openrouter":
        return query_openrouter(prompt, model)
    elif provider == "ollama":
        return query_ollama(prompt, model)
    return None


def _checkpoint_path(model_key):
    return CHECKPOINT_DIR / f"{model_key}.json"


def _load_checkpoint(model_key, info):
    """Load existing per-model checkpoint or create a fresh one."""
    p = _checkpoint_path(model_key)
    if p.exists():
        try:
            with open(p) as f:
                cp = json.load(f)
            cp.setdefault("pairs", [])
            return cp
        except Exception as e:
            log.warning(f"  Could not read checkpoint {p}: {e} — starting fresh")
    return {
        "model": model_key,
        "tier": info["tier"],
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


def _summarize(cp):
    ru = sum(1 for x in cp["pairs"] if x["classified"] == "russian")
    ua = sum(1 for x in cp["pairs"] if x["classified"] == "ukrainian")
    other = sum(1 for x in cp["pairs"] if x["classified"] == "other")
    total = ru + ua + other
    adopt = ua / (ru + ua) * 100 if (ru + ua) > 0 else 0
    cp["summary"] = {"russian": ru, "ukrainian": ua, "other": other,
                     "total": total, "adoption": round(adopt, 1)}
    return cp


def run_test(model_keys=None, force=False):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    pairs = load_pairs()

    if model_keys is None:
        model_keys = list(MODELS.keys())

    log.info(f"LLM Spelling Test: {len(pairs)} pairs × {len(model_keys)} models = {len(pairs) * len(model_keys)} queries")

    results = []

    for model_key in model_keys:
        info = MODELS[model_key]
        log.info(f"\n  Model: {model_key} ({info['model']}, {info['tier']})")

        cp = {"model": model_key, "tier": info["tier"], "provider": info["provider"],
              "full_model": info["model"], "pairs": [],
              "timestamp": datetime.now(timezone.utc).isoformat()} if force else _load_checkpoint(model_key, info)

        done_ids = {x["pair_id"] for x in cp["pairs"]}
        if done_ids:
            log.info(f"    Resuming: {len(done_ids)}/{len(pairs)} pairs already done")

        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 5
        for pair in pairs:
            if pair["id"] in done_ids:
                continue
            prompt = make_prompt(pair)
            try:
                response = query_model(model_key, prompt)
                if response is None:
                    log.warning(f"    Pair {pair['id']}: no response")
                    consecutive_failures += 1
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        log.error(f"    {MAX_CONSECUTIVE_FAILURES} consecutive failures — aborting model {model_key}")
                        break
                    continue
                consecutive_failures = 0

                variant = classify_response(response, pair["russian"], pair["ukrainian"])

                cp["pairs"].append({
                    "pair_id": pair["id"],
                    "russian": pair["russian"],
                    "ukrainian": pair["ukrainian"],
                    "category": pair.get("category", ""),
                    "prompt": prompt,
                    "response": response[:200],
                    "classified": variant,
                })
                _save_checkpoint(cp)

                time.sleep(0.1)
            except KeyboardInterrupt:
                log.info("    Interrupted — checkpoint saved, rerun to resume")
                _save_checkpoint(_summarize(cp))
                raise
            except Exception as e:
                log.warning(f"    Pair {pair['id']}: {e}")

        cp = _summarize(cp)
        _save_checkpoint(cp)

        s = cp["summary"]
        log.info(f"    Results: RU={s['russian']} UA={s['ukrainian']} other={s['other']} → {s['adoption']:.0f}% UA adoption")
        results.append(cp)

    write_aggregate()
    return results


def write_aggregate():
    """Build llm_spelling_results.json + site data from ALL existing checkpoints."""
    if not CHECKPOINT_DIR.exists():
        return
    all_results = []
    for cp_path in sorted(CHECKPOINT_DIR.glob("*.json")):
        with open(cp_path) as f:
            all_results.append(json.load(f))

    out_path = OUT_DIR / "llm_spelling_results.json"
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    log.info(f"\nSaved: {out_path}  ({len(all_results)} models)")

    log.info(f"\n{'Model':25s} {'Tier':10s} {'RU':>4} {'UA':>4} {'?':>4} {'UA%':>6}")
    log.info("-" * 60)
    for r in sorted(all_results, key=lambda x: x["summary"]["adoption"]):
        s = r["summary"]
        log.info(f"{r['model']:25s} {r['tier']:10s} {s['russian']:>4} {s['ukrainian']:>4} {s['other']:>4} {s['adoption']:>5.1f}%")

    site_data = [{
        "model": r["model"], "tier": r["tier"],
        "russian": r["summary"]["russian"],
        "ukrainian": r["summary"]["ukrainian"],
        "other": r["summary"]["other"],
        "adoption": r["summary"]["adoption"],
    } for r in all_results]

    site_path = SITE_DATA / "llm_spelling.json"
    with open(site_path, "w") as f:
        json.dump(site_data, f, indent=2)
    log.info(f"Saved site data: {site_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="*", default=None,
                        help="Model keys to test (default: all). Use 'small-ollama' for the small ollama sweep.")
    parser.add_argument("--force", action="store_true",
                        help="Ignore checkpoints and re-run from scratch")
    parser.add_argument("--reclassify", action="store_true",
                        help="Re-classify stored responses without re-querying models")
    args = parser.parse_args()

    if args.reclassify:
        reclassify_checkpoints()
        write_aggregate()
        return

    models = args.models
    if models == ["small-ollama"]:
        models = SMALL_OLLAMA
    elif models == ["large-ollama"]:
        models = LARGE_OLLAMA
    elif models == ["all-ollama"]:
        models = ALL_OLLAMA
    run_test(models, force=args.force)


if __name__ == "__main__":
    main()
