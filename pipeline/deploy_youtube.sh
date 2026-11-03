#!/bin/bash
# Enrich -> rebuild -> build -> local commit for one pair. Safe to run repeatedly: it
# only commits when the site data actually changed, so a no-op run leaves no commit.
#   ./pipeline/deploy_youtube.sh chornobyl          # rebuild SERIES_END's year (2026) only
#   ./pipeline/deploy_youtube.sh chornobyl 2025,2026
#   ./pipeline/deploy_youtube.sh chornobyl all      # every year: see below first
#
# The default is the window's final year, not every year. An enrich over every
# year re-fetches stored rows lacking metadata and meets the orphan rows of
# superseded collections (chornobyl: 3,052, 2,298 of them verified 2010-2024),
# and either moves the shipped 2010-2025 series. enrich refuses orphans unless
# told --orphans keep|drop; run it by hand for that decision.
#
# It never pushes. Pushes go by hand through the pre-push hook (build, i18n parity,
# runtime smoke), and a deploy is confirmed by curling the live site for a marker:
# pushed is not deployed. This script used to push on its own, under commits
# carrying a session-attribution trailer that the repo forbids (4 such commits on
# 2026-08-28).
#
# pipefail: without it each `cmd | tail` / `cmd | grep` step tested only the last
# command of the pipe. A crashed enrich passed, and so did a failed astro build,
# since its "error" lines satisfied the grep.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
PAIR="${1:?usage: deploy_youtube.sh <pair> [years, e.g. 2026]}"
YEARS="${2:-$(/opt/anaconda3/bin/python -c 'from pipeline.config import SERIES_END; print(SERIES_END[:4])')}"
[ -z "$YEARS" ] && { echo "FAIL: could not read SERIES_END from pipeline.config"; exit 1; }
KEY=$(gcloud services api-keys get-key-string 029b0141-1889-4665-a569-36d75c0f6191 \
      --project=kyivnotkiev-yt --format='value(keyString)' 2>/dev/null)
[ -z "$KEY" ] && { echo "FAIL: no API key"; exit 1; }

# Incremental (enrich's default, named here for the reader): only ids lacking
# metadata are fetched. A failed fetch exits 1 and writes nothing, so this step
# can no longer blank a pair the way zaporizhzhia was blanked on 2026-09-14.
ENRICH=(--pair "$PAIR" --incremental --api-key "$KEY")
[ "$YEARS" != all ] && ENRICH+=(--years "$YEARS")
echo "STEP enrich $PAIR (years $YEARS)"
# The whole log, not its tail: `| tail -8` showed only the summary printed after
# "saved" and cut the WARNING lines (orphans, blank re-fetches) that say what moved.
/opt/anaconda3/bin/python -u -m pipeline.ingestion.youtube_enrich "${ENRICH[@]}" 2>&1 \
  || { echo "FAIL: enrich (the enriched file is unchanged on a failed fetch or refusal)"; exit 1; }

echo "STEP rebuild"
YOUTUBE_API_KEY="$KEY" /opt/anaconda3/bin/python -m pipeline.rebuild 2>&1 \
  | grep -E "ALL GOOD|issues found|ERROR" || { echo "FAIL: rebuild"; exit 1; }

echo "STEP site build"
( cd site && npm run build 2>&1 | grep -E "Complete!|error" ) || { echo "FAIL: build"; exit 1; }

if git diff --quiet -- site/src/data/ && [ -z "$(git ls-files --others --exclude-standard -- site/src/data/)" ]; then
  echo "STEP no change — nothing to commit"
else
  # Completeness by the orchestrator's own rule (every month the window expects,
  # both variants), not a private re-derivation of it.
  SPAN=$(/opt/anaconda3/bin/python - "$PAIR" <<'PY'
import sys
from pipeline.build_youtube_census import (STUDY_END_YEAR, STUDY_START_YEAR,
                                           declared_depths, is_complete)
from pipeline.config import SERIES_END
pair = sys.argv[1]
done = [y for y in range(STUDY_START_YEAR, STUDY_END_YEAR + 1)
        if all(is_complete(pair, v, y) for v in ("russian", "ukrainian"))]
span = f"{min(done)}-{max(done)}, {len(done)} complete" if done else "none complete"
print(f"{declared_depths().get(pair) or 'undeclared'} depth, years {span}, "
      f"window through {SERIES_END}")
PY
) || { echo "FAIL: completeness check"; exit 1; }
  git add -A -- site/src/data/
  git commit -q -m "data: $PAIR YouTube refreshed ($SPAN)

Steps (pipeline/deploy_youtube.sh): incremental enrich (years $YEARS), rebuild, build." \
    || { echo "FAIL: commit"; exit 1; }
  echo "STEP committed locally ($SPAN). Push by hand; the pre-push hook gates it."
fi
echo "DONE"
