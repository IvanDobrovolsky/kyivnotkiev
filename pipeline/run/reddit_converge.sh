#!/bin/zsh
# Drive the Reddit holdout tables to convergence.
#
# Each export ships probed-live posts ONLY and writes the ranked remainder to
# data/audit/reddit_holdout_candidates.json. Probing that remainder promotes
# replacements, which the next export picks up. Converged = every table at 100,
# or its candidate pool fully probed and simply not holding 100 live posts.
#
#   ./pipeline/run/reddit_converge.sh
#
# Liveness verdicts are cached in data/audit/reddit_liveness.json and carry a
# SCHEMA tag; bumping SCHEMA in site/reddit_liveness.mjs re-probes everything.
set -u
cd "$(dirname "$0")/../.."
LOG=data/audit/reddit_converge.log
: > $LOG
for round in 1 2 3 4 5 6; do
  n=$(/opt/anaconda3/bin/python -c "
import json,pathlib
p=pathlib.Path('data/audit/reddit_holdout_candidates.json')
print(sum(len(v) for v in json.loads(p.read_text()).values()) if p.exists() else 0)")
  echo "=== round $round: $n unprobed candidates $(date +%H:%M:%S)" >> $LOG
  # Concurrency 8 with context recycling; higher walls, lower is needlessly slow.
  node site/reddit_liveness.mjs --concurrency=8 >> $LOG 2>&1
  /opt/anaconda3/bin/python -m pipeline.export_site_data >> $LOG 2>&1
  /opt/anaconda3/bin/python -m pipeline.audit.holdout_convergence >> $LOG 2>&1
  left=$(/opt/anaconda3/bin/python -c "
import json,pathlib
p=pathlib.Path('data/audit/reddit_holdout_candidates.json')
print(sum(len(v) for v in json.loads(p.read_text()).values()) if p.exists() else 0)")
  echo "--- candidates left after round $round: $left" >> $LOG
  [ "$left" -eq 0 ] && { echo "pool fully probed" >> $LOG; break; }
done
echo "REDDIT CONVERGED $(date +%H:%M:%S)" >> $LOG
