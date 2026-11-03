#!/bin/zsh
# Full statistical rebuild: keyness -> clusters -> export -> audits.
#
# Run this after anything that changes the store or changes keyness/clusters
# code. The 2026-09-21 run took at least 2h35m for 24 pairs (keyness 19:13 to
# 20:58, clusters to 21:27, export to 21:48; its start was not logged, so ~3.5h
# was never a measurement). Everything is sequential on
# purpose: UMAP OOMs above ~40K texts, so clusters must never run in parallel,
# and the exporter takes an flock so two of these cannot interleave writes.
#
#   ./pipeline/run/full_rebuild.sh
#
# Progress: tail -f data/audit/full_rebuild.log
# Pair boundaries appear as "=== <pair> ===" (keyness) and
# "=== clusters <pair> <time>" (clusters).
set -u
cd "$(dirname "$0")/../.."
LOG=data/audit/full_rebuild.log
: > $LOG

# The window this run is built for, from pipeline/config.py (which refuses an
# inconsistent one at import). Keyness, collocations and clusters read the TEXT
# window; the export plots each source through its own series end. Logged so a
# log can be read against the window it was built for, and with a start time:
# the 2026-09-21 log began mid-keyness, so that rebuild's duration could only be
# bounded (>= 2h34m48s), never measured.
/opt/anaconda3/bin/python -c "
import pipeline.config as c
print(f'WINDOW series {c.STUDY_START}..{c.SERIES_END}  text ..{c.TEXT_END}  '
      f'headline {c.HEADLINE_YEAR}  sources ' + ' '.join(f'{k}={c.series_end(k)}' for k in sorted(c.SOURCE_SERIES_END)))
" >> $LOG 2>&1 || { echo "pipeline.config rejected the study window; see $LOG"; exit 1; }
echo "KEYNESS START $(date +%Y-%m-%dT%H:%M:%S)" >> $LOG

# --all means ENABLED pairs; retired ones (borscht) are skipped by design.
/opt/anaconda3/bin/python -m pipeline.stats.analyze_pair --all >> $LOG 2>&1
echo "KEYNESS DONE $(date +%H:%M:%S)" >> $LOG

PAIRS=$(/opt/anaconda3/bin/python -c "
import yaml; c=yaml.safe_load(open('config/pairs.yaml'))
print(' '.join(p['slug'] for p in c['pairs'] if p.get('enabled',True)))")
for p in ${=PAIRS}; do
  echo "=== clusters $p $(date +%H:%M:%S)" >> $LOG
  /opt/anaconda3/bin/python -m pipeline.stats.clusters --pair $p >> $LOG 2>&1
done
echo "CLUSTERS DONE $(date +%H:%M:%S)" >> $LOG

/opt/anaconda3/bin/python -m pipeline.export_site_data >> $LOG 2>&1
echo "EXPORT DONE $(date +%H:%M:%S)" >> $LOG

# Every gate exits non-zero on a gap. Run them all so the log shows the whole
# picture, then carry the worst status out: the exit code was previously thrown
# away, so a chain that ended with an unglossed chip or an amber pair still
# looked successful to anything reading $?.
GATE_FAIL=0
# gdelt_paths_reconcile fails when the verified GDELT series has a month inside
# series_end('gdelt') that its own corpus does not back, or a trailing partial
# month: 22 pairs' verified corpus stops at 2026-08-26, which the chart would
# draw as a collapse once GDELT's end reaches 2026-08.
for gate in green_check holdout_convergence gloss_coverage site_invariants gdelt_paths_reconcile; do
  /opt/anaconda3/bin/python -m pipeline.audit.$gate >> $LOG 2>&1 || {
    GATE_FAIL=1
    echo "GATE FAILED: $gate" >> $LOG
  }
done
echo "ALL DONE $(date +%H:%M:%S)" >> $LOG

if [ $GATE_FAIL -ne 0 ]; then
  echo "FAILED — a gate reported a gap; grep 'GATE FAILED' $LOG"
  exit 1
fi
echo "done — all gates pass"
