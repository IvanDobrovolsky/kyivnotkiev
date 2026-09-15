#!/bin/zsh
# Full statistical rebuild: keyness -> clusters -> export -> audits.
#
# Run this after anything that changes the store or changes keyness/clusters
# code. Takes ~3.5h for 24 pairs on this machine. Everything is sequential on
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

# Both audits exit non-zero on any gap, so the tail of the log is the verdict.
/opt/anaconda3/bin/python -m pipeline.audit.holdout_convergence >> $LOG 2>&1
/opt/anaconda3/bin/python -m pipeline.audit.gloss_coverage >> $LOG 2>&1
echo "ALL DONE $(date +%H:%M:%S)" >> $LOG

echo "done — new chips may need glosses; see the gloss_coverage output above"
