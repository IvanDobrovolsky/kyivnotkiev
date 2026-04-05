#!/bin/bash
# Creates vastai_bundle.tar.gz for uploading to vast.ai
#
# Usage: bash pipeline/cl/create_bundle.sh

set -e
cd "$(dirname "$0")/../.."

BUNDLE_DIR=$(mktemp -d)
echo "Creating bundle in $BUNDLE_DIR..."

# Dataset
mkdir -p "$BUNDLE_DIR/data"
cp data/cl/balanced/corpus.parquet "$BUNDLE_DIR/data/corpus.parquet"

# Scripts
cp pipeline/cl/vastai_setup.sh "$BUNDLE_DIR/setup.sh"
cp pipeline/cl/vastai_annotate.py "$BUNDLE_DIR/annotate.py"

# Verify
echo "Bundle contents:"
ls -lh "$BUNDLE_DIR/"
ls -lh "$BUNDLE_DIR/data/"

# Check corpus size
python3 -c "
import pandas as pd
df = pd.read_parquet('$BUNDLE_DIR/data/corpus.parquet')
print(f'Corpus: {len(df)} texts, {df[\"pair_id\"].nunique()} pairs')
print(f'Variants: {df[\"variant\"].value_counts().to_dict()}')
"

# Create archive
tar czf vastai_bundle.tar.gz -C "$BUNDLE_DIR" .
BUNDLE_SIZE=$(ls -lh vastai_bundle.tar.gz | awk '{print $5}')
echo ""
echo "Created: vastai_bundle.tar.gz ($BUNDLE_SIZE)"
echo ""
echo "Upload to vast.ai:"
echo "  scp vastai_bundle.tar.gz root@<VAST_IP>:/workspace/"
echo "  ssh root@<VAST_IP>"
echo "  cd /workspace && tar xzf vastai_bundle.tar.gz && bash setup.sh"

# Cleanup
rm -rf "$BUNDLE_DIR"
