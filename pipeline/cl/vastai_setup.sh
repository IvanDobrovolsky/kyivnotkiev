#!/bin/bash
# KyivNotKiev CL Pipeline — vast.ai B200 setup
#
# Usage:
#   scp vastai_bundle.tar.gz root@<VAST_IP>:/workspace/
#   ssh root@<VAST_IP>
#   cd /workspace && tar xzf vastai_bundle.tar.gz && bash setup.sh

set -e

echo "============================================"
echo "KyivNotKiev CL Pipeline — GPU Setup"
echo "============================================"

# 1. System deps
echo "[1/5] System dependencies..."
apt-get update -qq && apt-get install -y -qq python3-pip git > /dev/null 2>&1

# 2. Install all ML dependencies
echo "[2/5] Installing vLLM + transformers + training stack..."
pip install -q \
    vllm \
    openai \
    transformers \
    datasets \
    torch \
    sentence-transformers \
    scikit-learn \
    accelerate \
    pandas \
    pyarrow \
    numpy

# 3. Verify GPU
echo "[3/5] GPU check..."
python3 -c "
import torch
if not torch.cuda.is_available():
    print('ERROR: No GPU detected!')
    exit(1)
for i in range(torch.cuda.device_count()):
    props = torch.cuda.get_device_properties(i)
    print(f'  GPU {i}: {props.name}, {props.total_mem / 1e9:.1f} GB VRAM')
total_vram = sum(torch.cuda.get_device_properties(i).total_mem for i in range(torch.cuda.device_count()))
print(f'  Total VRAM: {total_vram / 1e9:.1f} GB')
n_gpus = torch.cuda.device_count()

# Recommend config
if total_vram > 150e9:
    print(f'  → Llama 70B FP16 (full precision, {n_gpus} GPU)')
elif total_vram > 80e9:
    print(f'  → Llama 70B FP8 (near-lossless, {n_gpus} GPU)')
else:
    print(f'  → Llama 70B AWQ 4-bit ({n_gpus} GPU)')
"

# 4. Check dataset
echo "[4/5] Checking dataset..."
if [ -f data/corpus.parquet ]; then
    python3 -c "
import pandas as pd
df = pd.read_parquet('data/corpus.parquet')
print(f'  Corpus: {len(df)} texts, {df[\"pair_id\"].nunique()} pairs')
print(f'  Variants: {df[\"variant\"].value_counts().to_dict()}')
print(f'  Sources: {df[\"source\"].value_counts().to_dict()}')
"
else
    echo "  ERROR: data/corpus.parquet not found!"
    exit 1
fi

# 5. Detect VRAM and pick model
echo "[5/5] Ready!"
echo ""

TOTAL_VRAM=$(python3 -c "
import torch
total = sum(torch.cuda.get_device_properties(i).total_mem for i in range(torch.cuda.device_count()))
print(int(total / 1e9))
")
N_GPUS=$(python3 -c "import torch; print(torch.cuda.device_count())")

if [ "$TOTAL_VRAM" -gt 150 ]; then
    MODEL="meta-llama/Llama-3.1-70B-Instruct"
    QUANT=""
    echo "Config: FP16 (full precision) on ${N_GPUS} GPU(s)"
elif [ "$TOTAL_VRAM" -gt 80 ]; then
    MODEL="neuralmagic/Meta-Llama-3.1-70B-Instruct-FP8"
    QUANT=""
    echo "Config: FP8 on ${N_GPUS} GPU(s)"
else
    MODEL="TechxGenus/Meta-Llama-3.1-70B-Instruct-AWQ"
    QUANT="--quantization awq"
    echo "Config: AWQ 4-bit on ${N_GPUS} GPU(s)"
fi

echo ""
echo "Step 1 — Start vLLM server (run in tmux or screen):"
echo ""
echo "  python3 -m vllm.entrypoints.openai.api_server \\"
echo "    --model $MODEL \\"
echo "    $QUANT \\"
echo "    --max-model-len 4096 \\"
echo "    --port 8000 \\"
echo "    --tensor-parallel-size $N_GPUS \\"
echo "    --gpu-memory-utilization 0.92"
echo ""
echo "Step 2 — Run annotation (in another terminal):"
echo ""
echo "  python3 annotate.py"
echo ""
echo "Step 3 — Download results:"
echo ""
echo "  scp root@<THIS_IP>:/workspace/data/corpus_labeled.parquet ."
echo ""
