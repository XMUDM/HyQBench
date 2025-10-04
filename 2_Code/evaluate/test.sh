#!/bin/bash

# Settings
GPU_DEVICES="0,1"           # GPU
TARGET="lotus"              # lotus, blendsql, cost
MODEL="DeepSeek-V3"         

export CUDA_VISIBLE_DEVICES="$GPU_DEVICES"

echo "GPU: $GPU_DEVICES"
echo "Model: $MODEL"

if [ "$TARGET" == "lotus" ]; then
    Script="testbed.py --method lotus"
elif [ "$TARGET" == "blendsql" ]; then
    Script="testbed.py --method blendsql"
elif [ "$TARGET" == "cost" ]; then
    Script="testbed_cost.py"
else
    echo "Unknown TARGET: $TARGET"
    exit 1
fi

CMD="python -u $Script --model $MODEL"

echo "==================================="
echo "Excute command: $CMD"
echo "==================================="

$CMD
