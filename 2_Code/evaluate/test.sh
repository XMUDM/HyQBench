#!/bin/bash

# Configuration
GPU_DEVICES="0,1"           # GPU
TARGET="lotus"              # lotus, blendsql, cost
MODEL="DeepSeek-V3"         

export CUDA_VISIBLE_DEVICES="$GPU_DEVICES"

echo "GPU: $GPU_DEVICES"
echo "Model: $MODEL"

if [ "$TARGET" == "lotus" ]; then
    Script="testbed.py --target lotus"
elif [ "$TARGET" == "blendsql" ]; then
    Script="testbed.py --target blendsql"
elif [ "$TARGET" == "cost" ]; then
    Script="testbed.py --target cost" 
else
    echo "Unknown TARGET: $TARGET"
    exit 1
fi

CMD="python -u $Script --model $MODEL"

echo "==================================="
echo "Excute command: $CMD"
echo "==================================="

$CMD
