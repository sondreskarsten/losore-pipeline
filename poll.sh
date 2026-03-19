#!/bin/bash
set -euo pipefail

BUCKET="${GCS_BUCKET:-sondre_brreg_data}"
PREFIX="${GCS_PREFIX:-losore/agder}"

while true; do
    echo "--- $(date '+%H:%M:%S') ---"
    gsutil cat "gs://${BUCKET}/${PREFIX}/status.json" 2>/dev/null | python3 -m json.tool || echo "No status yet"
    
    PHASE=$(gsutil cat "gs://${BUCKET}/${PREFIX}/status.json" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('phase',''))" 2>/dev/null || echo "")
    if [ "$PHASE" = "done" ] || [ "$PHASE" = "error" ]; then
        echo ""
        echo "Job finished with phase: ${PHASE}"
        break
    fi
    
    sleep 30
done
