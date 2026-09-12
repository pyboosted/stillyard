#!/usr/bin/env bash
set -euo pipefail
exec /home/pythonic/.local/share/stillyard/bin/stillyard --endpoint /home/pythonic/.local/share/stillyard/stillyard-v6.sock ensure --spec /home/pythonic/Development/stillyard-handoff-20260912/hello-final/job.json --idempotency-key 95d79a89-3772-4100-b78c-c1965b402cbe --result-file /home/pythonic/Development/stillyard-handoff-20260912/hello-final/receipt.json --wait --deadline-seconds 120
