#!/usr/bin/env bash
set -euo pipefail
exec /home/pythonic/.local/share/stillyard/bin/stillyard --endpoint /home/pythonic/.local/share/stillyard/stillyard-v6.sock ensure --spec /home/pythonic/Development/stillyard-handoff-20260912/hello-python/job.json --idempotency-key 35d30d53-5c5c-4ecb-8249-c552ef037795 --result-file /home/pythonic/Development/stillyard-handoff-20260912/hello-python/receipt.json --wait --deadline-seconds 120
