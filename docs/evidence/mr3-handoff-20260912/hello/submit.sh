#!/usr/bin/env bash
set -euo pipefail
exec /home/pythonic/.local/share/stillyard/bin/stillyard --endpoint /home/pythonic/.local/share/stillyard/stillyard-v6.sock ensure --spec /home/pythonic/Development/stillyard-handoff-20260912/hello/job.json --idempotency-key 8e62ea01-cac3-40b8-8b91-09d14917d97b --result-file /home/pythonic/Development/stillyard-handoff-20260912/hello/receipt.json --wait --deadline-seconds 120
