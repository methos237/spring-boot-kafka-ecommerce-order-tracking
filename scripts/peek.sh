#!/usr/bin/env bash
# Print the last N records (default 10) of a topic from the compose Kafka broker.
# Usage: scripts/peek.sh <topic> [n]
set -euo pipefail
topic="${1:?usage: peek.sh <topic> [n]}"
n="${2:-10}"
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic "$topic" \
  --from-beginning \
  --max-messages "$n" \
  --timeout-ms 5000 \
  --formatter-property print.key=true \
  --formatter-property key.separator=' => ' 2>/dev/null || true
