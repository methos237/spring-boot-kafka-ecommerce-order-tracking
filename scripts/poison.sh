#!/usr/bin/env bash
# Produce one non-JSON record to a topic to exercise the dead-letter path.
# Usage: scripts/poison.sh <topic>
set -euo pipefail
topic="${1:?usage: poison.sh <topic>}"
printf 'poison|this is not json\n' | docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic "$topic" \
  --reader-property parse.key=true \
  --reader-property key.separator='|' 2>/dev/null
echo "poison record sent to $topic; watch $topic.DLT"
