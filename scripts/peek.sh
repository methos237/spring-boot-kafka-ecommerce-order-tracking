#!/usr/bin/env bash
# Print the last N records (default 10) of a topic from the compose broker.
# Event topics are Avro and decoded through the schema registry; dead letter topics are raw bytes.
# Usage: scripts/peek.sh <topic> [n]
set -euo pipefail
topic="${1:?usage: peek.sh <topic> [n]}"
n="${2:-10}"
if [[ "$topic" == *.DLT ]]; then
  docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic "$topic" --from-beginning --max-messages "$n" --timeout-ms 5000 \
    --formatter-property print.key=true --formatter-property key.separator=' => ' 2>/dev/null || true
else
  # keys are plain strings, only values are Avro; tool logging goes to stdout, so keep only record lines
  docker exec schema-registry kafka-avro-console-consumer \
    --bootstrap-server kafka:29092 --topic "$topic" --from-beginning --max-messages "$n" --timeout-ms 5000 \
    --property schema.registry.url=http://localhost:8081 \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property print.key=true --property key.separator=' => ' 2>/dev/null | grep -F ' => ' || true
fi
