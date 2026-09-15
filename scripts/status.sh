#!/usr/bin/env bash
# Consumer group lag per partition for every group on the compose broker.
set -euo pipefail
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null \
  | awk 'NR==1 || /^GROUP/ {next} NF {printf "%-24s %-22s p%-2s lag=%s\n", $1, $2, $3, $6}' | sort
