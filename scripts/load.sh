#!/usr/bin/env bash
# Place 50 orders with a deterministic mix, then report how many settled as CONFIRMED / CANCELLED.
#   1-30  valid       SKU-1, priced under the limit      -> CONFIRMED
#  31-40  over limit  SKU-1, priced 1500.00              -> CANCELLED (payment)
#  41-50  no stock    SKU-3, priced 9.99                 -> CANCELLED (inventory, refund)
# Ten distinct customers so the partitioner spreads keys. Usage: scripts/load.sh [base-url]
set -euo pipefail
base="${1:-http://localhost:8080}"
ids=()
for i in $(seq 1 50); do
  customer="customer-$((i % 10))"
  if   [ "$i" -le 30 ]; then body="{\"customerId\":\"$customer\",\"items\":[{\"sku\":\"SKU-1\",\"quantity\":1,\"unitPrice\":$((10 + i)).00}]}"
  elif [ "$i" -le 40 ]; then body="{\"customerId\":\"$customer\",\"items\":[{\"sku\":\"SKU-1\",\"quantity\":1,\"unitPrice\":1500.00}]}"
  else                       body="{\"customerId\":\"$customer\",\"items\":[{\"sku\":\"SKU-3\",\"quantity\":1,\"unitPrice\":9.99}]}"
  fi
  id=$(curl -sf -X POST "$base/api/orders" -H 'Content-Type: application/json' -d "$body" | sed -E 's/.*"id":"([^"]+)".*/\1/')
  ids+=("$id")
done
echo "placed ${#ids[@]} orders; expecting CONFIRMED=30 CANCELLED=20"

for attempt in $(seq 1 30); do
  confirmed=0; cancelled=0; pending=0
  for id in "${ids[@]}"; do
    case "$(curl -sf "$base/api/orders/$id" | sed -E 's/.*"status":"([A-Z]+)".*/\1/')" in
      CONFIRMED) confirmed=$((confirmed + 1)) ;;
      CANCELLED) cancelled=$((cancelled + 1)) ;;
      *)         pending=$((pending + 1)) ;;
    esac
  done
  [ "$pending" -eq 0 ] && break
  sleep 1
done
echo "settled:  CONFIRMED=$confirmed CANCELLED=$cancelled PENDING=$pending"
[ "$confirmed" -eq 30 ] && [ "$cancelled" -eq 20 ] && echo "OK" || { echo "MISMATCH"; exit 1; }
