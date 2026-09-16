#!/usr/bin/env bash
# Build images, create a kind cluster, deploy everything, run the load test inside the cluster.
# Usage: scripts/kind-up.sh            (add --no-build to reuse images already in Docker)
# Cleanup: kind delete cluster --name order-tracking
set -euo pipefail
cd "$(dirname "$0")/.."
cluster=order-tracking
apps=(order-service payment-service inventory-service notification-service order-analytics)

if [[ "${1:-}" != "--no-build" ]]; then
  echo "== building images with Paketo buildpacks"
  mvn -B -q -DskipTests package spring-boot:build-image
fi

if ! kind get clusters 2>/dev/null | grep -qx "$cluster"; then
  echo "== creating kind cluster $cluster"
  kind create cluster --name "$cluster" --wait 120s
fi
kubectl config use-context "kind-$cluster" >/dev/null

echo "== loading images into the cluster"
for app in "${apps[@]}"; do
  kind load docker-image "order-tracking/$app:0.1.0-SNAPSHOT" --name "$cluster" >/dev/null
done

echo "== applying manifests"
kubectl apply -k k8s/base
kubectl -n order-tracking rollout status statefulset/kafka --timeout=180s
kubectl -n order-tracking rollout status statefulset/postgres --timeout=120s
kubectl -n order-tracking rollout status deployment/schema-registry --timeout=180s
for app in "${apps[@]}"; do
  kubectl -n order-tracking rollout status "deployment/$app" --timeout=300s
done
kubectl -n order-tracking get pods

echo "== port-forwarding order-service (8080) and order-analytics (8084)"
kubectl -n order-tracking port-forward svc/order-service 8080:8080 >/dev/null 2>&1 &
pf1=$!
kubectl -n order-tracking port-forward svc/order-analytics 8084:8084 >/dev/null 2>&1 &
pf2=$!
trap 'kill $pf1 $pf2 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do curl -sf localhost:8080/actuator/health >/dev/null && break; sleep 1; done

echo "== load test through the cluster"
scripts/load.sh
sleep 3
echo "== analytics"
curl -s 'localhost:8084/analytics/orders-per-minute?last=5'; echo
curl -s localhost:8084/analytics/customers/customer-0/revenue; echo
