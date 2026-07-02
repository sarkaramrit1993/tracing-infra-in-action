#!/usr/bin/env bash
# Sets up a local Kind cluster and deploys Chapter 3 collector infrastructure.
# Requires: kind, kubectl, docker
set -euo pipefail

CLUSTER_NAME="tracing-ch3"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
K8S_DIR="$SCRIPT_DIR/../k8s"
APP_DIR="$SCRIPT_DIR/../app"

echo "=== Chapter 3: Trace Collection and Routing ==="
echo ""

# Create Kind cluster
if kind get clusters 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    echo "Cluster '$CLUSTER_NAME' already exists"
else
    echo "Creating Kind cluster '$CLUSTER_NAME'..."
    kind create cluster --name "$CLUSTER_NAME" --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
EOF
fi

echo ""
echo "=== Building application image ==="
docker build -t checkout-service:latest "$APP_DIR"
kind load docker-image checkout-service:latest --name "$CLUSTER_NAME"

echo ""
echo "=== Deploying Kubernetes manifests ==="

# Namespaces first (observability + kafka). kafka.yaml carries the kafka namespace.
kubectl apply -f "$K8S_DIR/namespace.yaml"
kubectl apply -f "$K8S_DIR/kafka.yaml"

# Create service account for agent
kubectl create serviceaccount otel-agent -n observability --dry-run=client -o yaml | kubectl apply -f -

# Backend: Jaeger + Kafka consumer (drains otlp_spans into Jaeger)
kubectl apply -f "$K8S_DIR/jaeger.yaml"
kubectl apply -f "$K8S_DIR/consumer-configmap.yaml"
kubectl apply -f "$K8S_DIR/consumer-deployment.yaml"

# Collector infrastructure
kubectl apply -f "$K8S_DIR/agent-configmap.yaml"
kubectl apply -f "$K8S_DIR/agent-daemonset.yaml"
kubectl apply -f "$K8S_DIR/gateway-configmap.yaml"
kubectl apply -f "$K8S_DIR/gateway-deployment.yaml"
kubectl apply -f "$K8S_DIR/hpa.yaml"
kubectl apply -f "$K8S_DIR/pdb.yaml"
kubectl apply -f "$K8S_DIR/network-policies.yaml"

# Sample app
kubectl apply -f "$K8S_DIR/sample-app-deployment.yaml"

echo ""
echo "=== Waiting for rollout ==="
kubectl rollout status statefulset/kafka -n kafka --timeout=180s
kubectl rollout status deployment/jaeger -n observability --timeout=120s
kubectl rollout status deployment/otel-consumer -n observability --timeout=120s
kubectl rollout status daemonset/otel-agent -n observability --timeout=120s
kubectl rollout status deployment/otel-gateway -n observability --timeout=120s
kubectl rollout status deployment/checkout-service -n default --timeout=120s

echo ""
echo "=== Deployment complete ==="
echo ""
echo "Port forwards (run in separate terminals):"
echo "  kubectl port-forward -n default svc/checkout-service 8080:8080"
echo "  kubectl port-forward -n observability svc/otel-gateway 8888:8888"
echo "  kubectl port-forward -n observability svc/jaeger 16686:16686"
echo ""
echo "Then run load generator:"
echo "  python scripts/load-generator.py --scenario steady --duration 60"
