# Chapter 3: Kubernetes Walkthrough

Companion Kubernetes deployment for *Tracing Infrastructure in Action* (Manning, 2026) by Amrit Sarkar.

The docker-compose stacks in the [main README](../README.md) are enough to run every collector example. This guide covers the Kubernetes path: a self-contained, single-command cluster that mirrors the production agent/gateway topology end to end.

## What gets deployed

`scripts/setup-kind.sh` stands up a complete trace pipeline on a local Kind cluster. Nothing external is required beyond `kind`, `kubectl`, and `docker`. It creates a `tracing-ch3` cluster (one control-plane, two workers), builds and loads the `checkout-service:latest` image, then applies the manifests below.

The stack spans three namespaces:

| Namespace       | Workloads                                                  |
|-----------------|-----------------------------------------------------------|
| `kafka`         | Single-broker Kafka (KRaft) StatefulSet + headless service |
| `observability` | Agent DaemonSet, gateway Deployment, consumer collector, Jaeger |
| `default`       | `checkout-service` sample app                              |

## End-to-end flow

```
checkout-service ──→ otel-agent ──→ otel-gateway ──→ Kafka (otlp_spans) ──→ otel-consumer ──→ jaeger
   (default)          (DaemonSet)    (Deployment)        (kafka ns)            (drain)        (UI)
```

The teaching point from the chapter is preserved: the gateway exports to Kafka, exactly as it would in production, so spans for one trace land on the same partition. The in-cluster broker (`apache/kafka:4.3.0`) answers at `kafka-0.kafka-headless.kafka.svc:9092`, the DNS name the gateway config targets. The `otel-consumer` collector is the local drain: it reads the `otlp_spans` topic and forwards to Jaeger over OTLP, which is what makes the traces visible during the demo.

## Manifests

All manifests live in `k8s/`:

| File                         | Listing | Resource                                   |
|------------------------------|---------|--------------------------------------------|
| `kafka.yaml`                 | --      | `kafka` namespace + single-broker Kafka StatefulSet |
| `agent-daemonset.yaml`       | 3.9     | DaemonSet with hostPort                    |
| `agent-configmap.yaml`       | 3.10    | Agent config with node enrichment          |
| `gateway-deployment.yaml`    | 3.11    | Deployment + headless service              |
| `gateway-configmap.yaml`     | 3.12    | Gateway config with Kafka export           |
| `consumer-deployment.yaml`   | --      | Consumer collector draining `otlp_spans`   |
| `consumer-configmap.yaml`    | --      | Consumer config (Kafka receiver to Jaeger) |
| `jaeger.yaml`                | --      | Jaeger all-in-one + service                |
| `hpa.yaml`                   | 3.13    | Asymmetric scale-up/scale-down             |
| `network-policies.yaml`      | 3.14    | Agent and gateway network rules            |
| `prometheus-rules.yaml`      | 3.15    | Alerts for drops, failures, memory         |
| `pdb.yaml`                   | --      | Pod disruption budget                      |
| `service-monitor.yaml`       | --      | Prometheus ServiceMonitor                  |
| `sample-app-deployment.yaml` | --      | App with node IP discovery                 |

The collector image is pinned at `otel/opentelemetry-collector-contrib:0.154.0` across the agent, gateway, and consumer.

## Deploy

```bash
chmod +x scripts/setup-kind.sh
./scripts/setup-kind.sh
```

The script waits for Kafka, Jaeger, the consumer, the agent DaemonSet, the gateway, and the sample app to roll out before it returns.

## Generate traffic

Port-forward the sample app and run the load generator:

```bash
kubectl port-forward -n default svc/checkout-service 8080:8080
python3 scripts/load-generator.py --scenario steady --duration 60
```

Scenarios: `steady`, `spike`, `backpressure`, `multi-tenant`, `hot-trace`, `failover`.

## View traces

Port-forward Jaeger and open the UI:

```bash
kubectl port-forward -n observability svc/jaeger 16686:16686
```

Then open http://localhost:16686 and search for the `checkout-service` traces. Each trace's spans flow through the agent, the gateway, Kafka, and the consumer before they appear here.

To watch collector metrics, port-forward the gateway:

```bash
kubectl port-forward -n observability svc/otel-gateway 8888:8888
```

## DNS resolution for the gateway

The agent reaches gateways through the headless Service, so every export depends
on cluster DNS. Two things are worth knowing when you deploy this for real.

The load balancing exporter re-resolves on `resolver.dns.interval`, so a new
gateway pod is not routed to until the next resolution. Lower the interval if you
scale the gateway tier often.

Kubernetes sets `ndots:5` in a pod's `/etc/resolv.conf` by default, so a name with
fewer than five dots is tried against each search domain before it is tried as
written. For a fully qualified target like
`otel-gateway-headless.observability.svc.cluster.local` that means several wasted
NXDOMAIN lookups per resolution. If DNS load matters at your span rate, set
`dnsConfig.options` with `ndots: 1` on the agent pod spec, or keep the trailing
dot on the name.

## Tear down

```bash
./scripts/teardown.sh
```

Removes the docker-compose stacks (with their volumes) and deletes the `tracing-ch3` Kind cluster.
