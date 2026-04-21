# OpenMeter Self-Hosted — Meter Engine Design

Date: 2026-04-16
Status: Approved

## Context

The metering stack on `nerc-ocp-test` has a raw event pipeline:

```
Kubernetes API → OTel Collector → Kafka (billing-events) → Stream Processor → ClickHouse (billing_events)
                                                          → Reconciler CronJob (synthetic events)
```

The V1 architecture diagram requires a **Meter engine** between Kafka and the billing DB that handles usage aggregation and provides a pricing interface. This spec adds OpenMeter self-hosted as that component.

## Goal

Deploy OpenMeter OSS on `nerc-ocp-test` as a dedicated meter engine that:
- Reads `billing-events` from Kafka independently of the stream processor
- Maps billing event reasons to usage meter subjects (namespace + resource_type)
- Computes START/STOP session durations and usage windows
- Writes aggregated usage to ClickHouse `billing.billing_sessions` and `billing.namespace_usage_daily`
- Exposes an internal API (port 8080) for future pricing interface integration

## Architecture

```
Kafka (billing-events)
  ├── metering-stream-processor (existing)  →  ClickHouse billing.billing_events   [raw archive]
  └── metering-openmeter (new)              →  ClickHouse billing.billing_sessions  [aggregated usage]
                                                         billing.namespace_usage_daily
```

The stream processor keeps its current role as raw event archiver. OpenMeter is a second independent Kafka consumer on the same topic using its own consumer group.

## New Resources

| Resource | Kind | Purpose |
|---|---|---|
| `metering-openmeter` | Deployment | OpenMeter OSS container, reads Kafka, writes ClickHouse |
| `openmeter-config` | ConfigMap | OpenMeter config file (Kafka source, ClickHouse sink, meter definitions) |
| `metering-openmeter` | Service | ClusterIP port 8080, future pricing API access |
| `metering-openmeter` | KafkaUser | SCRAM-SHA-512, Read on `billing-events`, own consumer group |

No new operators, databases, or external dependencies. Reuses existing Kafka and ClickHouse.

## Meter Definitions

OpenMeter meters map Kafka event fields to usage subjects:

| Meter slug | Event field | Values captured |
|---|---|---|
| `pod-running` | `billing_action` = `START_RUNNING` / `STOP` | namespace + resource_type = `pod` |
| `storage` | `billing_action` = `START_STORAGE` / `STOP` | namespace + resource_type = `pvc` |
| `pod-error` | `billing_action` = `STOP_ERROR` | namespace, error events |

Subject key: `{nerc_tenant}/{namespace}/{pod_name}` — uniquely identifies a billable resource instance.

## Event→Action Mapping (recap)

| K8s event reason | billing_action |
|---|---|
| `Started` | `START_RUNNING` |
| `Killing` | `STOP` |
| `Failed` / `OOMKilling` | `STOP_ERROR` |
| `ProvisioningSucceeded` | `START_STORAGE` |

## ClickHouse Sink

OpenMeter writes to existing tables — no schema changes needed:
- `billing.billing_sessions` — one row per completed START/STOP pair (duration_seconds computed)
- `billing.namespace_usage_daily` — refreshed via existing materialized view on `billing_sessions`

## KafkaUser ACLs

```
metering-openmeter:
  topic billing-events: Read, Describe
  consumer group metering-openmeter: Read, Describe
```

## Out of Scope

- Pricing rate tables (future)
- OpenMeter UI / external API exposure
- OpenMeter Postgres/Redis backend (not needed for OSS Kafka+ClickHouse mode)
- Production cluster rollout
