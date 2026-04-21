# OpenMeter Self-Hosted — Meter Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add OpenMeter self-hosted as the meter engine between Kafka and ClickHouse, reading `billing-events` independently of the stream processor and writing aggregated sessions to ClickHouse.

**Architecture:** OpenMeter runs as a second Kafka consumer on `billing-events` with its own consumer group `metering-openmeter`. It maps billing_action values to meter subjects and writes completed START/STOP pairs as rows in `billing.billing_sessions`. The stream processor continues writing raw events to `billing.billing_events` unchanged.

**Tech Stack:** Strimzi KafkaUser, OpenMeter OSS (`ghcr.io/openmeterio/openmeter`), ClickHouse (existing), Kustomize base+overlay.

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Create | `metering/base/openmeter-config.yaml` | ConfigMap with OpenMeter config.yaml (Kafka source, ClickHouse sink, meter definitions) |
| Create | `metering/base/openmeter.yaml` | Deployment + Service for OpenMeter |
| Modify | `metering/base/kafkausers.yaml` | Add `metering-openmeter` KafkaUser |
| Modify | `metering/base/kustomization.yaml` | Add `openmeter-config.yaml` and `openmeter.yaml` to resources |

No overlay changes needed — `metering/overlays/nerc-ocp-test/kustomization.yaml` already references `../../base`.

---

## Task 1: Add KafkaUser for OpenMeter

**Files:**
- Modify: `metering/base/kafkausers.yaml`

- [ ] **Step 1: Append the KafkaUser to `metering/base/kafkausers.yaml`**

Add this block at the end of the file (after the last `---` separator):

```yaml
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: metering-openmeter
  namespace: metering-thor
  labels:
    strimzi.io/cluster: metering
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
    - resource:
        type: topic
        name: billing-events
        patternType: literal
      operations: [Read, Describe]
    - resource:
        type: group
        name: metering-openmeter
        patternType: literal
      operations: [Read, Describe]
```

- [ ] **Step 2: Validate kustomize build still works**

Run:
```bash
kustomize build metering/overlays/nerc-ocp-test
```
Expected: no errors, output includes `kind: KafkaUser` with `name: metering-openmeter`.

- [ ] **Step 3: Commit**

```bash
git add metering/base/kafkausers.yaml
git commit -m "feat(metering): add KafkaUser metering-openmeter for OpenMeter consumer group"
```

---

## Task 2: Create OpenMeter ConfigMap

**Files:**
- Create: `metering/base/openmeter-config.yaml`

OpenMeter is configured via a YAML config file mounted at `/etc/openmeter/config.yaml`. The config defines:
- Kafka ingest source (reads `billing-events`)
- ClickHouse sink (writes to `billing.billing_sessions`)
- Three meter definitions mapping `billing_action` field values to usage meters

- [ ] **Step 1: Create `metering/base/openmeter-config.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: openmeter-config
  namespace: metering-thor
data:
  config.yaml: |
    address: 0.0.0.0:8080

    ingest:
      kafka:
        brokers:
          - metering-kafka-bootstrap.metering-thor.svc:9092
        securityProtocol: SASL_PLAINTEXT
        saslMechanisms: SCRAM-SHA-512
        saslUsername: metering-openmeter
        # saslPassword injected via KAFKA_SASL_PASSWORD env var
        topics:
          - billing-events
        consumerGroupId: metering-openmeter

    sink:
      clickhouse:
        address: chi-metering-metering-0-0.metering-thor.svc:9000
        database: billing
        username: default
        # password injected via CLICKHOUSE_PASSWORD env var

    meters:
      - slug: pod-running
        description: "Pod running time (START_RUNNING / STOP pairs)"
        eventType: billing-event
        valueProperty: "$.duration_seconds"
        groupBy:
          nerc_tenant: "$.nerc_tenant"
          namespace: "$.namespace"
          pod_name: "$.pod_name"
          resource_type: "$.resource_type"
        aggregation: SUM
        windowSize: HOUR

      - slug: storage
        description: "PVC attached time (START_STORAGE / STOP pairs)"
        eventType: billing-event
        valueProperty: "$.duration_seconds"
        groupBy:
          nerc_tenant: "$.nerc_tenant"
          namespace: "$.namespace"
          pod_name: "$.pod_name"
          resource_type: "$.resource_type"
        aggregation: SUM
        windowSize: HOUR

      - slug: pod-error
        description: "Pod error/OOM events (STOP_ERROR)"
        eventType: billing-event
        valueProperty: "$.duration_seconds"
        groupBy:
          nerc_tenant: "$.nerc_tenant"
          namespace: "$.namespace"
          pod_name: "$.pod_name"
        aggregation: COUNT
        windowSize: HOUR
```

- [ ] **Step 2: Add `openmeter-config.yaml` to `metering/base/kustomization.yaml`**

In `metering/base/kustomization.yaml`, add `- openmeter-config.yaml` to the resources list, after `clickhouse-schema-job.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: metering-thor
commonLabels:
  app.kubernetes.io/part-of: metering

resources:
- rbac.yaml
- kafka.yaml
- kafkatopics.yaml
- kafkausers.yaml
- clickhouse.yaml
- clickhouse-schema-configmap.yaml
- clickhouse-schema-job.yaml
- openmeter-config.yaml
- stream-processor.yaml
- reconciler-cronjob.yaml
```

- [ ] **Step 3: Validate**

```bash
kustomize build metering/overlays/nerc-ocp-test
```
Expected: no errors, output includes `kind: ConfigMap` with `name: openmeter-config`.

- [ ] **Step 4: Commit**

```bash
git add metering/base/openmeter-config.yaml metering/base/kustomization.yaml
git commit -m "feat(metering): add OpenMeter config ConfigMap with Kafka source and meter definitions"
```

---

## Task 3: Create OpenMeter Deployment and Service

**Files:**
- Create: `metering/base/openmeter.yaml`
- Modify: `metering/base/kustomization.yaml`

OpenMeter reads its config from the mounted ConfigMap. SCRAM password and ClickHouse password are injected as env vars (OpenMeter OSS supports `KAFKA_SASL_PASSWORD` and `CLICKHOUSE_PASSWORD` env var overrides for secrets).

- [ ] **Step 1: Create `metering/base/openmeter.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: metering-openmeter
  namespace: metering-thor
spec:
  replicas: 1
  selector:
    matchLabels:
      app: metering-openmeter
  template:
    metadata:
      labels:
        app: metering-openmeter
    spec:
      containers:
      - name: openmeter
        image: ghcr.io/openmeterio/openmeter:latest
        args:
          - --config
          - /etc/openmeter/config.yaml
        env:
        - name: KAFKA_SASL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: metering-openmeter
              key: password
        - name: CLICKHOUSE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: clickhouse-billing-credentials
              key: password
        ports:
        - name: api
          containerPort: 8080
          protocol: TCP
        volumeMounts:
        - name: config
          mountPath: /etc/openmeter
          readOnly: true
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 512Mi
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
      volumes:
      - name: config
        configMap:
          name: openmeter-config
---
apiVersion: v1
kind: Service
metadata:
  name: metering-openmeter
  namespace: metering-thor
spec:
  selector:
    app: metering-openmeter
  ports:
  - name: api
    port: 8080
    targetPort: 8080
    protocol: TCP
  type: ClusterIP
```

- [ ] **Step 2: Add `openmeter.yaml` to `metering/base/kustomization.yaml`**

Add `- openmeter.yaml` after `openmeter-config.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: metering-thor
commonLabels:
  app.kubernetes.io/part-of: metering

resources:
- rbac.yaml
- kafka.yaml
- kafkatopics.yaml
- kafkausers.yaml
- clickhouse.yaml
- clickhouse-schema-configmap.yaml
- clickhouse-schema-job.yaml
- openmeter-config.yaml
- openmeter.yaml
- stream-processor.yaml
- reconciler-cronjob.yaml
```

- [ ] **Step 3: Validate**

```bash
kustomize build metering/overlays/nerc-ocp-test
```
Expected: no errors, output includes `kind: Deployment` with `name: metering-openmeter` and `kind: Service` with `name: metering-openmeter`.

- [ ] **Step 4: Run full CI validation**

```bash
./ci/validate-manifests.sh
```
Expected: all overlays pass with no errors.

- [ ] **Step 5: Commit**

```bash
git add metering/base/openmeter.yaml metering/base/kustomization.yaml
git commit -m "feat(metering): add OpenMeter Deployment and Service as meter engine"
```

---

## Task 4: Commit spec and plan docs

**Files:**
- `docs/superpowers/specs/2026-04-16-openmeter-design.md` (already written)
- `docs/superpowers/plans/2026-04-16-openmeter.md` (this file)

- [ ] **Step 1: Stage and commit docs**

```bash
git add docs/superpowers/specs/2026-04-16-openmeter-design.md \
        docs/superpowers/plans/2026-04-16-openmeter.md
git commit -m "docs: add OpenMeter meter engine design spec and implementation plan"
```
