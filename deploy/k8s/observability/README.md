# Observability Kubernetes files

This directory defines the local Prometheus, Loki, Tempo, Alloy, and Grafana stack.
Use `make observability-deploy` from the repository root. The raw manifests contain
image/configuration placeholders; `scripts/observability-render` resolves them and
creates ConfigMaps before applying the workloads.

## Manifests

| File | Purpose |
| --- | --- |
| `namespace.yaml` | Creates the `observability` namespace and enforces Kubernetes' restricted Pod Security profile. |
| `rbac.yaml` | Creates component ServiceAccounts and the narrow discovery permissions used by Prometheus and Alloy. Discovery is limited to pods in `payment-system`; Alloy can additionally read their logs. Neither collector can read Secrets or execute commands. |
| `storage.yaml` | Creates persistent volume claims for Prometheus, Loki, Tempo, and Grafana. These preserve stored telemetry and Grafana state across pod restarts. Alloy uses temporary storage instead. |
| `network-policies.yaml` | Defines default-deny networking and the specific paths needed for DNS, Kubernetes discovery, application metrics/traces, and communication between the telemetry components. |
| `workloads.yaml` | Defines ClusterIP Services and Deployments for all five tools, including resource limits, non-root security settings, TLS mounts, authenticated health probes, storage mounts, and configuration hashes that trigger rollouts. |

Filenames deliberately have no numeric prefixes. Kubernetes does not require
numbers in filenames and does not infer resource readiness from them. The renderer
explicitly emits namespace → RBAC → storage → network policies → generated
ConfigMaps → Services/Deployments. The deploy script separately waits for readiness.
Renaming these files does not rename or recreate Kubernetes objects: those objects
are identified by their manifest kind, namespace, and `metadata.name`.

## Tool configuration

| File | Purpose |
| --- | --- |
| `config/prometheus.yml` | Configures scrape frequency, discovery of every application pod's metrics port, verified mTLS, bounded scrape sizes, and service/pod labels. This is how `go_goroutines` and the other application metrics reach Prometheus. |
| `config/prometheus-web.yml` | Protects Prometheus' own HTTP API with server TLS and required client-certificate authentication. It secures queries into Prometheus, whereas `prometheus.yml` secures scrapes out to applications. |
| `config/loki.yml` | Configures log ingestion/querying, local storage, seven-day retention, query/ingestion limits, backend mTLS, and private internal listeners. |
| `config/tempo.yml` | Configures trace ingestion/querying, local storage/WAL, three-day retention, bounded query sharding, ingestion limits, and mTLS. |
| `config/config.alloy` | Defines the collection pipelines: Kubernetes pod logs → Loki, and application OTLP traces → bounded processing/batching → Tempo. Its extension reflects Alloy's configuration language. |
| `config/grafana.ini` | Configures Grafana's HTTPS listener, login/security settings, mounted credential files, storage paths, and disabled anonymous access/self-registration/external analytics. |
| `config/datasources.yml` | Provisions Grafana's Prometheus, Loki, and Tempo connections with TLS credentials. Also creates log-to-trace links and trace-to-log queries. |
| `config/dashboards.yml` | Tells Grafana where provisioned dashboard JSON files live and which folder to place them in. It does not define individual charts. |
| `config/payment-system.json` | Defines the actual dashboard: panels, PromQL queries, units, legends, and service/pod filters. Includes goroutines, heap, GC, RSS, CPU, file descriptors, traffic, latency, retries, circuit breakers, outboxes, consumers, database pools, and cache activity. |

## Image inputs

| File | Purpose |
| --- | --- |
| `images.lock` | Pins the upstream tool images and Go builder to exact versions and immutable registry digests. Build and render derive the same image tag from these inputs. |
| `images/Dockerfile` | Builds the small Go health/storage helper and adds it to each pinned upstream image. Authenticated probes can therefore work without adding a shell or bypassing TLS verification. |
| `README.md` | This file guide. It is documentation, not a Kubernetes resource. |

## Related files elsewhere under `deploy/k8s`

- `services/account.yaml`, `services/api-gateway.yaml`, `services/auth.yaml`,
  `services/ledger.yaml`, and `services/payment.yaml` configure each application
  to expose its mTLS metrics port and export traces to Alloy. They add discovery
  labels, pod identity variables, and a read-only telemetry certificate mount.
- `deploy.sh` bootstraps the application telemetry Secret, applies database
  migrations before application rollout, and waits for service readiness.
- `migrations/jobs.yaml` defines the database migration Jobs. They also apply
  the additive outbox `trace_context` migrations, whose SQL lives under each
  service's `migrations/` directory.
- The parent `namespace.yaml` creates the application namespace, `payment-system`.
  This directory's `namespace.yaml` creates the separate telemetry namespace.
- The parent `secrets.yaml` contains the existing local payment-system dependency
  credentials. Observability certificates/passwords are generated separately by
  the bootstrap helper; they are not added to that manifest.
- The PostgreSQL/Redis/NATS manifests under `infrastructure/` run the payment
  system's dependencies. They are not the observability backends.

Certificate signing keys and generated login passwords are not stored in this
checkout. The bootstrap helper keeps them in the private local state directory
and distributes only the required workload credentials through Kubernetes Secrets.

See [the operating guide](../../../docs/observability.md) for access, example
queries, deployment, verification, and certificate renewal.
