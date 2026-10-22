# Running payment-system on kind

Run these commands from the repository root. The default cluster is
`payment-system` and the default kubectl context is `kind-payment-system`.

```sh
make docker-build
make kind-load
make k8s-deploy
```

`k8s-deploy` applies the namespace, local development credentials and
infrastructure, waits for dependencies, runs database migrations, and then
applies and restarts the five services. It waits for each rollout to finish.
It preserves an existing auth signing key, or generates an RSA key pair and
stores it directly in a Kubernetes Secret. Private keys never enter the repo.
The signing Secret is managed separately from `secrets.yaml` so reapplying
the development credentials cannot erase the keys.

Use `KIND_CLUSTER=my-cluster` for `kind-load`, and
`KUBE_CONTEXT=my-context` for `k8s-deploy` or `k8s-migrate` to change the defaults.
The checked-in database, Redis and NATS credentials are for local development.
On other clusters, provision appropriate credentials instead of using these
defaults, and publish the service images to a registry accessible to the nodes.

`kind-load` exports only `linux/amd64` before importing, avoiding missing
platform digests with Docker's containerd image store. This requires a Docker
client/server supporting `docker image save --platform` (API 1.48 or later).
For ARM nodes, build ARM images and set `IMAGE_PLATFORM=linux/arm64`.

## Database migrations

```sh
make k8s-migrate
```

This uploads each service's SQL files from `services/<service>/migrations` into
a ConfigMap and creates a separate Job for each database using
`migrate/migrate:v4.18.2`, the same tool as Docker Compose. Jobs use the database
URL from the corresponding Secret, wait for PostgreSQL, and run `up`. They
record applied versions in `schema_migrations`; running the command again is
safe and reports `no change` if there are no pending migrations.

Jobs get fresh names on every run, retry failures up to three times, have a
five-minute deadline, and are retained for one day for inspection. A failed
migration stops `k8s-deploy` before it updates the service Deployments.
Run only one migration command at a time. After adding migrations, use
`make k8s-deploy` to apply them before rolling out service changes.

The first run downloads the migration image if it is not cached on the nodes.
To preload it for kind:

```sh
docker pull migrate/migrate:v4.18.2
bash deploy/k8s/kind-load.sh migrate/migrate:v4.18.2
```

Inspect migrations and schema state:

```sh
kubectl --context kind-payment-system -n payment-system get jobs
kubectl --context kind-payment-system -n payment-system logs job/<job-name> -c migrate
kubectl --context kind-payment-system -n payment-system exec account-postgres-0 -- \
  psql -U account -d account -c 'TABLE schema_migrations;'
```

If a migration fails, inspect the Job logs and database state before retrying.
A dirty migration requires repairing the failed change before adjusting its
recorded version. Do not delete PVCs or run down migrations to recover startup.

## Checking and accessing the services

```sh
kubectl --context kind-payment-system -n payment-system get pods
kubectl --context kind-payment-system -n payment-system get endpointslices
kubectl --context kind-payment-system -n payment-system logs deployment/account --tail=30
kubectl --context kind-payment-system -n payment-system port-forward service/api-gateway 8080:8080
```

In another terminal:

```sh
curl http://localhost:8080/healthz
```

The four gRPC services implement the standard gRPC health protocol used by
their readiness and liveness probes. All service pods run under explicit
numeric non-root users. Account and payment accept both authenticated Redis
URLs and the `host:port` format used in Compose.

For further image changes, repeat `docker-build`, `kind-load`, and `k8s-deploy`:
loading a replacement `latest` image alone does not restart existing pods.
