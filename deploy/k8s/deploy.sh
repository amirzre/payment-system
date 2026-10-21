#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
KUBE_CONTEXT=${KUBE_CONTEXT:-kind-payment-system}
kube() { kubectl --context "$KUBE_CONTEXT" --namespace payment-system "$@"; }

keys() {
    # Never overwrite an existing signing key or write private material to the repo.
    local size
    size=$(kube get secret auth-signing-keys --ignore-not-found \
        -o go-template='{{len (index .data "PRIVATE_KEY_PEM")}}')
    if [[ -n "$size" && "$size" != 0 ]]; then
        echo "Keeping existing auth signing key."
        return
    fi
    local keydir
    keydir=$(mktemp -d)
    trap 'rm -rf "$keydir"' EXIT
    openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
        -out "$keydir/private.pem" 2>/dev/null
    openssl pkey -in "$keydir/private.pem" -pubout -out "$keydir/public.pem"
    kube create secret generic auth-signing-keys \
        --from-file=PRIVATE_KEY_PEM="$keydir/private.pem" \
        --from-file=PUBLIC_KEY_PEM="$keydir/public.pem" \
        --dry-run=client -o yaml | kube apply -f -
    rm -rf "$keydir"
    trap - EXIT
}

migrate() {
    for service in account auth ledger payment; do
        kube rollout status "statefulset/$service-postgres" --timeout=180s
        kube create configmap "$service-migrations" \
            --from-file="$ROOT/services/$service/migrations" \
            --dry-run=client -o yaml | kube apply -f -
    done
    local jobs
    jobs=$(kube create -f "$ROOT/deploy/k8s/migrations/jobs.yaml" -o name)
    for job in $jobs; do
        if ! kube wait --for=condition=complete "$job" --timeout=360s; then
            kube get pods -l "job-name=${job#job.batch/}" -o wide
            kube logs "$job" --all-containers=true || true
            echo "Migration failed: $job. Services were not rolled out." >&2
            return 1
        fi
        kube logs "$job" -c migrate
    done
}

case "${1:-deploy}" in
    deploy)
        kube apply -f "$ROOT/deploy/k8s/namespace.yaml"
        kube apply -f "$ROOT/deploy/k8s/secrets.yaml"
        keys
        kube apply -f "$ROOT/deploy/k8s/infrastructure/"
        # Service manifests require the dedicated telemetry leaf credentials.
        # Existing local trust roots/passwords are preserved by bootstrap.
        kubectl --context "$KUBE_CONTEXT" apply -f "$ROOT/deploy/k8s/observability/namespace.yaml"
        KUBE_CONTEXT="$KUBE_CONTEXT" "$ROOT/scripts/observability-bootstrap" \
            --state-dir "${OBSERVABILITY_STATE_DIR:-$HOME/.local/state/payment-system-observability}" --apply
        for dependency in redis nats; do
            kube rollout status "statefulset/$dependency" --timeout=180s
        done
        migrate
        kube apply -f "$ROOT/deploy/k8s/services/"
        # Required when kind images were replaced under the same local tag.
        kube rollout restart deployment/account deployment/auth deployment/ledger \
            deployment/payment deployment/api-gateway
        for service in account auth ledger payment api-gateway; do
            kube rollout status "deployment/$service" --timeout=180s
        done
        ;;
    migrate) migrate ;;
    keys) keys ;;
    *) echo "Usage: $0 [deploy|migrate|keys]" >&2; exit 2 ;;
esac
