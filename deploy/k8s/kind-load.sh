#!/usr/bin/env bash
set -euo pipefail

KIND_CLUSTER=${KIND_CLUSTER:-payment-system}
IMAGE_PLATFORM=${IMAGE_PLATFORM:-linux/amd64}
archive=$(mktemp)
trap 'rm -f "$archive"' EXIT

if [[ $# == 0 ]]; then
    set -- payment-system-account:latest payment-system-auth:latest \
        payment-system-ledger:latest payment-system-payment:latest \
        payment-system-api-gateway:latest
fi

# Export only the node platform. Docker's containerd image store may otherwise
# include an index referencing platforms that are not downloaded locally.
docker image save --platform "$IMAGE_PLATFORM" -o "$archive" "$@"
kind load image-archive --name "$KIND_CLUSTER" "$archive"
