# No package installation, shell changes, or modified upstream entrypoints.
ARG GO_IMAGE
ARG BASE_IMAGE
FROM ${GO_IMAGE} AS probe
WORKDIR /src
COPY scripts/observability-probe.go ./main.go
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w -buildid=' -o /observability-probe main.go
FROM ${BASE_IMAGE}
COPY --from=probe --chmod=0555 /observability-probe /usr/local/bin/observability-probe
