FROM golang:1.26.5-alpine AS builder

RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /src

COPY shared/go.mod shared/go.sum ./shared/
COPY gen/go/go.mod gen/go/go.sum ./gen/go/
COPY services/ledger/go.mod services/ledger/go.sum ./services/ledger/

RUN cd services/ledger && go mod download

COPY shared/ ./shared/
COPY gen/go/ ./gen/go/
COPY services/ledger/ ./services/ledger/

WORKDIR /src/services/ledger

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-w -s -extldflags=-static" \
    -o /out/ledger-service \
    ./cmd/server/

FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -g 1000 appuser && adduser -u 1000 -G appuser -D -s /bin/sh appuser

COPY --from=builder /out/ledger-service /app/ledger-service

EXPOSE 50052

USER 1000:1000

ENTRYPOINT ["/app/ledger-service"]
