FROM golang:1.26.5-alpine AS builder

RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /src

COPY shared/go.mod shared/go.sum ./shared/
COPY gen/go/go.mod gen/go/go.sum ./gen/go/
COPY services/auth/go.mod services/auth/go.sum ./services/auth/

RUN cd services/auth && go mod download

COPY shared/ ./shared/
COPY gen/go/ ./gen/go/
COPY services/auth/ ./services/auth/

WORKDIR /src/services/auth

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-w -s -extldflags=-static" \
    -o /out/auth-service \
    ./cmd/server/

FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -g 1000 appuser && adduser -u 1000 -G appuser -D -s /bin/sh appuser

COPY --from=builder /out/auth-service /app/auth-service

EXPOSE 50054

USER appuser:appuser

ENTRYPOINT ["/app/auth-service"]
