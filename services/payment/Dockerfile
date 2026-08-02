FROM golang:1.26.5-alpine AS builder

RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /src

COPY shared/go.mod shared/go.sum ./shared/
COPY gen/go/go.mod gen/go/go.sum ./gen/go/
COPY services/payment/go.mod services/payment/go.sum ./services/payment/

RUN cd services/payment && go mod download

COPY shared/ ./shared/
COPY gen/go/ ./gen/go/
COPY services/payment/ ./services/payment/

WORKDIR /src/services/payment

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-w -s -extldflags=-static" \
    -o /out/payment-service \
    ./cmd/server/

FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -g 1000 appuser && adduser -u 1000 -G appuser -D -s /bin/sh appuser

COPY --from=builder /out/payment-service /app/payment-service

EXPOSE 50053

USER appuser:appuser

ENTRYPOINT ["/app/payment-service"]
