FROM golang:1.26.5-alpine AS builder

RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /src

COPY shared/go.mod shared/go.sum ./shared/
COPY gen/go/go.mod gen/go/go.sum ./gen/go/
COPY services/account/go.mod services/account/go.sum ./services/account/

RUN cd services/account && go mod download

COPY shared/ ./shared/
COPY gen/go/ ./gen/go/
COPY services/account/ ./services/account/

WORKDIR /src/services/account

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-w -s -extldflags=-static" \
    -o /out/account-service \
    ./cmd/server/

FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -S -g 10001 appgroup && adduser -S -D -H -u 10001 -G appgroup appuser

COPY --from=builder /out/account-service /app/account-service

EXPOSE 50051

USER 10001:10001

ENTRYPOINT ["/app/account-service"]
