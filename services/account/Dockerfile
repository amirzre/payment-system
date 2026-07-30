FROM golang:1.26.5-alpine AS builder

RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /src

COPY go.work go.work.sum ./
COPY shared/go.mod shared/go.sum ./shared/
COPY gen/go/go.mod gen/go/go.sum ./gen/go/
COPY services/account/go.mod services/account/go.sum ./services/account/

RUN go work sync

COPY shared/ ./shared/
COPY gen/go/ ./gen/go/
COPY services/account/ ./services/account/

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-w -s -extldflags=-static" \
    -o /out/account-service \
    ./services/account/cmd/server/

FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -g 1000 appuser && adduser -u 1000 -G appuser -D -s /bin/sh appuser

COPY --from=builder /out/account-service /app/account-service

EXPOSE 50051

USER appuser:appuser

ENTRYPOINT ["/app/account-service"]
