# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build

RUN apk add --no-cache ca-certificates git

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/log-forwarder \
    ./cmd/log-forwarder

FROM alpine:3.20

RUN apk add --no-cache ca-certificates su-exec \
    && addgroup -g 65532 -S forwarder \
    && adduser -u 65532 -S -G forwarder forwarder \
    && mkdir -p /state /output /dlq \
    && chown forwarder:forwarder /state /output /dlq

COPY --from=build /out/log-forwarder /usr/local/bin/log-forwarder
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Entrypoint starts as root to fix volume ownership, then drops to forwarder.
ENTRYPOINT ["/entrypoint.sh"]
CMD ["-config", "/config/config.yaml"]
