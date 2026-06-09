# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build

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

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/log-forwarder /usr/local/bin/log-forwarder

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/log-forwarder"]
CMD ["-config", "/config/config.yaml"]
