# Multi-stage build: compile a static binary, ship it on a minimal non-root base.
# GO_IMAGE can point at a mirror (e.g. mirror.gcr.io/library/golang:1.22-alpine)
# when Docker Hub rate limits apply.
ARG GO_IMAGE=golang:1.22-alpine
# Build on the native platform and cross-compile (fast multi-arch images).
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS=linux
ARG TARGETARCH
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN export CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
 && go build -trimpath -ldflags="-s -w" -o /out/cachenode ./cmd/cachenode \
 && go build -trimpath -ldflags="-s -w" -o /out/cachectl ./cmd/cachectl \
 && go build -trimpath -ldflags="-s -w" -o /out/cachebench ./cmd/cachebench

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
ENV SHC_LISTEN_ADDR=:7000 \
    SHC_ADMIN_ADDR=:8000
EXPOSE 7000 8000
USER nonroot:nonroot
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/cachenode", "-probe", "/healthz"]
ENTRYPOINT ["/usr/local/bin/cachenode"]
