# anvilkit-agent-control: built from this repository alone (the build context
# is the repository root; nothing from the parent checkout is read). The
# generated contract module is an ordinary versioned dependency resolved
# through GOPROXY: pass --build-arg GOPROXY=... (and GONOSUMDB=... for a module
# that is not in the public checksum database yet) to build against a private
# or local module proxy. The image carries two binaries of one build: the
# service and its migration Job (cmd/anvilkit-migration), which the chart runs
# as a separate Job with the migrator role; the service itself never runs DDL.
FROM golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS build
ARG GOPROXY=https://proxy.golang.org,direct
ARG GONOSUMDB=
ENV GOWORK=off GOFLAGS=-mod=readonly CGO_ENABLED=0 GOPROXY=$GOPROXY GONOSUMDB=$GONOSUMDB
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN go build -trimpath -ldflags="-s -w" -o /out/anvilkit-agent-control ./cmd/anvilkit-agent-control \
 && go build -trimpath -ldflags="-s -w" -o /out/anvilkit-migration ./cmd/anvilkit-migration

# Runtime: the binaries, the reviewed secret-free configuration file and a
# non-root user. The listener, the inventory and artifact backends' placement
# and the secrets (database URL, S3 credentials) are supplied through the
# allowlisted ANVILKIT_CONTROL_* environment overrides; the file itself may
# be replaced by mounting one at the path named by ANVILKIT_CONTROL_CONFIG.
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
COPY --from=build /out/anvilkit-agent-control /usr/local/bin/anvilkit-agent-control
COPY --from=build /out/anvilkit-migration /usr/local/bin/anvilkit-migration
COPY config.yaml /etc/anvilkit/anvilkit-agent-control/config.yaml
ENV ANVILKIT_CONTROL_CONFIG=/etc/anvilkit/anvilkit-agent-control/config.yaml
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/anvilkit-agent-control"]
