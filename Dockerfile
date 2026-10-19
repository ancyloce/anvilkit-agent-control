# anvilkit-agent-control: built from this repository alone (the build context
# is the repository root; nothing from the parent checkout is read). The
# generated contract module is an ordinary versioned dependency resolved
# through GOPROXY: pass --build-arg GOPROXY=... (and GONOSUMDB=... for a module
# that is not in the public checksum database yet) to build against a private
# or local module proxy. The image carries two binaries of one build: the
# service and its migration Job (cmd/anvilkit-migration), which the chart runs
# as a separate Job with the migrator role; the service itself never runs DDL.
FROM golang:1.27.2-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS build
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
FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
COPY --from=build /out/anvilkit-agent-control /usr/local/bin/anvilkit-agent-control
COPY --from=build /out/anvilkit-migration /usr/local/bin/anvilkit-migration
COPY config.yaml /etc/anvilkit/anvilkit-agent-control/config.yaml
ENV ANVILKIT_CONTROL_CONFIG=/etc/anvilkit/anvilkit-agent-control/config.yaml
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/anvilkit-agent-control"]
