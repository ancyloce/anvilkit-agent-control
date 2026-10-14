{{/* The stable service identity (architecture.md naming): Deployment, Service,
ServiceAccount and Helm release share it. */}}
{{- define "anvilkit-agent-control.name" -}}
anvilkit-agent-control
{{- end -}}

{{- define "anvilkit-agent-control.fullname" -}}
{{- if eq .Release.Name (include "anvilkit-agent-control.name" .) -}}
{{- .Release.Name -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "anvilkit-agent-control.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-control.labels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-control.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/component: control
app.kubernetes.io/part-of: anvilkit
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "anvilkit-agent-control.selectorLabels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-control.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "anvilkit-agent-control.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "anvilkit-agent-control.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The image reference: a digest pins the exact image, otherwise the tag
(defaulting to the chart's appVersion). */}}
{{- define "anvilkit-agent-control.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* Required environment values, checked once for every template. */}}
{{- define "anvilkit-agent-control.require" -}}
{{- $kube := eq .Values.secrets.provider "kubernetes" -}}
{{- if not (has .Values.secrets.provider (list "kubernetes" "csi")) }}
{{- fail "secrets.provider must be kubernetes or csi" }}
{{- end }}
{{- if and (not $kube) (or (not .Values.secrets.csi.address) (not .Values.secrets.csi.path)) }}
{{- fail "secrets.csi.address and secrets.csi.path are required under secrets.provider csi (the OpenBao address and the service's KV v2 data path)" }}
{{- end }}
{{- if and (not $kube) .Values.migration.enabled (not .Values.secrets.csi.migrationPath) }}
{{- fail "secrets.csi.migrationPath is required under secrets.provider csi while migration.enabled is true" }}
{{- end }}
{{- if and $kube (not .Values.database.secret.name) }}
{{- fail "database.secret.name is required: an existing Secret holding the application-role URL (ANVILKIT_CONTROL_DATABASE_URL), never created by this chart" }}
{{- end }}
{{- if not .Values.temporal.address }}
{{- fail "temporal.address is required: the Temporal frontend (ANVILKIT_CONTROL_TEMPORAL_ADDRESS)" }}
{{- end }}
{{- if or (not .Values.inventory.s3.endpoint) (not .Values.inventory.s3.bucket) (and $kube (not .Values.inventory.s3.secret.name)) }}
{{- fail "inventory.s3.endpoint, inventory.s3.bucket and inventory.s3.secret.name are required: the shared obligation inventory every replica uses (ANVILKIT_CONTROL_INVENTORY_S3_*)" }}
{{- end }}
{{- if and .Values.artifacts.enabled (or (not .Values.artifacts.s3.endpoint) (not .Values.artifacts.s3.bucket) (and $kube (not .Values.artifacts.s3.secret.name))) }}
{{- fail "artifacts.s3.endpoint, artifacts.s3.bucket and artifacts.s3.secret.name are required while artifacts.enabled is true (ANVILKIT_CONTROL_ARTIFACTS_S3_*)" }}
{{- end }}
{{- if and $kube .Values.migration.enabled (not .Values.migration.secret.name) }}
{{- fail "migration.secret.name is required while migration.enabled is true: an existing Secret holding the migrator-role URL" }}
{{- end }}
{{- if not (has .Values.identity.mode (list "mtls" "development")) }}
{{- fail "identity.mode must be mtls or development" }}
{{- end }}
{{- if and (eq .Values.identity.mode "development") (not .Values.development.enabled) }}
{{- fail "identity.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (a plaintext listener authenticates and authorizes no caller)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.trustDomain) (not .Values.development.enabled) }}
{{- fail "identity.trustDomain is required outside development (the development default anvilkit.local applies only with development.enabled: true)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") .Values.identity.certificate.create (not .Values.identity.certificate.issuerRef.name) }}
{{- fail "identity.certificate.issuerRef.name is required: the cert-manager issuer of the workload certificate (or set identity.certificate.create false and identity.secretName)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.certificate.create) (not .Values.identity.secretName) }}
{{- fail "identity.secretName is required while identity.certificate.create is false" }}
{{- end }}
{{- /* P2.1/P2.3: the dispatch chain's authority inputs. */ -}}
{{- $dispatchDev := dig "dispatch" "development" "enabled" false .Values.config -}}
{{- if and $dispatchDev (not .Values.development.enabled) }}
{{- fail "config.dispatch.development.enabled is DEVELOPMENT_ONLY: it renders only with development.enabled: true (fixture prices, routes and unsigned evidence)" }}
{{- end }}
{{- if not (has .Values.modelProxy.identityMode (list "mtls" "development")) }}
{{- fail "modelProxy.identityMode must be mtls or development" }}
{{- end }}
{{- if and (eq .Values.modelProxy.identityMode "development") (not .Values.development.enabled) }}
{{- fail "modelProxy.identityMode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (a bearer token authenticates no workload)" }}
{{- end }}
{{- if and (eq .Values.modelProxy.identityMode "development") .Values.modelProxy.address (not .Values.modelProxy.tokenSecret.name) }}
{{- fail "modelProxy.tokenSecret.name is required under modelProxy.identityMode development (ANVILKIT_CONTROL_MODEL_PROXY_TOKEN)" }}
{{- end }}
{{- if and (eq .Values.modelProxy.identityMode "mtls") .Values.modelProxy.address (ne .Values.identity.mode "mtls") }}
{{- fail "modelProxy.identityMode mtls presents the workload certificate: it needs identity.mode mtls" }}
{{- end }}
{{- if not (has .Values.authority.identity (list "mtls" "token")) }}
{{- fail "authority.identity must be mtls or token" }}
{{- end }}
{{- if and .Values.authority.url (eq .Values.authority.identity "token") (ne .Values.secrets.provider "csi") }}
{{- fail "authority.identity token reads the CSI file authority-token: it needs secrets.provider csi" }}
{{- end }}
{{- if and .Values.authority.url (eq .Values.authority.identity "mtls") (ne .Values.identity.mode "mtls") }}
{{- fail "authority.identity mtls presents the workload certificate: it needs identity.mode mtls" }}
{{- end }}
{{- if and .Values.evidence.issuers (not .Values.evidence.configMap) }}
{{- fail "evidence.configMap is required with evidence.issuers: the ConfigMap holding <keyId>.pem per issuer key" }}
{{- end }}
{{- if not $dispatchDev }}
{{- if not .Values.authority.url }}
{{- fail "authority.url is required outside config.dispatch.development: the ENV-07 commercial authority decision endpoint (P2.1)" }}
{{- end }}
{{- if not .Values.evidence.issuers }}
{{- fail "evidence.issuers is required outside config.dispatch.development: the trusted signers of not-sent and disposition evidence (P2.1)" }}
{{- end }}
{{- if not .Values.modelProxy.address }}
{{- fail "modelProxy.address is required outside config.dispatch.development: the original-identity query of model dispatches (P2.3)" }}
{{- end }}
{{- end }}
{{- if not (has .Values.temporal.tls.mode (list "tls" "mtls" "development")) }}
{{- fail "temporal.tls.mode must be tls, mtls or development" }}
{{- end }}
{{- if and (eq .Values.temporal.tls.mode "development") (not .Values.development.enabled) }}
{{- fail "temporal.tls.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (ne .Values.temporal.tls.mode "development") (not .Values.temporal.tls.caSecret.name) }}
{{- fail "temporal.tls.caSecret.name is required under temporal.tls.mode tls or mtls: the Secret holding the frontend's CA bundle (key ca.crt)" }}
{{- end }}
{{- if and (eq .Values.temporal.tls.mode "mtls") (ne .Values.identity.mode "mtls") }}
{{- fail "temporal.tls.mode mtls presents the workload certificate and needs identity.mode mtls" }}
{{- end }}
{{- if .Values.telemetry.otlpEndpoint }}
{{- /* P2.8: operation events reach JetStream outside development. */ -}}
{{- if and (not .Values.events.enabled) (not .Values.development.enabled) }}
{{- fail "events.enabled is required outside development: operation events reach JetStream through the forwarder (P2.8)" }}
{{- end }}
{{- if .Values.events.enabled }}
{{- if not .Values.events.nats.url }}
{{- fail "events.nats.url is required while events.enabled is true: the JetStream placement (ANVILKIT_CONTROL_NATS_URL)" }}
{{- end }}
{{- if not (has .Values.events.nats.tls.mode (list "tls" "mtls" "development")) }}
{{- fail "events.nats.tls.mode must be tls, mtls or development" }}
{{- end }}
{{- if and (eq .Values.events.nats.tls.mode "development") (not .Values.development.enabled) }}
{{- fail "events.nats.tls.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (ne .Values.events.nats.tls.mode "development") (not .Values.events.nats.tls.caSecret.name) }}
{{- fail "events.nats.tls.caSecret.name is required under events.nats.tls.mode tls or mtls: the Secret holding the NATS server's CA bundle (key ca.crt)" }}
{{- end }}
{{- if and (eq .Values.events.nats.tls.mode "mtls") (ne .Values.identity.mode "mtls") }}
{{- fail "events.nats.tls.mode mtls presents the workload certificate and needs identity.mode mtls" }}
{{- end }}
{{- if and (ne .Values.events.nats.tls.mode "development") (ne .Values.secrets.provider "csi") (not .Values.events.nats.credentials.secret.name) }}
{{- fail "events.nats.credentials.secret.name is required under secrets.provider kubernetes unless events.nats.tls.mode is development (the NATS user credential; under csi it is the key nats-creds)" }}
{{- end }}
{{- end }}
{{- if not (has .Values.telemetry.otlpTls.mode (list "tls" "mtls" "development")) }}
{{- fail "telemetry.otlpTls.mode must be tls, mtls or development" }}
{{- end }}
{{- if and (eq .Values.telemetry.otlpTls.mode "development") (not .Values.development.enabled) }}
{{- fail "telemetry.otlpTls.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (ne .Values.telemetry.otlpTls.mode "development") (not .Values.telemetry.otlpTls.caSecret.name) }}
{{- fail "telemetry.otlpTls.caSecret.name is required under telemetry.otlpTls.mode tls or mtls" }}
{{- end }}
{{- end }}
{{- end -}}

{{/* The identity Secret: the rendered Certificate's or the environment's. */}}
{{- define "anvilkit-agent-control.identitySecret" -}}
{{- if .Values.identity.certificate.create -}}
{{- printf "%s-identity" (include "anvilkit-agent-control.fullname" .) -}}
{{- else -}}
{{- .Values.identity.secretName -}}
{{- end -}}
{{- end -}}

{{/* The rendered configuration: the reviewed sections plus the chart-owned
identity, guard, health and transport settings, so every value above is
wired to the loader's keys. */}}
{{- define "anvilkit-agent-control.config" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- $_ := set $cfg.inventory "backend" "s3" -}}
{{- $_ = set $cfg.artifacts "backend" (ternary "s3" "disabled" .Values.artifacts.enabled) -}}
{{- $_ = set $cfg "development" (dict "enabled" .Values.development.enabled) -}}
{{- $_ = set $cfg "health" (dict "listen" (printf "0.0.0.0:%d" (int .Values.health.port))) -}}
{{- $id := dict "mode" .Values.identity.mode "max_connection_age" .Values.identity.maxConnectionAge "reload_interval" (dig "identity" "reload_interval" "5s" $cfg.grpc) -}}
{{- if .Values.identity.trustDomain }}{{- $_ = set $id "trust_domain" .Values.identity.trustDomain }}{{- end -}}
{{- if eq .Values.identity.mode "mtls" }}
{{- $_ = set $id "cert_file" "/etc/anvilkit/identity/tls.crt" }}
{{- $_ = set $id "key_file" "/etc/anvilkit/identity/tls.key" }}
{{- $_ = set $id "ca_file" "/etc/anvilkit/identity/ca.crt" }}
{{- end -}}
{{- $_ = set $cfg.grpc "identity" $id -}}
{{- /* P2.3: the Model Proxy placement and the identity Control presents. */ -}}
{{- $mp := default (dict) $cfg.model_proxy -}}
{{- if .Values.modelProxy.address }}{{- $_ = set $mp "address" .Values.modelProxy.address }}{{- end -}}
{{- $mpid := dict "mode" .Values.modelProxy.identityMode -}}
{{- if eq .Values.modelProxy.identityMode "mtls" }}
{{- $_ = set $mpid "mtls" (dict "cert_file" "/etc/anvilkit/identity/tls.crt" "key_file" "/etc/anvilkit/identity/tls.key" "ca_file" "/etc/anvilkit/identity/ca.crt" "server_name" .Values.modelProxy.serverName) }}
{{- end -}}
{{- $_ = set $mp "identity" $mpid -}}
{{- $_ = set $cfg "model_proxy" $mp -}}
{{- /* P2.1: the production authority inputs of the dispatch chain. */ -}}
{{- $d := default (dict) $cfg.dispatch -}}
{{- if .Values.authority.url }}
{{- $a := dict "url" .Values.authority.url -}}
{{- if .Values.authority.serverName }}{{- $_ = set $a "server_name" .Values.authority.serverName }}{{- end -}}
{{- if eq .Values.authority.identity "token" }}
{{- $_ = set $a "token_file" "/var/run/secrets/anvilkit/csi/authority-token" }}
{{- else }}
{{- $_ = set $a "cert_file" "/etc/anvilkit/identity/tls.crt" }}
{{- $_ = set $a "key_file" "/etc/anvilkit/identity/tls.key" }}
{{- end -}}
{{- $_ = set $d "authority" $a -}}
{{- end -}}
{{- if .Values.evidence.issuers }}
{{- $issuers := list -}}
{{- range .Values.evidence.issuers }}
{{- $issuers = append $issuers (dict "name" .name "key_id" .keyId "public_key_file" (printf "/etc/anvilkit/evidence/%s.pem" .keyId)) -}}
{{- end -}}
{{- $_ = set $d "evidence" (dict "issuers" $issuers) -}}
{{- end -}}
{{- if .Values.prices.configMap }}
{{- $p := default (dict) $d.prices -}}
{{- $_ = set $p "initial_file" "/etc/anvilkit/prices/prices.yaml" -}}
{{- $_ = set $d "prices" $p -}}
{{- end -}}
{{- $_ = set $cfg "dispatch" $d -}}
{{- $tt := dict "mode" .Values.temporal.tls.mode -}}
{{- if ne .Values.temporal.tls.mode "development" }}
{{- $_ = set $tt "ca_file" "/etc/anvilkit/temporal-ca/ca.crt" }}
{{- if .Values.temporal.tls.serverName }}{{- $_ = set $tt "server_name" .Values.temporal.tls.serverName }}{{- end }}
{{- end -}}
{{- if eq .Values.temporal.tls.mode "mtls" }}
{{- $_ = set $tt "cert_file" "/etc/anvilkit/identity/tls.crt" }}
{{- $_ = set $tt "key_file" "/etc/anvilkit/identity/tls.key" }}
{{- end -}}
{{- $_ = set $cfg.temporal "tls" $tt -}}
{{- /* P2.8: the event publication; the URL and the credential file are environment placements. */ -}}
{{- $ev := merge (dict "enabled" .Values.events.enabled "wake" .Values.events.wake "retention" .Values.events.retention) (default (dict) $cfg.events) -}}
{{- $nt := dict "mode" .Values.events.nats.tls.mode -}}
{{- if ne .Values.events.nats.tls.mode "development" }}
{{- $_ = set $nt "ca_file" "/etc/anvilkit/nats-ca/ca.crt" }}
{{- if .Values.events.nats.tls.serverName }}{{- $_ = set $nt "server_name" .Values.events.nats.tls.serverName }}{{- end }}
{{- end -}}
{{- if eq .Values.events.nats.tls.mode "mtls" }}
{{- $_ = set $nt "cert_file" "/etc/anvilkit/identity/tls.crt" }}
{{- $_ = set $nt "key_file" "/etc/anvilkit/identity/tls.key" }}
{{- end -}}
{{- $_ = set $ev "nats" (merge (dict "tls" $nt) (default (dict) $ev.nats)) -}}
{{- $_ = set $cfg "events" $ev -}}
{{- if .Values.telemetry.otlpEndpoint }}
{{- $ot := dict "mode" .Values.telemetry.otlpTls.mode -}}
{{- if ne .Values.telemetry.otlpTls.mode "development" }}
{{- $_ = set $ot "ca_file" "/etc/anvilkit/otlp-ca/ca.crt" }}
{{- if .Values.telemetry.otlpTls.serverName }}{{- $_ = set $ot "server_name" .Values.telemetry.otlpTls.serverName }}{{- end }}
{{- end -}}
{{- if eq .Values.telemetry.otlpTls.mode "mtls" }}
{{- $_ = set $ot "cert_file" "/etc/anvilkit/identity/tls.crt" }}
{{- $_ = set $ot "key_file" "/etc/anvilkit/identity/tls.key" }}
{{- end -}}
{{- $_ = set $cfg "telemetry" (merge (dict "otlp_tls" $ot) (default (dict) $cfg.telemetry)) -}}
{{- end -}}
{{- toYaml $cfg -}}
{{- end -}}

{{/* P0.6: the CSI SecretProviderClass of one workload role: provider
openbao, the role's Kubernetes-auth role, and one file per key of its KV v2
path (world-readable inside the Pod's own volume: the containers run as
non-root users). */}}
{{- define "anvilkit-agent-control.secretProviderClass" -}}
{{- $c := .root.Values.secrets.csi -}}
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: {{ .name }}
  labels:
    {{- include "anvilkit-agent-control.labels" .root | nindent 4 }}
  {{- with .annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  provider: openbao
  parameters:
    baoAddress: {{ required "secrets.csi.address is required under secrets.provider csi (https://<openbao>:8200)" $c.address | quote }}
    {{- with $c.caCertPath }}
    baoCACertPath: {{ . | quote }}
    {{- end }}
    roleName: {{ .role | quote }}
    audience: {{ $c.audience | quote }}
    objects: |
      {{- range .keys }}
      - objectName: {{ . | quote }}
        secretPath: {{ $.path | quote }}
        secretKey: {{ . | quote }}
        filePermission: 0444
      {{- end }}
{{- end -}}

{{- define "anvilkit-agent-control.csiKeys" -}}
{{- $keys := list "database-url" "inventory-access-key-id" "inventory-secret-access-key" -}}
{{- if .Values.artifacts.enabled -}}
{{- $keys = concat $keys (list "artifacts-access-key-id" "artifacts-secret-access-key") -}}
{{- end -}}
{{- if and .Values.authority.url (eq .Values.authority.identity "token") -}}
{{- $keys = append $keys "authority-token" -}}
{{- end -}}
{{- if .Values.events.enabled -}}
{{- $keys = append $keys "nats-creds" -}}
{{- end -}}
{{- toJson $keys -}}
{{- end -}}
