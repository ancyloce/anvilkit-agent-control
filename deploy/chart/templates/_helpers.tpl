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
{{- toJson $keys -}}
{{- end -}}
