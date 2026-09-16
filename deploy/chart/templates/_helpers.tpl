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
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
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
{{- if not .Values.database.secret.name }}
{{- fail "database.secret.name is required: an existing Secret holding the application-role URL (ANVILKIT_CONTROL_DATABASE_URL), never created by this chart" }}
{{- end }}
{{- if not .Values.temporal.address }}
{{- fail "temporal.address is required: the Temporal frontend (ANVILKIT_CONTROL_TEMPORAL_ADDRESS)" }}
{{- end }}
{{- if or (not .Values.inventory.s3.endpoint) (not .Values.inventory.s3.bucket) (not .Values.inventory.s3.secret.name) }}
{{- fail "inventory.s3.endpoint, inventory.s3.bucket and inventory.s3.secret.name are required: the shared obligation inventory every replica uses (ANVILKIT_CONTROL_INVENTORY_S3_*)" }}
{{- end }}
{{- if and .Values.artifacts.enabled (or (not .Values.artifacts.s3.endpoint) (not .Values.artifacts.s3.bucket) (not .Values.artifacts.s3.secret.name)) }}
{{- fail "artifacts.s3.endpoint, artifacts.s3.bucket and artifacts.s3.secret.name are required while artifacts.enabled is true (ANVILKIT_CONTROL_ARTIFACTS_S3_*)" }}
{{- end }}
{{- if and .Values.migration.enabled (not .Values.migration.secret.name) }}
{{- fail "migration.secret.name is required while migration.enabled is true: an existing Secret holding the migrator-role URL" }}
{{- end }}
{{- end -}}
