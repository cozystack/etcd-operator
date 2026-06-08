{{/* Chart name (overridable). */}}
{{- define "etcd-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified resource-name prefix. */}}
{{- define "etcd-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "etcd-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common labels. */}}
{{- define "etcd-operator.labels" -}}
helm.sh/chart: {{ include "etcd-operator.chart" . }}
{{ include "etcd-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels — stable; also used by the metrics Service selector. */}}
{{- define "etcd-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "etcd-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* ServiceAccount name. */}}
{{- define "etcd-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- include "etcd-operator.fullname" . -}}
{{- else -}}
default
{{- end -}}
{{- end -}}

{{/*
Full operator image reference. Used for BOTH the manager container image and
its OPERATOR_IMAGE env var — they MUST be identical, or the operator refuses to
start (the snapshot/restore agent runs this same image).
*/}}
{{- define "etcd-operator.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
