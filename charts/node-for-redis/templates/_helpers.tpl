{{/*
Expand the name of the chart.
*/}}
{{- define "node-for-redis.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "node-for-redis.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "node-for-redis.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Version labels
*/}}
{{- define "node-for-redis.chartLabels" }}
helm.sh/chart: {{ include "node-for-redis.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "node-for-redis.labels" -}}
{{ include "node-for-redis.selectorLabels" . }}
{{ include "node-for-redis.chartLabels" . }}
{{- end }}

{{- define "node-for-redis-metrics.labels" -}}
app.kubernetes.io/name: {{ include "node-for-redis.name" . }}-metrics
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: metrics
{{ include "node-for-redis.chartLabels" . }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "node-for-redis.selectorLabels" -}}
app.kubernetes.io/name: {{ include "node-for-redis.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: database
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "node-for-redis.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "node-for-redis.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Builds a list of extra arguments for the Redis cluster
*/}}
{{- define "redis-cluster.extraarglist" -}}
{{- if .Values.extraArgs -}}
{{- range (compact .Values.extraArgs) }}{{$arg := . | quote}}{{ print ","  $arg }}{{- end}}
{{- end -}}
{{- end -}}

{{/*
Tests to see if there is extra config
*/}}
{{- define "node-for-redis.hasextraconfig" -}}
{{- or .Values.redis.configuration.file .Values.redis.configuration.valueMap }}
{{- end -}}
