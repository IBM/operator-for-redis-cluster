{{/*
Expand the name of the chart.
*/}}
{{- define "operator-for-redis.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "operator-for-redis.fullname" -}}
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
{{- define "operator-for-redis.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "operator-for-redis.labels" -}}
helm.sh/chart: {{ include "operator-for-redis.chart" . }}
{{ include "operator-for-redis.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "operator-for-redis.selectorLabels" -}}
app.kubernetes.io/name: {{ include "operator-for-redis.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "operator-for-redis.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "operator-for-redis.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Builds the argument list for the redis operator
*/}}
{{- define "operator-for-redis.arglist" -}}
{{- $logLevel := (print "--v=" .Values.logLevel) -}}
{{- $args := concat (prepend .Values.args $logLevel) .Values.extraArgs -}}
{{- $argsList := list }}
{{- range $args }}{{- $argAsStr := . | quote }}{{- $argsList = append $argsList $argAsStr}}{{- end}}
{{- join "," (compact $argsList) }}
{{- end -}}
