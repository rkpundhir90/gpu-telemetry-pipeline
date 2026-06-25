{{- define "gpu-telemetry-queue.name" -}}
gpu-telemetry-queue
{{- end -}}

{{- define "gpu-telemetry-queue.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "gpu-telemetry-queue.labels" -}}
app.kubernetes.io/name: {{ include "gpu-telemetry-queue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: gpu-telemetry-pipeline
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "gpu-telemetry-queue.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gpu-telemetry-queue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
