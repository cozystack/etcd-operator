{{- define "type" -}}
{{- $type := . -}}
{{- if markdownShouldRenderType $type -}}

#### {{ $type.Name }}

{{ if $type.IsAlias }}_Underlying type:_ _{{ markdownRenderTypeLink $type.UnderlyingType  }}_{{ end }}

{{ $type.Doc }}

{{ if $type.Validation -}}
_Validation:_
{{- range $type.Validation }}
- {{ . }}
{{- end }}
{{- end }}

{{/*
Filter SortedReferences down to ones that will actually have a
heading in this document. Without the filter, a referencing
type that crd-ref-docs decides not to render (e.g. EtcdBackupStatus
under our .crd-docs.yaml `ignoreFields: status$`) leaves behind a
markdown link to a non-existent anchor — flagged by MD051 / markdown
review tooling and broken for human readers clicking the
"Appears in:" line.
*/}}
{{- $rendered := list -}}
{{- range $type.SortedReferences -}}
{{- if markdownShouldRenderType . -}}
{{- $rendered = append $rendered . -}}
{{- end -}}
{{- end -}}
{{ if $rendered -}}
_Appears in:_
{{- range $rendered }}
- {{ markdownRenderTypeLink . }}
{{- end }}
{{- end }}

{{ if $type.Members -}}
| Field | Description | Default | Validation |
| --- | --- | --- | --- |
{{ if $type.GVK -}}
| `apiVersion` _string_ | `{{ $type.GVK.Group }}/{{ $type.GVK.Version }}` | | |
| `kind` _string_ | `{{ $type.GVK.Kind }}` | | |
{{ end -}}

{{ range $type.Members -}}
| `{{ .Name  }}` _{{ markdownRenderType .Type }}_ | {{ template "type_members" . }} | {{ markdownRenderDefault .Default }} | {{ range .Validation -}} {{ . | replace "|" "\\|" }} <br />{{ end }} |
{{ end -}}

{{ end -}}

{{- end -}}
{{- end -}}
