/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package migrate

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

// Legacy GVRs the tool discovers and deletes.
var (
	ClusterGVR  = schema.GroupVersionResource{Group: legacy.Group, Version: legacy.Version, Resource: "etcdclusters"}
	BackupGVR   = schema.GroupVersionResource{Group: legacy.Group, Version: legacy.Version, Resource: "etcdbackups"}
	ScheduleGVR = schema.GroupVersionResource{Group: legacy.Group, Version: legacy.Version, Resource: "etcdbackupschedules"}
)

// versionRe is the new API's spec.version pattern.
var versionRe = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

// TranslateOptions carries the per-run knobs that influence translation.
type TranslateOptions struct {
	// VersionOverride forces spec.version for every cluster instead of
	// extracting it from the legacy image tag.
	VersionOverride string
	// AuthSecretName references an existing kubernetes.io/basic-auth Secret
	// (in each cluster's namespace) for clusters with enableAuth. Empty ⇒
	// the tool generates one per cluster.
	AuthSecretName string
}

// TranslateCluster converts one legacy EtcdCluster into a v1alpha2 plan
// entry. It is pure apart from generating a random password for the auth
// Secret when one is needed and none was supplied.
func TranslateCluster(name, namespace string, spec legacy.EtcdClusterSpec, opts TranslateOptions) ResourcePlan {
	plan := ResourcePlan{
		SourceKind: "EtcdCluster",
		SourceName: name,
		Namespace:  namespace,
		Action:     ActionCreate,
		DeleteRef:  &ObjectRef{GVR: ClusterGVR, Namespace: namespace, Name: name},
	}

	out := &lll.EtcdCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: lll.GroupVersion.String(), Kind: "EtcdCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}

	// Replicas: same semantics, same default (3).
	if spec.Replicas != nil {
		r := *spec.Replicas
		out.Spec.Replicas = &r
	}

	// Version from the etcd container image tag (or override).
	version, vWarns, vErr := extractVersion(spec.PodTemplate.Spec, opts.VersionOverride)
	plan.Warnings = append(plan.Warnings, vWarns...)
	if vErr != nil {
		plan.Errors = append(plan.Errors, vErr.Error())
	}
	out.Spec.Version = version

	// Storage.
	storage, sWarns, sErr := translateStorage(spec.Storage)
	plan.Warnings = append(plan.Warnings, sWarns...)
	if sErr != nil {
		plan.Errors = append(plan.Errors, sErr.Error())
	}
	out.Spec.Storage = storage

	// Pod template: the mappable subset, plus warnings for everything else.
	translatePodTemplate(spec.PodTemplate, out, &plan)

	// Templates the new operator owns outright.
	if spec.ServiceTemplate != nil {
		plan.Warnings = append(plan.Warnings,
			"spec.serviceTemplate is dropped: the new operator owns the client Service (named \"<cluster>-client\")")
	}
	if spec.HeadlessServiceTemplate != nil {
		plan.Warnings = append(plan.Warnings,
			"spec.headlessServiceTemplate is dropped: the new operator owns the headless Service (named \"<cluster>\")")
	}
	if spec.PodDisruptionBudgetTemplate != nil {
		plan.Warnings = append(plan.Warnings,
			"spec.podDisruptionBudgetTemplate is dropped: the new operator auto-emits a PDB with maxUnavailable=(voters-1)/2")
	}

	// etcd args: v1alpha2's spec.options is a closed typed struct covering
	// exactly the keys Cozystack's legacy package set. Map those four;
	// anything else has no typed equivalent and is dropped with a warning.
	if len(spec.Options) > 0 {
		typed, oWarns, oErrs := translateEtcdOptions(spec.Options)
		plan.Warnings = append(plan.Warnings, oWarns...)
		plan.Errors = append(plan.Errors, oErrs...)
		out.Spec.Options = typed
	}

	// TLS.
	tls, tWarns := translateTLS(spec.Security)
	plan.Warnings = append(plan.Warnings, tWarns...)
	out.Spec.TLS = tls

	// Auth.
	translateAuth(spec.Security, out, &plan, opts)

	// Restore-at-bootstrap is dropped: the adopted cluster already has its
	// data, and the new API consults spec.bootstrap only at first bootstrap
	// — which an adopted cluster (status.clusterID prefilled) never runs.
	if spec.Bootstrap != nil && spec.Bootstrap.Restore != nil {
		plan.Warnings = append(plan.Warnings,
			"spec.bootstrap.restore is dropped: it was consumed at the legacy cluster's creation and an adopted cluster never bootstraps")
	}

	if len(plan.Errors) > 0 {
		plan.Action = ActionError
		plan.Target = nil
		plan.Extras = nil
		plan.DeleteRef = nil
		return plan
	}
	plan.Target = out
	return plan
}

// extractVersion derives spec.version from the legacy etcd container image
// tag, honoring an override.
func extractVersion(podSpec corev1.PodSpec, override string) (string, []string, error) {
	if override != "" {
		if !versionRe.MatchString(override) {
			return "", nil, fmt.Errorf("--version %q does not match required pattern X.Y.Z", override)
		}
		return override, nil, nil
	}
	image := legacy.DefaultEtcdImage
	var warns []string
	if c := findContainer(podSpec.Containers, "etcd"); c != nil && c.Image != "" {
		image = c.Image
	} else {
		warns = append(warns, fmt.Sprintf("no etcd image override in podTemplate; assuming the legacy default %s", legacy.DefaultEtcdImage))
	}
	idx := strings.LastIndex(image, ":")
	if idx < 0 || idx == len(image)-1 {
		return "", warns, fmt.Errorf("cannot extract etcd version from image %q (no tag); pass --version", image)
	}
	tag := strings.TrimPrefix(image[idx+1:], "v")
	if !versionRe.MatchString(tag) {
		return "", warns, fmt.Errorf("cannot derive etcd version from image tag %q (want X.Y.Z); pass --version", image[idx+1:])
	}
	return tag, warns, nil
}

func findContainer(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

// translateStorage maps the legacy emptyDir/volumeClaimTemplate union onto
// the new size/medium/storageClassName triple.
func translateStorage(s legacy.StorageSpec) (lll.StorageSpec, []string, error) {
	// emptyDir takes precedence over volumeClaimTemplate in the legacy
	// operator, so it does here too.
	if s.EmptyDir != nil {
		// In-place adoption hands the new operator the EXISTING pods and
		// their PVCs. An emptyDir cluster has no PVCs to adopt: the data
		// lives in pod-bound volumes, an EtcdMember would have nothing to
		// own, and the first replacement would silently lose the member's
		// data. Recreate such clusters manually.
		return lll.StorageSpec{}, nil, fmt.Errorf(
			"storage.emptyDir cannot be migrated in place: the data lives in pod-bound volumes with no PVC for the new operator to adopt; recreate this cluster manually")
	}

	vct := s.VolumeClaimTemplate
	size, hasSize := vct.Spec.Resources.Requests[corev1.ResourceStorage]
	vctSet := hasSize || vct.Spec.StorageClassName != nil || len(vct.Spec.AccessModes) > 0 || vct.Name != ""
	if !vctSet {
		// Neither emptyDir nor volumeClaimTemplate: the legacy operator
		// defaulted to a disk-backed emptyDir — same dead end as above.
		return lll.StorageSpec{}, nil, fmt.Errorf(
			"no storage configured: the legacy operator defaulted to a disk-backed emptyDir, which cannot be migrated in place (no PVC to adopt); recreate this cluster manually")
	}

	out := lll.StorageSpec{StorageClassName: vct.Spec.StorageClassName}
	var warns []string
	if hasSize {
		out.Size = size
	} else {
		out.Size = resource.MustParse("1Gi")
		warns = append(warns, "volumeClaimTemplate has no requests.storage; defaulting spec.storage.size to 1Gi")
	}
	return out, warns, nil
}

// translatePodTemplate maps the supported pod-template subset and warns
// about everything else it finds populated.
// translateEtcdOptions maps the legacy free-form spec.options map onto
// v1alpha2's closed typed EtcdOptions. The four keys Cozystack's legacy
// package set translate 1:1; unknown keys are dropped with a warning.
// Unparsable numeric values are errors, not warnings — silently dropping a
// backend quota the user had set would change the cluster's NOSPACE
// behaviour on migrate.
func translateEtcdOptions(options map[string]string) (*lll.EtcdOptions, []string, []string) {
	var warnings, errs []string
	typed := &lll.EtcdOptions{}
	mapped := false

	keys := make([]string, 0, len(options))
	for k := range options {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var unknown []string
	for _, k := range keys {
		v := options[k]
		switch k {
		case "quota-backend-bytes":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errs = append(errs, fmt.Sprintf("spec.options[%q]=%q is not an integer", k, v))
				continue
			}
			typed.QuotaBackendBytes = &n
			mapped = true
		case "auto-compaction-mode":
			if v != string(lll.AutoCompactionModePeriodic) && v != string(lll.AutoCompactionModeRevision) {
				errs = append(errs, fmt.Sprintf("spec.options[%q]=%q must be %q or %q", k, v,
					lll.AutoCompactionModePeriodic, lll.AutoCompactionModeRevision))
				continue
			}
			typed.AutoCompactionMode = lll.AutoCompactionMode(v)
			mapped = true
		case "auto-compaction-retention":
			typed.AutoCompactionRetention = v
			mapped = true
		case "snapshot-count":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errs = append(errs, fmt.Sprintf("spec.options[%q]=%q is not an integer", k, v))
				continue
			}
			typed.SnapshotCount = &n
			mapped = true
		default:
			unknown = append(unknown, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(unknown) > 0 {
		warnings = append(warnings,
			"spec.options keys with no typed v1alpha2 equivalent; dropped etcd args: "+strings.Join(unknown, ", "))
	}
	if !mapped {
		typed = nil
	}
	return typed, warnings, errs
}

func translatePodTemplate(pt legacy.PodTemplate, out *lll.EtcdCluster, plan *ResourcePlan) {
	if len(pt.Labels) > 0 || len(pt.Annotations) > 0 {
		out.Spec.AdditionalMetadata = &lll.AdditionalMetadata{
			Labels:      pt.Labels,
			Annotations: pt.Annotations,
		}
	}
	ps := pt.Spec
	out.Spec.Affinity = ps.Affinity
	out.Spec.TopologySpreadConstraints = ps.TopologySpreadConstraints

	// Carry pull secrets so the new operator can still pull from a private
	// (e.g. air-gapped) registry. v1alpha2 grew spec.imagePullSecrets, so
	// this is no longer dropped.
	if len(ps.ImagePullSecrets) > 0 {
		out.Spec.ImagePullSecrets = ps.ImagePullSecrets
	}

	var dropped []string
	if c := findContainer(ps.Containers, "etcd"); c != nil {
		out.Spec.Resources = c.Resources
		// Image (consumed above by extractVersion → spec.version) and Resources
		// are mapped; everything else on the etcd container is an unmappable
		// override. The image's registry/tag is deliberately not carried: the
		// operator pins the etcd image to spec.version, so a private mirror is
		// repointed via the operator-wide --etcd-image-repository, not per
		// cluster.
		for field, set := range map[string]bool{
			"command":         len(c.Command) > 0,
			"args":            len(c.Args) > 0,
			"env":             len(c.Env) > 0,
			"envFrom":         len(c.EnvFrom) > 0,
			"volumeMounts":    len(c.VolumeMounts) > 0,
			"ports":           len(c.Ports) > 0,
			"livenessProbe":   c.LivenessProbe != nil,
			"readinessProbe":  c.ReadinessProbe != nil,
			"startupProbe":    c.StartupProbe != nil,
			"securityContext": c.SecurityContext != nil,
		} {
			if set {
				dropped = append(dropped, "containers[etcd]."+field)
			}
		}
	}
	for i := range ps.Containers {
		if ps.Containers[i].Name != "etcd" {
			dropped = append(dropped, fmt.Sprintf("containers[%s] (sidecar)", ps.Containers[i].Name))
		}
	}
	for field, set := range map[string]bool{
		"initContainers":                len(ps.InitContainers) > 0,
		"volumes":                       len(ps.Volumes) > 0,
		"nodeSelector":                  len(ps.NodeSelector) > 0,
		"tolerations":                   len(ps.Tolerations) > 0,
		"serviceAccountName":            ps.ServiceAccountName != "",
		"securityContext":               ps.SecurityContext != nil && !equality.Semantic.DeepEqual(*ps.SecurityContext, corev1.PodSecurityContext{}),
		"priorityClassName":             ps.PriorityClassName != "",
		"hostNetwork":                   ps.HostNetwork,
		"hostAliases":                   len(ps.HostAliases) > 0,
		"dnsPolicy":                     ps.DNSPolicy != "",
		"dnsConfig":                     ps.DNSConfig != nil,
		"runtimeClassName":              ps.RuntimeClassName != nil,
		"schedulerName":                 ps.SchedulerName != "",
		"terminationGracePeriodSeconds": ps.TerminationGracePeriodSeconds != nil,
	} {
		if set {
			dropped = append(dropped, field)
		}
	}
	if len(dropped) > 0 {
		sort.Strings(dropped)
		plan.Warnings = append(plan.Warnings,
			"spec.podTemplate fields with no v1alpha2 equivalent are dropped: "+strings.Join(dropped, ", "))
	}
}

// translateTLS maps the legacy six-secret layout onto the new two-subtree
// model. The new operator expects ca.crt INSIDE the server/peer secrets;
// legacy kept CAs in separate secrets, so merges become user follow-ups.
func translateTLS(sec *legacy.SecuritySpec) (*lll.EtcdClusterTLS, []string) {
	if sec == nil {
		return nil, nil
	}
	t := sec.TLS
	var warns []string
	out := &lll.EtcdClusterTLS{}

	if t.ServerSecret != "" {
		out.Client = &lll.ClientTLS{
			ServerSecretRef: &corev1.LocalObjectReference{Name: t.ServerSecret},
		}
		if t.ClientSecret != "" {
			out.Client.OperatorClientSecretRef = &corev1.LocalObjectReference{Name: t.ClientSecret}
		}
		if t.ServerTrustedCASecret != "" && t.ServerTrustedCASecret != t.ServerSecret {
			warns = append(warns, fmt.Sprintf(
				"merge ca.crt from secret %q into secret %q before starting the new operator: v1alpha2 reads the client-plane CA from the server secret's ca.crt",
				t.ServerTrustedCASecret, t.ServerSecret))
		}
		if t.ClientTrustedCASecret != "" && t.ClientTrustedCASecret != t.ServerSecret {
			warns = append(warns, fmt.Sprintf(
				"secret %q (clientTrustedCASecret) is dropped: v1alpha2 uses the server secret's ca.crt as etcd's --trusted-ca-file; merge the CA into %q's ca.crt if client certs are signed by it",
				t.ClientTrustedCASecret, t.ServerSecret))
		}
	} else if t.ClientSecret != "" || t.ServerTrustedCASecret != "" || t.ClientTrustedCASecret != "" {
		warns = append(warns,
			"client-plane TLS secrets are dropped: legacy spec sets client-plane material without serverSecret, which enabled nothing in the legacy operator either")
	}

	if t.PeerSecret != "" {
		out.Peer = &lll.PeerTLS{SecretRef: &corev1.LocalObjectReference{Name: t.PeerSecret}}
		if t.PeerTrustedCASecret != "" && t.PeerTrustedCASecret != t.PeerSecret {
			warns = append(warns, fmt.Sprintf(
				"merge ca.crt from secret %q into secret %q before starting the new operator: v1alpha2 reads the peer CA from the peer secret's ca.crt",
				t.PeerTrustedCASecret, t.PeerSecret))
		}
	} else if t.PeerTrustedCASecret != "" {
		warns = append(warns,
			"peerTrustedCASecret without peerSecret is dropped: it enabled nothing in the legacy operator either")
	}

	if out.Client == nil && out.Peer == nil {
		return nil, warns
	}
	return out, warns
}

// translateAuth maps enableAuth onto the new BYO-credentials model. The
// legacy operator provisioned root with NoPassword (cert-only); the new one
// requires a password Secret, so one is referenced or generated here.
func translateAuth(sec *legacy.SecuritySpec, out *lll.EtcdCluster, plan *ResourcePlan, opts TranslateOptions) {
	if sec == nil || !sec.EnableAuth {
		return
	}
	if out.Spec.TLS == nil || out.Spec.TLS.Client == nil {
		plan.Errors = append(plan.Errors,
			"security.enableAuth=true requires client TLS in v1alpha2 (spec.auth.enabled demands spec.tls.client — credentials must not cross a plaintext wire), but the legacy cluster has no serverSecret")
		return
	}

	secretName := opts.AuthSecretName
	if secretName == "" {
		secretName = out.Name + "-root-credentials"
		password := randomPassword()
		plan.Extras = append(plan.Extras, &corev1.Secret{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: out.Namespace},
			Type:       corev1.SecretTypeBasicAuth,
			StringData: map[string]string{
				corev1.BasicAuthUsernameKey: "root",
				corev1.BasicAuthPasswordKey: password,
			},
		})
		plan.Notes = append(plan.Notes, fmt.Sprintf(
			"generated root-credentials Secret %q: point etcd consumers (e.g. a Kamaji DataStore basicAuth) at it",
			secretName))
	}
	out.Spec.Auth = &lll.AuthSpec{
		Enabled:                  true,
		RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: secretName},
	}

	plan.Notes = append(plan.Notes,
		"the tool disables auth on the legacy etcd (certificate-authenticated) at apply, because the legacy root user has NoPassword and could never match a credentials Secret; the new operator re-enables auth with the referenced Secret once it adopts the cluster")
	plan.Warnings = append(plan.Warnings,
		"auth is OFF from the moment the tool runs `auth disable` on the legacy etcd until the new operator re-enables it — plan the cutover window accordingly")
}

// translateLocation maps the legacy S3/PVC destination union onto the
// (field-for-field identical) v1alpha2 SnapshotLocation.
func translateLocation(d legacy.BackupDestination) (lll.SnapshotLocation, error) {
	switch {
	case d.S3 != nil && d.PVC != nil:
		return lll.SnapshotLocation{}, fmt.Errorf("both s3 and pvc destinations set; exactly one is allowed")
	case d.S3 != nil:
		return lll.SnapshotLocation{S3: &lll.S3SnapshotLocation{
			Endpoint:             d.S3.Endpoint,
			Bucket:               d.S3.Bucket,
			Key:                  d.S3.Key,
			CredentialsSecretRef: d.S3.CredentialsSecretRef,
			Region:               d.S3.Region,
			ForcePathStyle:       d.S3.ForcePathStyle,
		}}, nil
	case d.PVC != nil:
		return lll.SnapshotLocation{PVC: &lll.PVCSnapshotLocation{
			ClaimName: d.PVC.ClaimName,
			SubPath:   d.PVC.SubPath,
		}}, nil
	default:
		return lll.SnapshotLocation{}, fmt.Errorf("neither s3 nor pvc destination set; exactly one is required")
	}
}

// TranslateBackup converts one legacy EtcdBackup into an EtcdSnapshot plan
// entry. The specs are field-for-field compatible.
func TranslateBackup(name, namespace string, spec legacy.EtcdBackupSpec) ResourcePlan {
	plan := ResourcePlan{
		SourceKind: "EtcdBackup",
		SourceName: name,
		Namespace:  namespace,
		Action:     ActionCreate,
		DeleteRef:  &ObjectRef{GVR: BackupGVR, Namespace: namespace, Name: name},
	}
	dest, err := translateLocation(spec.Destination)
	if err != nil {
		plan.Action = ActionError
		plan.DeleteRef = nil
		plan.Errors = append(plan.Errors, "spec.destination: "+err.Error())
		return plan
	}
	plan.Target = &lll.EtcdSnapshot{
		TypeMeta:   metav1.TypeMeta{APIVersion: lll.GroupVersion.String(), Kind: "EtcdSnapshot"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: lll.EtcdSnapshotSpec{
			ClusterRef:  spec.ClusterRef,
			Destination: dest,
		},
	}
	plan.Notes = append(plan.Notes,
		"the EtcdSnapshot runs once the NEW operator is started and the referenced cluster exists under the new API")
	return plan
}

// randomPassword returns a 32-hex-char cryptographically random password.
func randomPassword() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing means the platform's entropy source is
		// broken; generating a weak password silently is worse than dying.
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}
