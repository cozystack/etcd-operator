/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// envMap flattens a container's env into name→value (plain values only) and
// name→secret-key for ValueFrom.SecretKeyRef entries.
func envMap(env []corev1.EnvVar) (vals map[string]string, secretKeys map[string][2]string) {
	vals = map[string]string{}
	secretKeys = map[string][2]string{}
	for _, e := range env {
		if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			secretKeys[e.Name] = [2]string{e.ValueFrom.SecretKeyRef.Name, e.ValueFrom.SecretKeyRef.Key}
			continue
		}
		vals[e.Name] = e.Value
	}
	return
}

func mountByName(mounts []corev1.VolumeMount, name string) (corev1.VolumeMount, bool) {
	for _, m := range mounts {
		if m.Name == name {
			return m, true
		}
	}
	return corev1.VolumeMount{}, false
}

func volumeByName(vols []corev1.Volume, name string) (corev1.Volume, bool) {
	for _, v := range vols {
		if v.Name == name {
			return v, true
		}
	}
	return corev1.Volume{}, false
}

func s3Backup(name, cluster string) *lll.EtcdBackup {
	return &lll.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID("uid-" + name)},
		Spec: lll.EtcdBackupSpec{
			ClusterRef: corev1.LocalObjectReference{Name: cluster},
			Destination: lll.BackupDestination{
				S3: &lll.S3BackupDestination{
					Endpoint:             "https://minio.svc:9000",
					Bucket:               "etcd",
					Key:                  "backups",
					Region:               "us-east-1",
					ForcePathStyle:       true,
					CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
				},
			},
		},
	}
}

func TestBuildBackupJob_S3(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	job := buildBackupJob(s3Backup("b1", "c1"), cluster, "operator:latest")

	if job.Name != "b1-backup" {
		t.Errorf("job name = %q, want b1-backup", job.Name)
	}
	if job.Namespace != "ns" {
		t.Errorf("job namespace = %q, want ns", job.Namespace)
	}
	if job.Labels[LabelCluster] != "c1" {
		t.Errorf("job cluster label = %q, want c1", job.Labels[LabelCluster])
	}

	// Owned/GC settings.
	if job.Spec.TTLSecondsAfterFinished == nil {
		t.Fatal("TTLSecondsAfterFinished not set")
	}
	// A wall-clock bound is required so a Pod that HANGS (e.g. dialing a parked
	// cluster) is killed and the Job goes Failed, rather than wedging the backup
	// in Started forever — BackoffLimit only catches Pods that exit non-zero.
	if job.Spec.ActiveDeadlineSeconds == nil {
		t.Fatal("ActiveDeadlineSeconds not set: a hung backup Pod would never fail the Job")
	}
	pod := job.Spec.Template.Spec
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restart policy = %q, want Never", pod.RestartPolicy)
	}
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Error("AutomountServiceAccountToken must be explicitly false")
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot {
		t.Error("pod must run as non-root")
	}

	if len(pod.Containers) != 1 {
		t.Fatalf("want 1 container, got %d", len(pod.Containers))
	}
	ctr := pod.Containers[0]
	if ctr.Image != "operator:latest" {
		t.Errorf("image = %q, want operator:latest", ctr.Image)
	}
	if got, want := ctr.Command, []string{"/manager", "backup-agent"}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("command = %v, want %v", got, want)
	}

	vals, secretKeys := envMap(ctr.Env)
	if vals["BACKUP_DEST_KIND"] != "s3" {
		t.Errorf("BACKUP_DEST_KIND = %q, want s3", vals["BACKUP_DEST_KIND"])
	}
	if vals["BACKUP_NAME"] != "b1" {
		t.Errorf("BACKUP_NAME = %q, want b1", vals["BACKUP_NAME"])
	}
	if vals["BACKUP_UID"] != "uid-b1" {
		t.Errorf("BACKUP_UID = %q, want uid-b1 (object-ownership stamp)", vals["BACKUP_UID"])
	}
	if vals["S3_BUCKET"] != "etcd" || vals["S3_KEY"] != "backups" {
		t.Errorf("S3 bucket/key = %q/%q", vals["S3_BUCKET"], vals["S3_KEY"])
	}
	if vals["S3_FORCE_PATH_STYLE"] != "true" {
		t.Errorf("S3_FORCE_PATH_STYLE = %q, want true", vals["S3_FORCE_PATH_STYLE"])
	}
	if vals["ETCD_ENDPOINTS"] != "http://c1-client.ns.svc:2379" {
		t.Errorf("ETCD_ENDPOINTS = %q", vals["ETCD_ENDPOINTS"])
	}
	if got, ok := secretKeys["AWS_ACCESS_KEY_ID"]; !ok || got != [2]string{"s3-creds", "AWS_ACCESS_KEY_ID"} {
		t.Errorf("AWS_ACCESS_KEY_ID secret ref = %v", got)
	}
	if got, ok := secretKeys["AWS_SECRET_ACCESS_KEY"]; !ok || got != [2]string{"s3-creds", "AWS_SECRET_ACCESS_KEY"} {
		t.Errorf("AWS_SECRET_ACCESS_KEY secret ref = %v", got)
	}

	// Plaintext, no-auth cluster: no TLS/auth env, no PVC volume.
	if _, ok := vals["ETCD_TLS_CA_PATH"]; ok {
		t.Error("ETCD_TLS_CA_PATH set for plaintext cluster")
	}
	if _, ok := secretKeys["ETCD_PASSWORD"]; ok {
		t.Error("ETCD_PASSWORD set for no-auth cluster")
	}

	// Resources must be set so the Pod isn't BestEffort / rejected by a LimitRange.
	if ctr.Resources.Requests.Cpu().IsZero() || ctr.Resources.Requests.Memory().IsZero() {
		t.Errorf("backup-agent has no resource requests: %+v", ctr.Resources.Requests)
	}
	if ctr.Resources.Limits.Memory().IsZero() {
		t.Errorf("backup-agent has no memory limit: %+v", ctr.Resources.Limits)
	}
}

func TestBuildBackupJob_PVC(t *testing.T) {
	backup := &lll.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec: lll.EtcdBackupSpec{
			ClusterRef:  corev1.LocalObjectReference{Name: "c1"},
			Destination: lll.BackupDestination{PVC: &lll.PVCBackupDestination{ClaimName: "snap-pvc", SubPath: "sub"}},
		},
	}
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	job := buildBackupJob(backup, cluster, "operator:latest")
	pod := job.Spec.Template.Spec
	ctr := pod.Containers[0]

	vals, _ := envMap(ctr.Env)
	if vals["BACKUP_DEST_KIND"] != "pvc" {
		t.Errorf("BACKUP_DEST_KIND = %q, want pvc", vals["BACKUP_DEST_KIND"])
	}
	if vals["PVC_MOUNT_PATH"] != backupPVCMountPath {
		t.Errorf("PVC_MOUNT_PATH = %q, want %q", vals["PVC_MOUNT_PATH"], backupPVCMountPath)
	}
	if vals["PVC_SUBPATH"] != "sub" {
		t.Errorf("PVC_SUBPATH = %q, want sub", vals["PVC_SUBPATH"])
	}

	v, ok := volumeByName(pod.Volumes, "backup-data")
	if !ok || v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != "snap-pvc" {
		t.Errorf("backup-data volume not wired to claim snap-pvc: %+v", v)
	}
	if m, ok := mountByName(ctr.VolumeMounts, "backup-data"); !ok || m.MountPath != backupPVCMountPath {
		t.Errorf("backup-data mount = %+v", m)
	}
}

func TestBuildBackupJob_TLSAndAuth(t *testing.T) {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{
					ServerSecretRef:         &corev1.LocalObjectReference{Name: "c1-server"},
					OperatorClientSecretRef: &corev1.LocalObjectReference{Name: "c1-op-client"},
				},
			},
			Auth: &lll.AuthSpec{
				Enabled:                  true,
				RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "c1-root"},
			},
		},
		Status: lll.EtcdClusterStatus{AuthEnabled: true},
	}
	job := buildBackupJob(s3Backup("b1", "c1"), cluster, "operator:latest")
	pod := job.Spec.Template.Spec
	ctr := pod.Containers[0]
	vals, secretKeys := envMap(ctr.Env)

	// Endpoint must be https now.
	if vals["ETCD_ENDPOINTS"] != "https://c1-client.ns.svc:2379" {
		t.Errorf("ETCD_ENDPOINTS = %q, want https", vals["ETCD_ENDPOINTS"])
	}
	// CA + client cert paths + mounted secrets.
	if vals["ETCD_TLS_CA_PATH"] == "" || vals["ETCD_TLS_CERT_PATH"] == "" || vals["ETCD_TLS_KEY_PATH"] == "" {
		t.Errorf("TLS paths not all set: %+v", vals)
	}
	if v, ok := volumeByName(pod.Volumes, "etcd-ca"); !ok || v.Secret == nil || v.Secret.SecretName != "c1-server" {
		t.Errorf("etcd-ca volume = %+v", v)
	}
	if v, ok := volumeByName(pod.Volumes, "etcd-client"); !ok || v.Secret == nil || v.Secret.SecretName != "c1-op-client" {
		t.Errorf("etcd-client volume = %+v", v)
	}
	// Auth: root username + password from secret.
	if vals["ETCD_USERNAME"] != "root" {
		t.Errorf("ETCD_USERNAME = %q, want root", vals["ETCD_USERNAME"])
	}
	if got, ok := secretKeys["ETCD_PASSWORD"]; !ok || got[0] != "c1-root" {
		t.Errorf("ETCD_PASSWORD secret ref = %v", got)
	}
}

// When auth is configured on the spec but status.authEnabled is still false,
// the agent must NOT be given credentials (auth isn't live yet).
func TestBuildBackupJob_AuthNotYetEnabled(t *testing.T) {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Auth: &lll.AuthSpec{
				Enabled:                  true,
				RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "c1-root"},
			},
		},
		Status: lll.EtcdClusterStatus{AuthEnabled: false},
	}
	job := buildBackupJob(s3Backup("b1", "c1"), cluster, "operator:latest")
	_, secretKeys := envMap(job.Spec.Template.Spec.Containers[0].Env)
	if _, ok := secretKeys["ETCD_PASSWORD"]; ok {
		t.Error("credentials passed before status.authEnabled latched")
	}
}
