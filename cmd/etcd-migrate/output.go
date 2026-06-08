/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"fmt"
	"io"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/cozystack/etcd-operator/internal/migrate"
)

// render prints the full plan: per-resource action, errors/warnings/notes,
// and the manifests the tool will create (or, for schedules, the manifests
// the user applies themselves).
func render(w io.Writer, plans []migrate.ResourcePlan) {
	for i := range plans {
		p := &plans[i]
		fmt.Fprintf(w, "── %s %s/%s → %s ──\n", p.SourceKind, p.Namespace, p.SourceName, p.Action)
		for _, e := range p.Errors {
			fmt.Fprintf(w, "  ERROR: %s\n", e)
		}
		for _, warn := range p.Warnings {
			fmt.Fprintf(w, "  warning: %s\n", warn)
		}
		for _, note := range p.Notes {
			fmt.Fprintf(w, "  note: %s\n", note)
		}
		if p.Action == migrate.ActionAdopt && p.Adoption != nil {
			a := p.Adoption
			fmt.Fprintf(w, "  steps (pods are never restarted):\n")
			fmt.Fprintf(w, "    1. create EtcdCluster (status prefilled: clusterID=%s) and %d EtcdMember CRs (with reserved annotations)\n",
				a.ClusterStatus.ClusterID, len(a.Members))
			fmt.Fprintf(w, "    2. owner-reference legacy headless Service %q to the adopted members (auto-GCs as they roll)\n",
				a.HeadlessServiceName)
			fmt.Fprintf(w, "    3. orphan-delete legacy EtcdCluster %s/%s and StatefulSet %q (children survive)\n",
				p.Namespace, p.SourceName, a.StatefulSetName)
			fmt.Fprintf(w, "    4. delete legacy ConfigMap %q and PodDisruptionBudget %q\n", a.ConfigMapName, a.PDBName)
			fmt.Fprintf(w, "    5. re-own + label each member's Pod and PVC\n")
			fmt.Fprintf(w, "    6. replace legacy client Service %q in place with the operator's native headless Service of the same name\n",
				a.ClientServiceName)
			for _, extra := range p.Extras {
				renderManifest(w, extra)
			}
			renderManifest(w, p.Target)
			for _, ma := range a.Members {
				renderManifest(w, ma.Member)
			}
			fmt.Fprintln(w)
			continue
		}
		if p.DeleteRef != nil {
			fmt.Fprintf(w, "  cleanup: delete legacy %s %s/%s\n", p.DeleteRef.GVR.Resource, p.DeleteRef.Namespace, p.DeleteRef.Name)
		}
		if p.Action == migrate.ActionCreate || p.Action == migrate.ActionPrint {
			for _, extra := range p.Extras {
				renderManifest(w, extra)
			}
			if p.Target != nil {
				renderManifest(w, p.Target)
			}
		}
		fmt.Fprintln(w)
	}
}

// renderManifest prints one object as a `---`-separated YAML document.
func renderManifest(w io.Writer, obj client.Object) {
	data, err := yaml.Marshal(obj)
	if err != nil {
		fmt.Fprintf(w, "  (failed to render %T: %v)\n", obj, err)
		return
	}
	fmt.Fprintln(w, "---")
	_, _ = w.Write(data)
}

// printCRDNotice reminds about the one cleanup step the tool never performs.
func printCRDNotice(w io.Writer) {
	fmt.Fprintln(w, `
NOTE: the legacy CRDs are not removed by this tool. Once no etcd.aenix.io
CRs remain (check EtcdBackupSchedules — they are never auto-deleted), remove
the CRDs manually:

  kubectl delete crd etcdclusters.etcd.aenix.io etcdbackups.etcd.aenix.io etcdbackupschedules.etcd.aenix.io`)
}
