/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package factory

import (
	"context"
	"fmt"

	"github.com/aenix-io/etcd-operator/internal/log"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// contextWithGVK returns a new context with the GroupVersionKind (GVK) information
// of the given resource added to the context values. It uses the provided scheme
// to determine the GVK.
// If the resource is nil, it returns an error with the message "resource cannot be nil".
// If there is an error while obtaining the GVK, it returns an error with the message
// "failed to get GVK" followed by the detailed error message.
// The context value is updated with the following key-value pairs:
// - "group": GVK's GroupVersion string
// - "kind": GVK's Kind
// - "name": Resource's name
func contextWithGVK(ctx context.Context, resource client.Object, scheme *runtime.Scheme) (context.Context, error) {
	if resource == nil {
		return nil, fmt.Errorf("resource cannot be nil")
	}
	gvk, err := apiutil.GVKForObject(resource, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to get GVK: %w", err)
	}
	ctx = log.WithValues(ctx, "group", gvk.GroupVersion().String(), "kind", gvk.Kind, "name", resource.GetName())
	return ctx, nil
}
