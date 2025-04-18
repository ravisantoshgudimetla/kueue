/*
Copyright The Kubernetes Authors.

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

package deployment

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	ctrlconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
)

type deploymentAdapter struct {
	localClient client.Client
}

var _ jobframework.MultiKueueAdapter = (*deploymentAdapter)(nil)

func (a *deploymentAdapter) SyncJob(ctx context.Context, localClient client.Client, remoteClient client.Client, key types.NamespacedName, workloadName, origin string) error {
	// Get the local deployment
	localDeployment := &appsv1.Deployment{}
	if err := localClient.Get(ctx, key, localDeployment); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	// Skip if the deployment is being deleted
	if !localDeployment.DeletionTimestamp.IsZero() {
		return nil
	}
	// Get the remote deployment
	remoteDeployment := &appsv1.Deployment{}
	err := remoteClient.Get(ctx, key, remoteDeployment)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Create new remote deployment
		remoteDeployment = localDeployment.DeepCopy()
		remoteDeployment.SetResourceVersion("")
		remoteDeployment.Spec.Paused = false // Always ensure remote deployment is not paused
		if remoteDeployment.Labels == nil {
			remoteDeployment.Labels = make(map[string]string)
		}
		remoteDeployment.Labels[ctrlconstants.PrebuiltWorkloadLabel] = workloadName
		remoteDeployment.Labels[kueue.MultiKueueOriginLabel] = origin
		if err := remoteClient.Create(ctx, remoteDeployment); err != nil {
			return err
		}
	} else {
		// Update local deployment status from remote
		localDeployment.Status = remoteDeployment.Status
		if err := localClient.Status().Update(ctx, localDeployment); err != nil {
			return err
		}
	}

	return nil
}

func (a *deploymentAdapter) DeleteRemoteObject(ctx context.Context, c client.Client, key types.NamespacedName) error {
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, key, deployment); err != nil {
		return client.IgnoreNotFound(err)
	}

	// Only handle deployments with colocation label
	if deployment.Labels[colocationLabel] != "true" {
		return nil
	}

	return client.IgnoreNotFound(c.Delete(ctx, deployment))
}

func (a *deploymentAdapter) KeepAdmissionCheckPending() bool {
	return false
}

func (a *deploymentAdapter) IsJobManagedByKueue(ctx context.Context, c client.Client, key types.NamespacedName) (bool, string, error) {
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, key, deployment); err != nil {
		return false, "", err
	}

	// Only handle deployments with colocation label
	if deployment.Labels[colocationLabel] != "true" {
		return false, fmt.Sprintf("deployment %s does not have colocation label %s=true", key.String(), colocationLabel), nil
	}

	return true, "", nil
}

func (a *deploymentAdapter) GVK() schema.GroupVersionKind {
	return deploymentGVK
}

func (a *deploymentAdapter) GetEmptyList() client.ObjectList {
	return &appsv1.DeploymentList{}
}

func (a *deploymentAdapter) WorkloadKeyFor(job client.Object) types.NamespacedName {
	deployment := job.(*appsv1.Deployment)

	// Only handle deployments with colocation label
	if deployment.Labels[colocationLabel] != "true" {
		return types.NamespacedName{}
	}

	return types.NamespacedName{
		Name:      deployment.Name,
		Namespace: deployment.Namespace,
	}
}
