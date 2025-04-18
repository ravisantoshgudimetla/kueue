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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"sigs.k8s.io/kueue/pkg/controller/constants"
	testingdeployment "sigs.k8s.io/kueue/pkg/util/testingjobs/deployment"
)

const (
	TestNamespace = "ns"
)

func TestMultiKueueAdapter(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add apps/v1 to scheme: %v", err)
	}

	baseDeployment := testingdeployment.MakeDeployment("deployment", TestNamespace).
		Label(colocationLabel, "true").
		Paused(false).
		Obj()

	// Ignore ResourceVersion in comparisons
	ignoreResourceVersion := cmpopts.IgnoreFields(appsv1.Deployment{}, "ObjectMeta.ResourceVersion")

	testCases := map[string]struct {
		localDeployment      *appsv1.Deployment
		remoteDeployment     *appsv1.Deployment
		wantErr              error
		wantLocalDeployment  *appsv1.Deployment
		wantRemoteDeployment *appsv1.Deployment
		wantManaged          bool
		wantMessage          string
	}{
		"create missing remote deployment": {
			localDeployment:     baseDeployment.DeepCopy(),
			wantLocalDeployment: baseDeployment.DeepCopy(),
			wantRemoteDeployment: testingdeployment.MakeDeployment("deployment", TestNamespace).
				Label(colocationLabel, "true").
				Label(constants.PrebuiltWorkloadLabel, "workload-key").
				Label(kueuealpha.MultiKueueOriginLabel, "origin").
				Paused(false).
				Obj(),
			wantManaged: true,
		},
		"sync status from remote deployment": {
			localDeployment: baseDeployment.DeepCopy(),
			remoteDeployment: testingdeployment.MakeDeployment("deployment", TestNamespace).
				Label(colocationLabel, "true").
				Label(constants.PrebuiltWorkloadLabel, "workload-key").
				Label(kueuealpha.MultiKueueOriginLabel, "origin").
				Paused(false).
				SetStatus(appsv1.DeploymentStatus{
					Replicas:        1,
					ReadyReplicas:   1,
					UpdatedReplicas: 1,
				}).
				Obj(),
			wantLocalDeployment: testingdeployment.MakeDeployment("deployment", TestNamespace).
				Label(colocationLabel, "true").
				Paused(false).
				SetStatus(appsv1.DeploymentStatus{
					Replicas:        1,
					ReadyReplicas:   1,
					UpdatedReplicas: 1,
				}).
				Obj(),
			wantRemoteDeployment: testingdeployment.MakeDeployment("deployment", TestNamespace).
				Label(colocationLabel, "true").
				Label(constants.PrebuiltWorkloadLabel, "workload-key").
				Label(kueuealpha.MultiKueueOriginLabel, "origin").
				Paused(false).
				SetStatus(appsv1.DeploymentStatus{
					Replicas:        1,
					ReadyReplicas:   1,
					UpdatedReplicas: 1,
				}).
				Obj(),
			wantManaged: true,
		},
		"missing deployment not managed": {
			wantManaged: false,
		},
		"deployment without colocation label not managed": {
			localDeployment: testingdeployment.MakeDeployment("deployment", TestNamespace).Obj(),
			wantManaged:     false,
			wantMessage:     "deployment ns/deployment does not have colocation label kueue.x-k8s.io/multikueue-colocation=true",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			localBuilder := fake.NewClientBuilder().WithScheme(scheme)
			remoteBuilder := fake.NewClientBuilder().WithScheme(scheme)

			if tc.localDeployment != nil {
				localBuilder = localBuilder.WithObjects(tc.localDeployment).WithStatusSubresource(tc.localDeployment)
			}
			if tc.remoteDeployment != nil {
				remoteBuilder = remoteBuilder.WithObjects(tc.remoteDeployment).WithStatusSubresource(tc.remoteDeployment)
			}

			localClient := localBuilder.Build()
			remoteClient := remoteBuilder.Build()

			adapter := &deploymentAdapter{localClient: localClient}
			key := types.NamespacedName{Name: "deployment", Namespace: TestNamespace}
			managed, msg, err := adapter.IsJobManagedByKueue(ctx, localClient, key)
			if diff := cmp.Diff(tc.wantErr, err); managed && diff != "" {
				t.Errorf("Unexpected error (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantManaged, managed); diff != "" {
				t.Errorf("Unexpected managed status (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantMessage, msg); diff != "" {
				t.Errorf("Unexpected message (-want,+got):\n%s", diff)
			}
			if tc.localDeployment == nil {
				return
			}

			err = adapter.SyncJob(ctx, localClient, remoteClient, key, "workload-key", "origin")
			if diff := cmp.Diff(tc.wantErr, err); diff != "" {
				t.Errorf("Unexpected error (-want,+got):\n%s", diff)
			}

			// Check local deployment
			localDeployment := &appsv1.Deployment{}
			if err := localClient.Get(ctx, key, localDeployment); err != nil {
				t.Errorf("Failed to get local deployment: %v", err)
			}
			if diff := cmp.Diff(tc.wantLocalDeployment, localDeployment, ignoreResourceVersion, cmpopts.EquateEmpty()); managed && diff != "" {
				t.Errorf("Unexpected local deployment (-want,+got):\n%s", diff)
			}

			// Check remote deployment
			if tc.wantRemoteDeployment != nil {
				remoteDeployment := &appsv1.Deployment{}
				if err := remoteClient.Get(ctx, key, remoteDeployment); err != nil {
					t.Errorf("Failed to get remote deployment: %v", err)
				}
				if diff := cmp.Diff(tc.wantRemoteDeployment.Status, remoteDeployment.Status); diff != "" {
					t.Errorf("Unexpected deployment status (-want,+got):\n%s", diff)
				}
			}
		})
	}
}
