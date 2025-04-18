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

	appsv1 "k8s.io/api/apps/v1"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/kueue/pkg/constants"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/controller/jobframework/webhook"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	"sigs.k8s.io/kueue/pkg/queue"
)

// Webhook implements admission.CustomDefaulter and admission.CustomValidator for Deployment objects.
type Webhook struct {
	client                       client.Client
	manageJobsWithoutQueueName   bool
	managedJobsNamespaceSelector labels.Selector
	queues                       *queue.Manager
}

func SetupWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.ProcessOptions(opts...)
	wh := &Webhook{
		client:                       mgr.GetClient(),
		manageJobsWithoutQueueName:   options.ManageJobsWithoutQueueName,
		managedJobsNamespaceSelector: options.ManagedJobsNamespaceSelector,
		queues:                       options.Queues,
	}
	obj := &appsv1.Deployment{}
	return webhook.WebhookManagedBy(mgr).
		For(obj).
		WithMutationHandler(admission.WithCustomDefaulter(mgr.GetScheme(), obj, wh)).
		WithValidator(wh).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-apps-v1-deployment,mutating=true,failurePolicy=fail,sideEffects=None,groups="apps",resources=deployments,verbs=create;update,versions=v1,name=mdeployment.kb.io,admissionReviewVersions=v1

var _ admission.CustomDefaulter = &Webhook{}

// Default implements admission.CustomDefaulter.
func (wh *Webhook) Default(ctx context.Context, obj runtime.Object) error {
	deployment := fromObject(obj)

	log := ctrl.LoggerFrom(ctx).WithName("deployment-webhook")
	log.V(5).Info("Propagating queue-name")

	jobframework.ApplyDefaultLocalQueue(deployment.Object(), wh.queues.DefaultLocalQueueExist)
	suspend, err := jobframework.WorkloadShouldBeSuspended(ctx, deployment.Object(), wh.client, wh.manageJobsWithoutQueueName, wh.managedJobsNamespaceSelector)
	if err != nil {
		return err
	}
	if suspend {
		if deployment.Spec.Template.Annotations == nil {
			deployment.Spec.Template.Annotations = make(map[string]string, 1)
		}
		deployment.Spec.Template.Annotations[podconstants.SuspendedByParentAnnotation] = FrameworkName
		if deployment.Spec.Template.Labels == nil {
			deployment.Spec.Template.Labels = make(map[string]string, 1)
		}
		deployment.Spec.Template.Labels[constants.ManagedByKueueLabelKey] = constants.ManagedByKueueLabelValue
		queueName := jobframework.QueueNameForObject(deployment.Object())
		if queueName != "" {
			deployment.Spec.Template.Labels[controllerconstants.QueueLabel] = queueName
		}
		if priorityClass := jobframework.WorkloadPriorityClassName(deployment.Object()); priorityClass != "" {
			deployment.Spec.Template.Labels[controllerconstants.WorkloadPriorityClassLabel] = priorityClass
		}
		// If this is a colocated deployment, ensure it's paused
		if deployment.Labels[colocationLabel] == "true" {
			deployment.Spec.Paused = true
		}
	}
	return nil
}

// ValidateCreate implements admission.CustomValidator.
func (wh *Webhook) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	deployment := fromObject(obj)

	log := ctrl.LoggerFrom(ctx).WithName("deployment-webhook")
	log.V(5).Info("Validating create")

	var allErrs field.ErrorList
	if hasColocationLabel(deployment) {
		if _, exists := deployment.GetLabels()[constants.QueueLabel]; !exists {
			allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("labels").Key(constants.QueueLabel),
				"deployments with colocation label must specify a queue name"))
		}
	}

	// Validate queue name format if it exists
	allErrs = append(allErrs, jobframework.ValidateQueueName(deployment.Object())...)

	return nil, allErrs.ToAggregate()
}

var (
	labelsPath         = field.NewPath("metadata", "labels")
	queueNameLabelPath = labelsPath.Key(controllerconstants.QueueLabel)
)

// ValidateUpdate implements webhook.Validator.
func (wh *Webhook) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (warnings admission.Warnings, err error) {
	oldDeployment := fromObject(oldObj)
	newDeployment := fromObject(newObj)

	log := ctrl.LoggerFrom(ctx).WithName("deployment-webhook")
	log.V(5).Info("Validating update")

	oldQueueName := jobframework.QueueNameForObject(oldDeployment.Object())
	newQueueName := jobframework.QueueNameForObject(newDeployment.Object())

	allErrs := jobframework.ValidateQueueName(newDeployment.Object())
	allErrs = append(allErrs, jobframework.ValidateUpdateForWorkloadPriorityClassName(oldDeployment.Object(), newDeployment.Object())...)

	// Prevents updating the queue-name if at least one Pod is not suspended
	// or if the queue-name has been deleted.
	if oldDeployment.Status.ReadyReplicas > 0 || newQueueName == "" {
		allErrs = append(allErrs, apivalidation.ValidateImmutableField(oldQueueName, newQueueName, queueNameLabelPath)...)
	}

	// Handle deployments with colocation label
	if oldDeployment.Labels[colocationLabel] == "true" {
		allErrs = append(allErrs, validateUpdateForColocatedDeployments(oldDeployment, newDeployment)...)
	}
	return warnings, allErrs.ToAggregate()
}

func validateUpdateForColocatedDeployments(oldDeployment, newDeployment *Deployment) field.ErrorList {
	allErrs := field.ErrorList{}
	// Prevent removing the colocation label
	if oldDeployment.Labels[colocationLabel] == "true" && newDeployment.Labels[colocationLabel] != "true" {
		return apivalidation.ValidateImmutableField(oldDeployment.Labels[colocationLabel], newDeployment.Labels[colocationLabel], labelsPath.Key(colocationLabel))
	}
	// Prevent scaling operations for deployments managed by MultiKueue
	if oldDeployment.Spec.Replicas != nil && newDeployment.Spec.Replicas != nil &&
		*oldDeployment.Spec.Replicas != *newDeployment.Spec.Replicas {
		return apivalidation.ValidateImmutableField(*oldDeployment.Spec.Replicas, *newDeployment.Spec.Replicas, field.NewPath("spec", "replicas"))
	}
	// Prevent unpausing the deployment
	if oldDeployment.Spec.Paused && !newDeployment.Spec.Paused {
		return apivalidation.ValidateImmutableField(oldDeployment.Spec.Paused, newDeployment.Spec.Paused, field.NewPath("spec", "paused"))
	}
	return allErrs
}

// ValidateDelete implements webhook.Validator.
func (wh *Webhook) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func hasColocationLabel(deployment *Deployment) bool {
	val, exists := deployment.Labels[colocationLabel]
	return exists && val == "true"
}
