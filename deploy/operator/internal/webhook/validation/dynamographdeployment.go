/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package validation

import (
	"context"
	"fmt"
	"os"
	"sort"

	semver "github.com/Masterminds/semver/v3"
	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/consts"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/dra"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/dynamo"
	internalwebhook "github.com/ai-dynamo/dynamo/deploy/operator/internal/webhook"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// DynamoGraphDeploymentValidator validates v1beta1 DynamoGraphDeployment resources.
type DynamoGraphDeploymentValidator struct {
	mgr          ctrl.Manager
	groveEnabled bool
}

// NewDynamoGraphDeploymentValidator creates a validator for v1beta1 DynamoGraphDeployment.
func NewDynamoGraphDeploymentValidator(
	mgr ctrl.Manager,
	groveEnabled bool,
) *DynamoGraphDeploymentValidator {
	return &DynamoGraphDeploymentValidator{
		mgr:          mgr,
		groveEnabled: groveEnabled,
	}
}

// dynamoGraphDeploymentValidation carries immutable request-wide dependencies.
// API values and derived traversal state remain explicit validator arguments.
type dynamoGraphDeploymentValidation struct {
	ctx               context.Context
	mgr               ctrl.Manager
	groveEnabled      bool
	userInfo          *authenticationv1.UserInfo
	operatorPrincipal string
}

type dynamoGraphDeploymentSpecValidationOptions struct {
	dgdName                 string
	generation              int64
	grovePathway            bool
	grovePathwayRequirement string
}

// Validate performs stateless validation on the v1beta1 DynamoGraphDeployment.
func (v *DynamoGraphDeploymentValidator) Validate(
	ctx context.Context,
	deployment *nvidiacomv1beta1.DynamoGraphDeployment,
) (admission.Warnings, error) {
	validation := &dynamoGraphDeploymentValidation{
		ctx:          ctx,
		mgr:          v.mgr,
		groveEnabled: v.groveEnabled,
	}

	allErrs := validation.validateDynamoGraphDeployment(deployment)
	if deployment == nil {
		return nil, invalidDynamoGraphDeploymentError(nil, allErrs)
	}
	alpha, err := alphaDynamoGraphDeploymentForValidation(deployment)
	if err != nil {
		return nil, fmt.Errorf("cannot validate preserved v1alpha1 DynamoGraphDeployment fields: %w", err)
	}
	allErrs = append(allErrs, validateV1Alpha1DynamoGraphDeployment(alpha)...)
	warnings := warningsForV1Alpha1DynamoGraphDeployment(alpha)

	return warnings, invalidDynamoGraphDeploymentError(deployment, allErrs)
}

// ValidateUpdate performs stateful validation comparing old and new v1beta1 DGD objects.
// If userInfo is nil, replica changes for DGDSA-enabled components fail closed.
func (v *DynamoGraphDeploymentValidator) ValidateUpdate(
	oldDGD *nvidiacomv1beta1.DynamoGraphDeployment,
	newDGD *nvidiacomv1beta1.DynamoGraphDeployment,
	userInfo *authenticationv1.UserInfo,
	operatorPrincipal string,
) (admission.Warnings, error) {
	validation := &dynamoGraphDeploymentValidation{
		mgr:               v.mgr,
		groveEnabled:      v.groveEnabled,
		userInfo:          userInfo,
		operatorPrincipal: operatorPrincipal,
	}

	warnings := warningsForDynamoGraphDeploymentUpdate(newDGD, oldDGD)
	allErrs := validation.validateDynamoGraphDeploymentUpdate(newDGD, oldDGD)
	return warnings, invalidDynamoGraphDeploymentError(newDGD, allErrs)
}

func (v *dynamoGraphDeploymentValidation) validateDynamoGraphDeployment(
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) field.ErrorList {
	if dgd == nil {
		return field.ErrorList{field.Required(field.NewPath("dynamoGraphDeployment"), "must not be nil")}
	}

	allErrs := field.ErrorList{}
	annotationsPath := field.NewPath("metadata", "annotations")
	if value, exists := dgd.Annotations[consts.KubeAnnotationDynamoOperatorOriginVersion]; exists {
		if _, err := semver.NewVersion(value); err != nil {
			allErrs = append(allErrs, field.Invalid(
				annotationsPath.Key(consts.KubeAnnotationDynamoOperatorOriginVersion),
				value,
				"must be valid semver",
			))
		}
	}
	if value, invalid := invalidVLLMDistributedExecutorBackendAnnotation(dgd.Annotations); invalid {
		allErrs = append(allErrs, field.Invalid(
			annotationsPath.Key(consts.KubeAnnotationVLLMDistributedExecutorBackend),
			value,
			`must be "mp" or "ray"`,
		))
	}
	if value, exists := dgd.Annotations[consts.KubeAnnotationDynamoKubeDiscoveryMode]; exists && value != "pod" && value != "container" {
		allErrs = append(allErrs, field.NotSupported(
			annotationsPath.Key(consts.KubeAnnotationDynamoKubeDiscoveryMode),
			value,
			[]string{"pod", "container"},
		))
	}

	grovePathway, grovePathwayRequirement := v.grovePathwayForDynamoGraphDeployment(dgd)
	specOpts := dynamoGraphDeploymentSpecValidationOptions{
		dgdName:                 dgd.Name,
		generation:              dgd.Generation,
		grovePathway:            grovePathway,
		grovePathwayRequirement: grovePathwayRequirement,
	}
	allErrs = append(allErrs, v.validateDynamoGraphDeploymentSpec(&dgd.Spec, field.NewPath("spec"), specOpts)...)

	if hasIntraPodFailover(&dgd.Spec) && dgd.Annotations[consts.KubeAnnotationDynamoKubeDiscoveryMode] != "container" {
		allErrs = append(allErrs, field.Invalid(
			annotationsPath.Key(consts.KubeAnnotationDynamoKubeDiscoveryMode),
			dgd.Annotations[consts.KubeAnnotationDynamoKubeDiscoveryMode],
			`must be "container" when intra-pod failover is configured`,
		))
	}

	return allErrs
}

func validateV1Alpha1DynamoGraphDeployment(
	dgd *nvidiacomv1alpha1.DynamoGraphDeployment,
) field.ErrorList {
	if dgd == nil || !hasV1Alpha1CompatibilityFields(dgd) {
		return nil
	}
	return validateV1Alpha1DynamoGraphDeploymentSpec(&dgd.Spec, field.NewPath("spec"))
}

func validateV1Alpha1DynamoGraphDeploymentSpec(
	spec *nvidiacomv1alpha1.DynamoGraphDeploymentSpec,
	fldPath *field.Path,
) field.ErrorList {
	allErrs := field.ErrorList{}
	servicesPath := fldPath.Child("services")
	for _, serviceName := range sortedV1Alpha1ServiceNames(spec.Services) {
		service := spec.Services[serviceName]
		servicePath := servicesPath.Key(serviceName)
		if service == nil {
			allErrs = append(allErrs, field.Required(servicePath, "must not be null"))
			continue
		}
		allErrs = append(allErrs, validateV1Alpha1DynamoComponentDeploymentSharedSpec(service, servicePath)...)
	}
	return allErrs
}

func validateV1Alpha1DynamoComponentDeploymentSharedSpec(
	spec *nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec,
	fldPath *field.Path,
) field.ErrorList {
	allErrs := field.ErrorList{}
	if value, invalid := invalidVLLMDistributedExecutorBackendAnnotation(spec.Annotations); invalid {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("annotations").Key(consts.KubeAnnotationVLLMDistributedExecutorBackend),
			value,
			`must be "mp" or "ray"`,
		))
	}

	volumeMountsPath := fldPath.Child("volumeMounts")
	for i := range spec.VolumeMounts {
		allErrs = append(allErrs, validateV1Alpha1VolumeMount(&spec.VolumeMounts[i], volumeMountsPath.Index(i))...)
	}
	if spec.Ingress != nil {
		allErrs = append(allErrs, validateV1Alpha1IngressSpec(spec.Ingress, fldPath.Child("ingress"))...)
	}
	if spec.FrontendSidecar != nil {
		allErrs = append(allErrs, validateV1Alpha1FrontendSidecarSpec(
			spec.FrontendSidecar,
			fldPath.Child("frontendSidecar"),
			spec.ExtraPodSpec,
		)...)
	}
	return allErrs
}

func validateV1Alpha1VolumeMount(
	volumeMount *nvidiacomv1alpha1.VolumeMount,
	fldPath *field.Path,
) field.ErrorList {
	if volumeMount.UseAsCompilationCache || volumeMount.MountPoint != "" {
		return nil
	}
	return field.ErrorList{field.Required(
		fldPath.Child("mountPoint"),
		"is required when useAsCompilationCache is false",
	)}
}

func validateV1Alpha1IngressSpec(
	ingress *nvidiacomv1alpha1.IngressSpec,
	fldPath *field.Path,
) field.ErrorList {
	if !ingress.Enabled || ingress.Host != "" {
		return nil
	}
	return field.ErrorList{field.Required(fldPath.Child("host"), "is required when ingress is enabled")}
}

func validateV1Alpha1FrontendSidecarSpec(
	frontendSidecar *nvidiacomv1alpha1.FrontendSidecarSpec,
	fldPath *field.Path,
	extraPodSpec *nvidiacomv1alpha1.ExtraPodSpec,
) field.ErrorList {
	if frontendSidecar == nil || extraPodSpec == nil || extraPodSpec.PodSpec == nil {
		return nil
	}
	if hasContainerNamed(extraPodSpec.PodSpec.Containers, consts.FrontendSidecarContainerName) {
		return field.ErrorList{field.Invalid(
			fldPath,
			frontendSidecar,
			fmt.Sprintf("cannot inject frontend sidecar: a container named %q already exists in extraPodSpec.containers", consts.FrontendSidecarContainerName),
		)}
	}
	return nil
}

func (v *dynamoGraphDeploymentValidation) validateDynamoGraphDeploymentSpec(
	spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec,
	fldPath *field.Path,
	opts dynamoGraphDeploymentSpecValidationOptions,
) field.ErrorList {
	allErrs := field.ErrorList{}

	if spec.PriorityClassName != "" && !opts.grovePathway {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("priorityClassName"), opts.grovePathwayRequirement))
	}

	componentsPath := fldPath.Child("components")
	if len(spec.Components) == 0 {
		allErrs = append(allErrs, field.Required(componentsPath, "must have at least one component"))
	}
	components := componentsByName(spec.Components)
	for i := range spec.Components {
		component := &spec.Components[i]
		componentPath := componentsPath.Index(i)

		if opts.grovePathway {
			combinedLength, detail := dgdComponentResourceNameLength(opts.dgdName, spec.Components, component)
			if combinedLength > maxCombinedResourceNameLength {
				allErrs = append(allErrs, field.Invalid(
					componentPath.Child("name"),
					component.ComponentName,
					fmt.Sprintf(
						"combined resource name length %d exceeds the %d-character pod-name limit (%s); shorten DynamoGraphDeployment name %q or component name %q",
						combinedLength,
						maxCombinedResourceNameLength,
						detail,
						opts.dgdName,
						component.ComponentName,
					),
				))
			}
		}

		gms := gpuMemoryServiceFor(component)
		if gms != nil && effectiveGMSMode(gms.Mode) == nvidiacomv1beta1.GMSModeInterPod {
			modePath := componentPath.Child("experimental", "gpuMemoryService", "mode")
			if !opts.grovePathway {
				allErrs = append(allErrs, field.Forbidden(modePath, opts.grovePathwayRequirement))
			}
			if spec.BackendFramework != string(dynamo.BackendFrameworkVLLM) {
				detected := spec.BackendFramework
				if detected == "" {
					detected = unsetValue
				}
				allErrs = append(allErrs, field.Invalid(
					modePath,
					gms.Mode,
					fmt.Sprintf("the inter-pod GMS layout is currently supported only for vLLM (detected backend: %s)", detected),
				))
			}
		}

		allErrs = append(allErrs, v.validateDynamoComponentDeploymentSharedSpec(
			component,
			componentPath,
			opts.grovePathway,
		)...)
	}

	if spec.Restart != nil {
		allErrs = append(allErrs, v.validateRestart(spec.Restart, fldPath.Child("restart"), components)...)
	}

	constraintPath := fldPath.Child("topologyConstraint")
	hasAnyConstraint := spec.TopologyConstraint != nil
	for i := range spec.Components {
		if spec.Components[i].TopologyConstraint != nil {
			hasAnyConstraint = true
			break
		}
	}
	if hasAnyConstraint {
		topologyErrs := field.ErrorList{}
		if spec.TopologyConstraint == nil {
			topologyErrs = append(topologyErrs, field.Required(
				constraintPath,
				"is required when any component topology constraint is set",
			))
		} else {
			if spec.TopologyConstraint.PackDomain == "" {
				for i := range spec.Components {
					if spec.Components[i].TopologyConstraint == nil {
						topologyErrs = append(topologyErrs, field.Required(
							componentsPath.Index(i).Child("topologyConstraint"),
							"is required because spec.topologyConstraint.packDomain is not set",
						))
					}
				}
			}

			var topologyInfo *clusterTopologyInfo
			if len(topologyErrs) == 0 && spec.TopologyConstraint.ClusterTopologyName != "" &&
				v.mgr != nil && opts.generation <= 1 && opts.grovePathway {
				var err error
				topologyInfo, err = v.readGroveClusterTopology(spec.TopologyConstraint.ClusterTopologyName)
				if err != nil {
					detail := fmt.Sprintf("failed to read ClusterTopology: %v", err)
					if k8serrors.IsNotFound(err) {
						detail = "references a ClusterTopology resource that was not found"
					}
					topologyErrs = append(topologyErrs, field.Invalid(
						constraintPath.Child("clusterTopologyName"),
						spec.TopologyConstraint.ClusterTopologyName,
						detail,
					))
				}
			}

			topologyErrs = append(topologyErrs, v.validateSpecTopologyConstraint(
				spec.TopologyConstraint,
				constraintPath,
				topologyInfo,
			)...)
			for i := range spec.Components {
				componentConstraint := spec.Components[i].TopologyConstraint
				if componentConstraint == nil {
					continue
				}
				topologyErrs = append(topologyErrs, v.validateTopologyConstraint(
					componentConstraint,
					componentsPath.Index(i).Child("topologyConstraint"),
					spec.TopologyConstraint,
					topologyInfo,
				)...)
			}
		}
		allErrs = append(allErrs, topologyErrs...)
	}

	if spec.Experimental != nil {
		allErrs = append(allErrs, v.validateDynamoGraphDeploymentExperimentalSpec(
			spec.Experimental,
			fldPath.Child("experimental"),
			opts.generation,
			opts.grovePathway,
			opts.grovePathwayRequirement,
		)...)
	}

	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateRestart(
	restart *nvidiacomv1beta1.Restart,
	fldPath *field.Path,
	components map[string]*nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
) field.ErrorList {
	if restart.Strategy == nil {
		return nil
	}
	return v.validateRestartStrategy(restart.Strategy, fldPath.Child("strategy"), components)
}

func (v *dynamoGraphDeploymentValidation) validateRestartStrategy(
	strategy *nvidiacomv1beta1.RestartStrategy,
	fldPath *field.Path,
	components map[string]*nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
) field.ErrorList {
	if len(strategy.Order) == 0 {
		return nil
	}

	orderPath := fldPath.Child("order")
	if strategy.Type == nvidiacomv1beta1.RestartStrategyTypeParallel {
		return field.ErrorList{field.Forbidden(orderPath, "cannot be specified when strategy is parallel")}
	}

	allErrs := field.ErrorList{}
	uniqueOrder := getUnique(strategy.Order)
	if len(uniqueOrder) != len(strategy.Order) {
		allErrs = append(allErrs, field.Invalid(orderPath, strategy.Order, "must be unique"))
	}
	if len(uniqueOrder) != len(components) {
		allErrs = append(allErrs, field.Invalid(
			orderPath,
			strategy.Order,
			"must have the same number of unique components as the deployment",
		))
	}
	for i, componentName := range strategy.Order {
		if _, exists := components[componentName]; !exists {
			allErrs = append(allErrs, field.NotSupported(orderPath.Index(i), componentName, sortedComponentNames(components)))
		}
	}
	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateSpecTopologyConstraint(
	constraint *nvidiacomv1beta1.SpecTopologyConstraint,
	fldPath *field.Path,
	topologyInfo *clusterTopologyInfo,
) field.ErrorList {
	if topologyInfo == nil || constraint.PackDomain == "" {
		return nil
	}
	if _, exists := topologyInfo.domainIndex[string(constraint.PackDomain)]; exists {
		return nil
	}
	return field.ErrorList{field.Invalid(
		fldPath.Child("packDomain"),
		constraint.PackDomain,
		fmt.Sprintf("does not exist in ClusterTopology %q; available domains: %v", topologyInfo.name, topologyInfo.domains),
	)}
}

func (v *dynamoGraphDeploymentValidation) validateTopologyConstraint(
	constraint *nvidiacomv1beta1.TopologyConstraint,
	fldPath *field.Path,
	specConstraint *nvidiacomv1beta1.SpecTopologyConstraint,
	topologyInfo *clusterTopologyInfo,
) field.ErrorList {
	if topologyInfo == nil {
		return nil
	}

	packDomainPath := fldPath.Child("packDomain")
	componentIndex, exists := topologyInfo.domainIndex[string(constraint.PackDomain)]
	if !exists {
		return field.ErrorList{field.Invalid(
			packDomainPath,
			constraint.PackDomain,
			fmt.Sprintf("does not exist in ClusterTopology %q; available domains: %v", topologyInfo.name, topologyInfo.domains),
		)}
	}
	if specConstraint.PackDomain == "" {
		return nil
	}
	specIndex, exists := topologyInfo.domainIndex[string(specConstraint.PackDomain)]
	if exists && componentIndex < specIndex {
		return field.ErrorList{field.Invalid(
			packDomainPath,
			constraint.PackDomain,
			fmt.Sprintf("must be equal to or narrower than the deployment-level domain %q", specConstraint.PackDomain),
		)}
	}
	return nil
}

func (v *dynamoGraphDeploymentValidation) validateDynamoGraphDeploymentExperimentalSpec(
	experimental *nvidiacomv1beta1.DynamoGraphDeploymentExperimentalSpec,
	fldPath *field.Path,
	generation int64,
	grovePathway bool,
	grovePathwayRequirement string,
) field.ErrorList {
	if experimental.KvTransferPolicy == nil {
		return nil
	}
	return v.validateKvTransferPolicy(
		experimental.KvTransferPolicy,
		fldPath.Child("kvTransferPolicy"),
		generation,
		grovePathway,
		grovePathwayRequirement,
	)
}

func (v *dynamoGraphDeploymentValidation) validateKvTransferPolicy(
	policy *nvidiacomv1beta1.KvTransferPolicy,
	fldPath *field.Path,
	generation int64,
	grovePathway bool,
	grovePathwayRequirement string,
) field.ErrorList {
	if policy.ClusterTopologyName == "" {
		return nil
	}

	allErrs := field.ErrorList{}
	namePath := fldPath.Child("clusterTopologyName")
	if !grovePathway {
		allErrs = append(allErrs, field.Forbidden(namePath, grovePathwayRequirement))
	}
	if len(allErrs) != 0 || v.mgr == nil || generation > 1 {
		return allErrs
	}

	topologyInfo, err := v.readGroveClusterTopology(policy.ClusterTopologyName)
	if err != nil {
		detail := fmt.Sprintf("failed to read ClusterTopology: %v", err)
		if k8serrors.IsNotFound(err) {
			detail = "references a ClusterTopology resource that was not found"
		}
		return append(allErrs, field.Invalid(namePath, policy.ClusterTopologyName, detail))
	}
	if _, exists := topologyInfo.domainIndex[string(policy.Domain)]; !exists {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("domain"),
			policy.Domain,
			fmt.Sprintf("does not exist in ClusterTopology %q; available domains: %v", topologyInfo.name, topologyInfo.domains),
		))
	}
	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateDynamoComponentDeploymentSharedSpec(
	spec *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
	fldPath *field.Path,
	grovePathway bool,
) field.ErrorList {
	allErrs := field.ErrorList{}

	if spec.PodTemplate != nil {
		containersPath := fldPath.Child("podTemplate", "spec", "containers")
		for i := range spec.PodTemplate.Spec.Containers {
			container := &spec.PodTemplate.Spec.Containers[i]
			if container.Name != consts.MainContainerName && container.Image == "" {
				allErrs = append(allErrs, field.Required(
					containersPath.Index(i).Child("image"),
					fmt.Sprintf("is required for sidecar container %q", container.Name),
				))
			}
		}

		initContainersPath := fldPath.Child("podTemplate", "spec", "initContainers")
		for i := range spec.PodTemplate.Spec.InitContainers {
			container := &spec.PodTemplate.Spec.InitContainers[i]
			if container.Image == "" {
				allErrs = append(allErrs, field.Required(
					initContainersPath.Index(i).Child("image"),
					fmt.Sprintf("is required for init container %q", container.Name),
				))
			}
		}

		if value, invalid := invalidVLLMDistributedExecutorBackendAnnotation(spec.PodTemplate.Annotations); invalid {
			allErrs = append(allErrs, field.Invalid(
				fldPath.Child("podTemplate", "metadata", "annotations").Key(consts.KubeAnnotationVLLMDistributedExecutorBackend),
				value,
				`must be "mp" or "ray"`,
			))
		}
	}

	if spec.MinAvailable != nil && !grovePathway {
		allErrs = append(allErrs, field.Forbidden(
			fldPath.Child("minAvailable"),
			"is currently supported only for Grove-backed DynamoGraphDeployment components",
		))
	}
	if spec.SharedMemorySize != nil && spec.SharedMemorySize.Sign() < 0 {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("sharedMemorySize"),
			spec.SharedMemorySize.String(),
			"must be non-negative",
		))
	}

	if spec.ComponentType == nvidiacomv1beta1.ComponentTypeEPP {
		if err := v.inferencePoolAvailabilityError(); err != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("type"), fmt.Sprintf("cannot deploy EPP component: %v", err)))
		}
		if spec.IsMultinode() {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("multinode"), "EPP component cannot be multinode"))
		}
		if spec.Replicas != nil && *spec.Replicas != 1 {
			allErrs = append(allErrs, field.Invalid(
				fldPath.Child("replicas"),
				*spec.Replicas,
				"EPP component must have exactly 1 replica",
			))
		}
		if spec.EPPConfig == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("eppConfig"), "is required for EPP components"))
		}
	}

	if spec.FrontendSidecar != nil {
		frontendSidecarPath := fldPath.Child("frontendSidecar")
		if spec.PodTemplate == nil {
			allErrs = append(allErrs, field.Required(
				fldPath.Child("podTemplate", "spec", "containers"),
				"is required when frontendSidecar is set",
			))
		} else if *spec.FrontendSidecar == "" {
			allErrs = append(allErrs, field.Invalid(frontendSidecarPath, *spec.FrontendSidecar, "must not be empty"))
		} else if !hasContainerNamed(spec.PodTemplate.Spec.Containers, *spec.FrontendSidecar) {
			allErrs = append(allErrs, field.Invalid(
				frontendSidecarPath,
				*spec.FrontendSidecar,
				"must match a podTemplate.spec.containers name",
			))
		}
	}

	if spec.Experimental != nil {
		allErrs = append(allErrs, v.validateExperimentalSpec(
			spec.Experimental,
			fldPath.Child("experimental"),
			spec.ComponentType,
			dynamo.GetMainContainerResources(spec),
		)...)
	}

	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateExperimentalSpec(
	experimental *nvidiacomv1beta1.ExperimentalSpec,
	fldPath *field.Path,
	componentType nvidiacomv1beta1.ComponentType,
	resources corev1.ResourceRequirements,
) field.ErrorList {
	allErrs := field.ErrorList{}
	if experimental.GPUMemoryService != nil {
		allErrs = append(allErrs, v.validateGPUMemoryServiceSpec(
			experimental.GPUMemoryService,
			fldPath.Child("gpuMemoryService"),
			componentType,
			resources,
		)...)
	}
	if experimental.Failover != nil {
		allErrs = append(allErrs, v.validateFailoverSpec(
			experimental.Failover,
			fldPath.Child("failover"),
			experimental.GPUMemoryService,
			componentType,
			resources,
		)...)
	}
	if experimental.Checkpoint != nil {
		allErrs = append(allErrs, v.validateComponentCheckpointConfig(
			experimental.Checkpoint,
			fldPath.Child("checkpoint"),
			experimental.GPUMemoryService,
		)...)
	}

	if experimental.Checkpoint != nil && experimental.Checkpoint.Enabled &&
		experimental.GPUMemoryService != nil && os.Getenv(consts.DynamoOperatorAllowGMSSnapshotEnvVar) != "1" {
		allErrs = append(allErrs, field.Forbidden(
			fldPath.Child("checkpoint"),
			"GMS + Snapshot is temporarily disabled; disable gpuMemoryService or enable the internal GMS + Snapshot gate",
		))
	}
	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateGPUMemoryServiceSpec(
	gms *nvidiacomv1beta1.GPUMemoryServiceSpec,
	fldPath *field.Path,
	componentType nvidiacomv1beta1.ComponentType,
	resources corev1.ResourceRequirements,
) field.ErrorList {
	allErrs := field.ErrorList{}
	switch componentType {
	case nvidiacomv1beta1.ComponentTypeWorker,
		nvidiacomv1beta1.ComponentTypePrefill,
		nvidiacomv1beta1.ComponentTypeDecode:
	default:
		allErrs = append(allErrs, field.Forbidden(
			fldPath,
			"GPU memory service is only supported for worker, prefill, or decode components",
		))
	}

	gpuCount, err := dra.ExtractGPUCountFromResourceRequirements(resources)
	if err != nil || gpuCount < 1 {
		allErrs = append(allErrs, field.Invalid(
			fldPath,
			gms,
			"GPU memory service requires podTemplate.spec.containers[main].resources.limits.nvidia.com/gpu >= 1",
		))
	}
	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateFailoverSpec(
	failover *nvidiacomv1beta1.FailoverSpec,
	fldPath *field.Path,
	gms *nvidiacomv1beta1.GPUMemoryServiceSpec,
	componentType nvidiacomv1beta1.ComponentType,
	resources corev1.ResourceRequirements,
) field.ErrorList {
	allErrs := field.ErrorList{}
	failoverMode := effectiveGMSMode(failover.Mode)
	if gms == nil {
		allErrs = append(allErrs, field.Invalid(
			fldPath,
			failover,
			fmt.Sprintf("gpuMemoryService is required when failover mode is %q", failoverMode),
		))
	} else if effectiveGMSMode(gms.Mode) != failoverMode {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("mode"),
			failover.Mode,
			fmt.Sprintf("must match gpuMemoryService.mode %q", gms.Mode),
		))
	}

	if failoverMode == nvidiacomv1beta1.GMSModeInterPod {
		gpuCount, err := dra.ExtractGPUCountFromResourceRequirements(resources)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(
				fldPath,
				failover,
				fmt.Sprintf("failed to read main-container GPU limit: %v", err),
			))
		} else if gpuCount < 1 {
			allErrs = append(allErrs, field.Invalid(
				fldPath,
				failover,
				"GMS failover requires at least 1 GPU in podTemplate.spec.containers[main].resources.limits.nvidia.com/gpu",
			))
		}

		switch componentType {
		case nvidiacomv1beta1.ComponentTypeEPP,
			nvidiacomv1beta1.ComponentTypeFrontend,
			nvidiacomv1beta1.ComponentTypePlanner:
			allErrs = append(allErrs, field.Forbidden(
				fldPath,
				fmt.Sprintf("GMS failover is not supported for component type %q", componentType),
			))
		}
	}
	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateComponentCheckpointConfig(
	checkpoint *nvidiacomv1beta1.ComponentCheckpointConfig,
	fldPath *field.Path,
	gms *nvidiacomv1beta1.GPUMemoryServiceSpec,
) field.ErrorList {
	if checkpoint.Job == nil {
		return nil
	}
	return v.validateComponentCheckpointJobConfig(checkpoint.Job, fldPath.Child("job"), gms)
}

func (v *dynamoGraphDeploymentValidation) validateComponentCheckpointJobConfig(
	job *nvidiacomv1beta1.ComponentCheckpointJobConfig,
	fldPath *field.Path,
	gms *nvidiacomv1beta1.GPUMemoryServiceSpec,
) field.ErrorList {
	if len(job.GMSClientContainers) == 0 {
		return nil
	}
	if gms == nil {
		return field.ErrorList{field.Forbidden(
			fldPath.Child("gmsClientContainers"),
			"requires gpuMemoryService to be set",
		)}
	}
	if effectiveGMSMode(gms.Mode) == nvidiacomv1beta1.GMSModeInterPod {
		return field.ErrorList{field.Forbidden(
			fldPath.Child("gmsClientContainers"),
			"is only supported with gpuMemoryService.mode=IntraPod",
		)}
	}
	return nil
}

func (v *dynamoGraphDeploymentValidation) validateDynamoGraphDeploymentUpdate(
	newDGD *nvidiacomv1beta1.DynamoGraphDeployment,
	oldDGD *nvidiacomv1beta1.DynamoGraphDeployment,
) field.ErrorList {
	allErrs := field.ErrorList{}
	if newDGD == nil {
		allErrs = append(allErrs, field.Required(field.NewPath("newDynamoGraphDeployment"), "must not be nil"))
	}
	if oldDGD == nil {
		allErrs = append(allErrs, field.Required(field.NewPath("oldDynamoGraphDeployment"), "must not be nil"))
	}
	if len(allErrs) != 0 {
		return allErrs
	}

	allErrs = append(allErrs, v.validateDynamoGraphDeploymentSpecUpdate(
		&newDGD.Spec,
		&oldDGD.Spec,
		field.NewPath("spec"),
	)...)

	if oldDGD.Status.RollingUpdate != nil {
		phase := oldDGD.Status.RollingUpdate.Phase
		if phase == nvidiacomv1beta1.RollingUpdatePhasePending || phase == nvidiacomv1beta1.RollingUpdatePhaseInProgress {
			oldID := restartID(oldDGD.Spec.Restart)
			newID := restartID(newDGD.Spec.Restart)
			if oldID != newID {
				allErrs = append(allErrs, field.Invalid(
					field.NewPath("spec", "restart", "id"),
					newID,
					fmt.Sprintf("cannot be changed while a rolling update is %s", phase),
				))
			}
		}
	}
	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateDynamoGraphDeploymentSpecUpdate(
	newSpec *nvidiacomv1beta1.DynamoGraphDeploymentSpec,
	oldSpec *nvidiacomv1beta1.DynamoGraphDeploymentSpec,
	fldPath *field.Path,
) field.ErrorList {
	allErrs := field.ErrorList{}
	newComponents := componentsByName(newSpec.Components)
	oldComponents := componentsByName(oldSpec.Components)

	added := difference(componentNameSet(newComponents), componentNameSet(oldComponents))
	removed := difference(componentNameSet(oldComponents), componentNameSet(newComponents))
	sort.Strings(added)
	sort.Strings(removed)
	if len(added) != 0 || len(removed) != 0 {
		detail := "component topology is immutable and cannot be modified after creation"
		switch {
		case len(added) != 0 && len(removed) != 0:
			detail = fmt.Sprintf("%s: components added: %v, components removed: %v", detail, added, removed)
		case len(added) != 0:
			detail = fmt.Sprintf("%s: components added: %v", detail, added)
		default:
			detail = fmt.Sprintf("%s: components removed: %v", detail, removed)
		}
		allErrs = append(allErrs, field.Invalid(fldPath.Child("components"), newSpec.Components, detail))
	}

	canModifyReplicas := v.userInfo != nil && internalwebhook.CanModifyDGDReplicas(v.operatorPrincipal, *v.userInfo)
	componentsPath := fldPath.Child("components")
	for i := range newSpec.Components {
		newComponent := &newSpec.Components[i]
		oldComponent, exists := oldComponents[newComponent.ComponentName]
		if !exists {
			continue
		}
		allErrs = append(allErrs, v.validateDynamoComponentDeploymentSharedSpecUpdate(
			newComponent,
			oldComponent,
			componentsPath.Index(i),
			canModifyReplicas,
		)...)
	}

	if newSpec.BackendFramework != oldSpec.BackendFramework {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("backendFramework"),
			newSpec.BackendFramework,
			"is immutable and cannot be changed after creation",
		))
	}

	allErrs = append(allErrs, v.validateSpecTopologyConstraintUpdate(
		newSpec.TopologyConstraint,
		oldSpec.TopologyConstraint,
		fldPath.Child("topologyConstraint"),
	)...)

	if newSpec.Experimental != nil || oldSpec.Experimental != nil {
		allErrs = append(allErrs, v.validateDynamoGraphDeploymentExperimentalSpecUpdate(
			newSpec.Experimental,
			oldSpec.Experimental,
			fldPath.Child("experimental"),
		)...)
	}

	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateDynamoComponentDeploymentSharedSpecUpdate(
	newComponent *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
	oldComponent *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
	fldPath *field.Path,
	canModifyReplicas bool,
) field.ErrorList {
	allErrs := field.ErrorList{}
	if newComponent.ScalingAdapter != nil && !canModifyReplicas &&
		effectiveReplicas(newComponent.Replicas) != effectiveReplicas(oldComponent.Replicas) {
		allErrs = append(allErrs, field.Forbidden(
			fldPath.Child("replicas"),
			"cannot be modified directly when scaling adapter is enabled; scale or update the related DynamoGraphDeploymentScalingAdapter instead",
		))
	}

	if newComponent.IsMultinode() != oldComponent.IsMultinode() {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("multinode"),
			newComponent.Multinode,
			"cannot change node topology between single-node and multi-node after creation",
		))
	}

	allErrs = append(allErrs, v.validateTopologyConstraintUpdate(
		newComponent.TopologyConstraint,
		oldComponent.TopologyConstraint,
		fldPath.Child("topologyConstraint"),
	)...)
	if newComponent.Experimental != nil || oldComponent.Experimental != nil {
		allErrs = append(allErrs, v.validateExperimentalSpecUpdate(
			newComponent.Experimental,
			oldComponent.Experimental,
			fldPath.Child("experimental"),
		)...)
	}
	return allErrs
}

func (v *dynamoGraphDeploymentValidation) validateSpecTopologyConstraintUpdate(
	newConstraint *nvidiacomv1beta1.SpecTopologyConstraint,
	oldConstraint *nvidiacomv1beta1.SpecTopologyConstraint,
	fldPath *field.Path,
) field.ErrorList {
	if specTopologyConstraintsEqual(newConstraint, oldConstraint) {
		return nil
	}
	return field.ErrorList{field.Invalid(
		fldPath,
		newConstraint,
		"is immutable and cannot be added, removed, or changed after creation; delete and recreate the DynamoGraphDeployment to change topology constraints",
	)}
}

func (v *dynamoGraphDeploymentValidation) validateTopologyConstraintUpdate(
	newConstraint *nvidiacomv1beta1.TopologyConstraint,
	oldConstraint *nvidiacomv1beta1.TopologyConstraint,
	fldPath *field.Path,
) field.ErrorList {
	if topologyConstraintsEqual(newConstraint, oldConstraint) {
		return nil
	}
	return field.ErrorList{field.Invalid(
		fldPath,
		newConstraint,
		"is immutable and cannot be added, removed, or changed after creation; delete and recreate the DynamoGraphDeployment to change topology constraints",
	)}
}

func (v *dynamoGraphDeploymentValidation) validateDynamoGraphDeploymentExperimentalSpecUpdate(
	newExperimental *nvidiacomv1beta1.DynamoGraphDeploymentExperimentalSpec,
	oldExperimental *nvidiacomv1beta1.DynamoGraphDeploymentExperimentalSpec,
	fldPath *field.Path,
) field.ErrorList {
	return v.validateKvTransferPolicyUpdate(
		kvTransferPolicyFor(newExperimental),
		kvTransferPolicyFor(oldExperimental),
		fldPath.Child("kvTransferPolicy"),
	)
}

func (v *dynamoGraphDeploymentValidation) validateKvTransferPolicyUpdate(
	newPolicy *nvidiacomv1beta1.KvTransferPolicy,
	oldPolicy *nvidiacomv1beta1.KvTransferPolicy,
	fldPath *field.Path,
) field.ErrorList {
	if kvTransferPoliciesEqual(newPolicy, oldPolicy) {
		return nil
	}
	return field.ErrorList{field.Invalid(
		fldPath,
		newPolicy,
		"is immutable and cannot be added, removed, or changed after creation; delete and recreate the DynamoGraphDeployment to change the KV transfer policy",
	)}
}

func (v *dynamoGraphDeploymentValidation) validateExperimentalSpecUpdate(
	newExperimental *nvidiacomv1beta1.ExperimentalSpec,
	oldExperimental *nvidiacomv1beta1.ExperimentalSpec,
	fldPath *field.Path,
) field.ErrorList {
	allErrs := field.ErrorList{}
	newGMS := gpuMemoryServiceForExperimental(newExperimental)
	oldGMS := gpuMemoryServiceForExperimental(oldExperimental)
	if isInterPodGMS(newGMS) != isInterPodGMS(oldGMS) {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("gpuMemoryService", "mode"),
			gmsMode(newGMS),
			"the inter-pod GMS layout cannot be toggled after creation; delete and recreate the DynamoGraphDeployment",
		))
	}

	newFailover := failoverForExperimental(newExperimental)
	oldFailover := failoverForExperimental(oldExperimental)
	if isInterPodFailover(newFailover) != isInterPodFailover(oldFailover) {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("failover"),
			newFailover,
			"inter-pod GMS failover cannot be toggled after creation; delete and recreate the DynamoGraphDeployment",
		))
	}
	if isInterPodFailover(newFailover) && isInterPodFailover(oldFailover) &&
		effectiveNumShadows(newFailover) != effectiveNumShadows(oldFailover) {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("failover", "numShadows"),
			newFailover.NumShadows,
			"is immutable for inter-pod GMS failover; delete and recreate the DynamoGraphDeployment to change it",
		))
	}
	return allErrs
}
