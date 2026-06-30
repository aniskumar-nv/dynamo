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
	"sort"
	"strings"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/consts"
	controllercommon "github.com/ai-dynamo/dynamo/deploy/operator/internal/controller_common"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/dynamo"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/dynamo/epp"
	grovev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	// maxCombinedResourceNameLength is kept as a local alias for readability.
	maxCombinedResourceNameLength = consts.MaxCombinedGroveResourceNameLength

	unsetValue = "<unset>"

	vllmDistributedExecutorBackendMP  = "mp"
	vllmDistributedExecutorBackendRay = "ray"
)

type clusterTopologyInfo struct {
	name        string
	domainIndex map[string]int
	domains     []string
}

func invalidDynamoGraphDeploymentError(
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
	allErrs field.ErrorList,
) error {
	if len(allErrs) == 0 {
		return nil
	}
	name := ""
	if dgd != nil {
		name = dgd.Name
	}
	return k8serrors.NewInvalid(nvidiacomv1beta1.DynamoGraphDeploymentGVK.GroupKind(), name, allErrs)
}

func warningsForDynamoGraphDeploymentUpdate(
	newDGD *nvidiacomv1beta1.DynamoGraphDeployment,
	oldDGD *nvidiacomv1beta1.DynamoGraphDeployment,
) admission.Warnings {
	if newDGD == nil || oldDGD == nil || newDGD.Spec.BackendFramework == oldDGD.Spec.BackendFramework {
		return nil
	}
	return admission.Warnings{"Changing spec.backendFramework may cause unexpected behavior"}
}

func alphaDynamoGraphDeploymentForValidation(
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) (*nvidiacomv1alpha1.DynamoGraphDeployment, error) {
	alpha := &nvidiacomv1alpha1.DynamoGraphDeployment{}
	if err := alpha.ConvertFrom(dgd); err != nil {
		return nil, fmt.Errorf("failed to reconstruct compatibility view: %w", err)
	}
	return alpha, nil
}

func hasV1Alpha1CompatibilityFields(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) bool {
	if len(dgd.Spec.PVCs) > 0 {
		return true
	}
	for _, service := range dgd.Spec.Services {
		if service == nil {
			return true
		}
		hasDeprecatedAutoscaling := false
		//nolint:staticcheck // SA1019: Intentionally checking deprecated fields preserved by conversion.
		if service.Autoscaling != nil {
			hasDeprecatedAutoscaling = true
		}
		if service.Ingress != nil ||
			len(service.Annotations) > 0 ||
			service.DynamoNamespace != nil ||
			hasDeprecatedAutoscaling ||
			len(service.VolumeMounts) > 0 ||
			service.SharedMemory != nil ||
			service.FrontendSidecar != nil ||
			(service.GPUMemoryService != nil && !service.GPUMemoryService.Enabled) {
			return true
		}
	}
	return false
}

func sortedV1Alpha1ServiceNames(
	services map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec,
) []string {
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func warningsForV1Alpha1DynamoGraphDeployment(
	dgd *nvidiacomv1alpha1.DynamoGraphDeployment,
) admission.Warnings {
	if dgd == nil || !hasV1Alpha1CompatibilityFields(dgd) {
		return nil
	}

	warnings := admission.Warnings{}
	servicesPath := field.NewPath("spec", "services")
	for _, serviceName := range sortedV1Alpha1ServiceNames(dgd.Spec.Services) {
		service := dgd.Spec.Services[serviceName]
		if service == nil {
			continue
		}
		servicePath := servicesPath.Key(serviceName)
		if service.DynamoNamespace != nil && *service.DynamoNamespace != "" {
			warnings = append(warnings, fmt.Sprintf(
				"%s.dynamoNamespace is deprecated and ignored. Value %q will be replaced with %q. Remove this field from your configuration",
				servicePath,
				*service.DynamoNamespace,
				dgd.GetDynamoNamespaceForService(service),
			))
		}
		//nolint:staticcheck // SA1019: Intentionally warning about a deprecated preserved field.
		if service.Autoscaling != nil {
			warnings = append(warnings, fmt.Sprintf(
				"%s.autoscaling is deprecated and ignored. Use DynamoGraphDeploymentScalingAdapter with HPA, KEDA, or Planner for autoscaling instead. See docs/kubernetes/autoscaling.md",
				servicePath,
			))
		}
	}
	return warnings
}

func (v *dynamoGraphDeploymentValidation) inferencePoolAvailabilityError() error {
	if v.mgr == nil {
		return fmt.Errorf("manager is required to detect InferencePool API availability")
	}
	ctx := v.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if controllercommon.DetectInferencePoolAvailability(ctx, v.mgr) {
		return nil
	}
	return fmt.Errorf(
		"InferencePool API group (%s) is not available in the cluster; install the Gateway API Inference Extension before deploying EPP components",
		epp.InferencePoolGroup,
	)
}

func (v *dynamoGraphDeploymentValidation) readGroveClusterTopology(name string) (*clusterTopologyInfo, error) {
	clusterTopology := &grovev1alpha1.ClusterTopology{}
	if err := v.mgr.GetClient().Get(v.ctx, types.NamespacedName{Name: name}, clusterTopology); err != nil {
		return nil, err
	}

	info := &clusterTopologyInfo{
		name:        name,
		domainIndex: make(map[string]int, len(clusterTopology.Spec.Levels)),
		domains:     make([]string, 0, len(clusterTopology.Spec.Levels)),
	}
	for i, level := range clusterTopology.Spec.Levels {
		domain := string(level.Domain)
		info.domainIndex[domain] = i
		info.domains = append(info.domains, domain)
	}
	sort.Strings(info.domains)
	return info, nil
}

func hasContainerNamed(containers []corev1.Container, name string) bool {
	for i := range containers {
		if containers[i].Name == name {
			return true
		}
	}
	return false
}

func (v *dynamoGraphDeploymentValidation) grovePathwayForDynamoGraphDeployment(
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) (bool, string) {
	if !v.groveEnabled {
		return false, "requires the Grove pathway, but Grove is disabled in the operator configuration"
	}
	annotationValue := strings.ToLower(dgd.Annotations[consts.KubeAnnotationEnableGrove])
	if annotationValue == consts.KubeLabelValueFalse {
		return false, fmt.Sprintf(
			"requires the Grove pathway; remove or unset annotation %q (currently %q)",
			consts.KubeAnnotationEnableGrove,
			dgd.Annotations[consts.KubeAnnotationEnableGrove],
		)
	}
	return true, ""
}

func dgdComponentNameLengthError(
	dgdName string,
	components []nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
	component *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
	fldPath *field.Path,
) *field.Error {
	pcsName := dynamo.PCSNameForDGD(dgdName, components)
	componentName := component.ComponentName
	combinedLength := len(pcsName) + len(strings.ToLower(componentName))
	detail := "PCS name + component name"

	if component.GetNumberOfNodes() > 1 || component.IsInterPodGMSEnabled() {
		longestPodCliqueName := dynamo.LongestPodCliqueNameForDGDComponent(componentName, component)
		combinedLength += len(longestPodCliqueName)
		detail = fmt.Sprintf("PCS name + PCSG name + longest PodClique name %q", longestPodCliqueName)
	}
	if combinedLength <= maxCombinedResourceNameLength {
		return nil
	}
	return field.Invalid(
		fldPath,
		componentName,
		fmt.Sprintf(
			"combined resource name length %d exceeds the %d-character pod-name limit (%s); shorten DynamoGraphDeployment name %q or component name %q",
			combinedLength,
			maxCombinedResourceNameLength,
			detail,
			dgdName,
			componentName,
		),
	)
}

func hasIntraPodFailover(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) bool {
	for i := range spec.Components {
		failover := failoverFor(&spec.Components[i])
		if failover != nil && effectiveGMSMode(failover.Mode) == nvidiacomv1beta1.GMSModeIntraPod {
			return true
		}
	}
	return false
}

func componentsByName(
	components []nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
) map[string]*nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec {
	byName := make(map[string]*nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec, len(components))
	for i := range components {
		byName[components[i].ComponentName] = &components[i]
	}
	return byName
}

func sortedComponentNames(
	components map[string]*nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
) []string {
	names := make([]string, 0, len(components))
	for name := range components {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func componentNameSet(
	components map[string]*nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
) map[string]struct{} {
	names := make(map[string]struct{}, len(components))
	for name := range components {
		names[name] = struct{}{}
	}
	return names
}

func restartID(restart *nvidiacomv1beta1.Restart) string {
	if restart == nil {
		return ""
	}
	return restart.ID
}

func effectiveReplicas(replicas *int32) int32 {
	if replicas == nil {
		return 1
	}
	return *replicas
}

func specTopologyConstraintsEqual(a, b *nvidiacomv1beta1.SpecTopologyConstraint) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.ClusterTopologyName == b.ClusterTopologyName && a.PackDomain == b.PackDomain
}

func topologyConstraintsEqual(a, b *nvidiacomv1beta1.TopologyConstraint) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.PackDomain == b.PackDomain
}

func kvTransferPolicyFor(
	experimental *nvidiacomv1beta1.DynamoGraphDeploymentExperimentalSpec,
) *nvidiacomv1beta1.KvTransferPolicy {
	if experimental == nil {
		return nil
	}
	return experimental.KvTransferPolicy
}

func kvTransferPoliciesEqual(a, b *nvidiacomv1beta1.KvTransferPolicy) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.ClusterTopologyName == b.ClusterTopologyName &&
		a.LabelKey == b.LabelKey &&
		a.Domain == b.Domain &&
		effectiveKvTransferEnforcement(a) == effectiveKvTransferEnforcement(b) &&
		preferredWeightsEqual(a.PreferredWeight, b.PreferredWeight)
}

func effectiveKvTransferEnforcement(policy *nvidiacomv1beta1.KvTransferPolicy) nvidiacomv1beta1.KvTransferEnforcement {
	if policy == nil || policy.Enforcement == "" {
		return nvidiacomv1beta1.KvTransferEnforcementRequired
	}
	return policy.Enforcement
}

func preferredWeightsEqual(a, b *float32) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func gpuMemoryServiceFor(
	component *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
) *nvidiacomv1beta1.GPUMemoryServiceSpec {
	if component == nil {
		return nil
	}
	return gpuMemoryServiceForExperimental(component.Experimental)
}

func gpuMemoryServiceForExperimental(experimental *nvidiacomv1beta1.ExperimentalSpec) *nvidiacomv1beta1.GPUMemoryServiceSpec {
	if experimental == nil {
		return nil
	}
	return experimental.GPUMemoryService
}

func failoverFor(
	component *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
) *nvidiacomv1beta1.FailoverSpec {
	if component == nil {
		return nil
	}
	return failoverForExperimental(component.Experimental)
}

func failoverForExperimental(experimental *nvidiacomv1beta1.ExperimentalSpec) *nvidiacomv1beta1.FailoverSpec {
	if experimental == nil {
		return nil
	}
	return experimental.Failover
}

func effectiveGMSMode(mode nvidiacomv1beta1.GPUMemoryServiceMode) nvidiacomv1beta1.GPUMemoryServiceMode {
	if mode == "" {
		return nvidiacomv1beta1.GMSModeIntraPod
	}
	return mode
}

func gmsMode(gms *nvidiacomv1beta1.GPUMemoryServiceSpec) nvidiacomv1beta1.GPUMemoryServiceMode {
	if gms == nil {
		return ""
	}
	return gms.Mode
}

func isInterPodGMS(gms *nvidiacomv1beta1.GPUMemoryServiceSpec) bool {
	return gms != nil && effectiveGMSMode(gms.Mode) == nvidiacomv1beta1.GMSModeInterPod
}

func isInterPodFailover(failover *nvidiacomv1beta1.FailoverSpec) bool {
	return failover != nil && effectiveGMSMode(failover.Mode) == nvidiacomv1beta1.GMSModeInterPod
}

func effectiveNumShadows(failover *nvidiacomv1beta1.FailoverSpec) int32 {
	if failover == nil {
		return 0
	}
	if failover.NumShadows < 1 {
		return 1
	}
	return failover.NumShadows
}

func getUnique[T comparable](slice []T) []T {
	seen := make(map[T]struct{}, len(slice))
	uniqueSlice := make([]T, 0, len(slice))
	for _, element := range slice {
		if _, exists := seen[element]; !exists {
			seen[element] = struct{}{}
			uniqueSlice = append(uniqueSlice, element)
		}
	}
	return uniqueSlice
}

// difference returns elements in set a that are not in set b (a - b).
func difference(a, b map[string]struct{}) []string {
	var result []string
	for name := range a {
		if _, exists := b[name]; !exists {
			result = append(result, name)
		}
	}
	return result
}

func invalidVLLMDistributedExecutorBackendAnnotation(annotations map[string]string) (string, bool) {
	value, exists := annotations[consts.KubeAnnotationVLLMDistributedExecutorBackend]
	if !exists {
		return "", false
	}

	switch strings.ToLower(value) {
	case vllmDistributedExecutorBackendMP, vllmDistributedExecutorBackendRay:
		return "", false
	default:
		return value, true
	}
}
