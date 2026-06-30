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
	"fmt"
	"sort"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
)

// alphaDynamoGraphDeploymentForValidation reconstructs the compatibility view.
// dgd must not be nil.
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
