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
	"strings"
	"testing"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/consts"
	grovev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	k8sptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDynamoGraphDeploymentValidator_Validate(t *testing.T) {
	tests := []struct {
		name         string
		deployment   *nvidiacomv1beta1.DynamoGraphDeployment
		groveEnabled bool
		wantErr      string
	}{
		{
			name:       "valid deployment with components",
			deployment: newBetaDGDForValidation(),
		},
		{
			name: "no components",
			deployment: &nvidiacomv1beta1.DynamoGraphDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: "test-graph", Namespace: "default"},
			},
			wantErr: "spec.components: Required value: must have at least one component",
		},
		{
			name: "component name validation is owned by the schema",
			deployment: &nvidiacomv1beta1.DynamoGraphDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: "test-graph", Namespace: "default"},
				Spec: nvidiacomv1beta1.DynamoGraphDeploymentSpec{
					Components: []nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{{}},
				},
			},
		},
		{
			name: "component name uniqueness is owned by CEL",
			deployment: &nvidiacomv1beta1.DynamoGraphDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: "test-graph", Namespace: "default"},
				Spec: nvidiacomv1beta1.DynamoGraphDeploymentSpec{
					Components: []nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{
						{ComponentName: "worker"},
						{ComponentName: "WORKER"},
					},
				},
			},
		},
		{
			name: "component replica minimum is owned by the schema",
			deployment: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.Replicas = k8sptr.To(int32(-1))
			}),
		},
		{
			name: "component minAvailable requires Grove",
			deployment: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.MinAvailable = k8sptr.To(int32(1))
			}),
			wantErr: "spec.components[1].minAvailable: Forbidden: is currently supported only for Grove-backed DynamoGraphDeployment components",
		},
		{
			name: "restart parallel strategy cannot specify order",
			deployment: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = &nvidiacomv1beta1.Restart{
					ID: "roll",
					Strategy: &nvidiacomv1beta1.RestartStrategy{
						Type:  nvidiacomv1beta1.RestartStrategyTypeParallel,
						Order: []string{"frontend", "worker"},
					},
				}
			}),
			wantErr: "spec.restart.strategy.order: Forbidden: cannot be specified when strategy is parallel",
		},
		{
			name: "component topology constraint requires deployment topology",
			deployment: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			}),
			wantErr: "spec.topologyConstraint: Required value: is required when any component topology constraint is set",
		},
		{
			name:         "inter-pod GMS requires Grove",
			groveEnabled: false,
			deployment: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				enableBetaInterPodGMS(worker)
			}),
			wantErr: "spec.components[1].experimental.gpuMemoryService.mode: Forbidden: requires the Grove pathway",
		},
		{
			name:         "inter-pod GMS requires vLLM backend",
			groveEnabled: true,
			deployment: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.BackendFramework = "sglang"
				enableBetaInterPodGMS(&spec.Components[1])
			}),
			wantErr: "spec.components[1].experimental.gpuMemoryService.mode: Invalid value",
		},
		{
			name: "kv transfer selector exclusivity is owned by CEL",
			deployment: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Experimental = &nvidiacomv1beta1.DynamoGraphDeploymentExperimentalSpec{
					KvTransferPolicy: &nvidiacomv1beta1.KvTransferPolicy{
						Domain: "rack",
					},
				}
			}),
		},
		{
			name: "intra-pod failover requires container discovery",
			deployment: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				enableBetaIntraPodGMS(worker)
				worker.Experimental.Failover = &nvidiacomv1beta1.FailoverSpec{
					Mode: nvidiacomv1beta1.GMSModeIntraPod,
				}
			}),
			wantErr: `metadata.annotations[nvidia.com/dynamo-kube-discovery-mode]: Invalid value: "": must be "container"`,
		},
		{
			name: "checkpoint job and ref exclusivity is owned by CEL",
			deployment: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.Experimental = &nvidiacomv1beta1.ExperimentalSpec{
					Checkpoint: &nvidiacomv1beta1.ComponentCheckpointConfig{
						Enabled:       true,
						CheckpointRef: k8sptr.To("existing-checkpoint"),
						Job:           &nvidiacomv1beta1.ComponentCheckpointJobConfig{},
					},
				}
			}),
		},
		{
			name: "GMS requires GPU resources on the main container",
			deployment: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.Experimental = &nvidiacomv1beta1.ExperimentalSpec{
					GPUMemoryService: &nvidiacomv1beta1.GPUMemoryServiceSpec{
						Mode: nvidiacomv1beta1.GMSModeIntraPod,
					},
				}
			}),
			wantErr: "spec.components[1].experimental.gpuMemoryService: Invalid value",
		},
		{
			name: "sidecars must provide an image",
			deployment: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.PodTemplate = &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: consts.MainContainerName},
							{Name: "metrics"},
						},
					},
				}
			}),
			wantErr: `spec.components[1].podTemplate.spec.containers[1].image: Required value: is required for sidecar container "metrics"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewDynamoGraphDeploymentValidator(nil, tt.groveEnabled)
			_, err := validator.Validate(context.Background(), tt.deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_GroveSchedulingMatrix(t *testing.T) {
	longDGDName := "test-graph-" + strings.Repeat("x", 50)
	boundaryComponentName := "w" + strings.Repeat("x", 36)
	tooLongComponentName := boundaryComponentName + "x"

	tests := []struct {
		name         string
		groveEnabled bool
		mutate       func(*nvidiacomv1beta1.DynamoGraphDeployment)
		wantErr      string
	}{
		{
			name:         "priority class requires Grove",
			groveEnabled: false,
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				dgd.Spec.PriorityClassName = "high-priority"
			},
			wantErr: "spec.priorityClassName: Forbidden: requires the Grove pathway",
		},
		{
			name:         "priority class is allowed with Grove",
			groveEnabled: true,
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				dgd.Spec.PriorityClassName = "high-priority"
			},
		},
		{
			name:         "minAvailable minimum is owned by the schema",
			groveEnabled: true,
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				betaWorkerComponent(dgd).MinAvailable = k8sptr.To(int32(0))
			},
		},
		{
			name:         "replicas and minAvailable relationship is owned by CEL",
			groveEnabled: true,
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				worker := betaWorkerComponent(dgd)
				worker.Replicas = k8sptr.To(int32(1))
				worker.MinAvailable = k8sptr.To(int32(2))
			},
		},
		{
			name:         "replicas zero can keep minAvailable for scale-up intent",
			groveEnabled: true,
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				worker := betaWorkerComponent(dgd)
				worker.Replicas = k8sptr.To(int32(0))
				worker.MinAvailable = k8sptr.To(int32(2))
			},
		},
		{
			name:         "rendered Grove resource name length accepts boundary",
			groveEnabled: true,
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				dgd.Name = longDGDName
				betaWorkerComponent(dgd).ComponentName = boundaryComponentName
			},
		},
		{
			name:         "rendered Grove resource name length rejects overflow",
			groveEnabled: true,
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				dgd.Name = longDGDName
				betaWorkerComponent(dgd).ComponentName = tooLongComponentName
			},
			wantErr: "combined resource name length",
		},
		{
			name:         "rendered Grove resource name length is skipped outside Grove",
			groveEnabled: false,
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				dgd.Name = longDGDName
				betaWorkerComponent(dgd).ComponentName = tooLongComponentName
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := newBetaDGDForValidation()
			tt.mutate(deployment)

			validator := NewDynamoGraphDeploymentValidator(nil, tt.groveEnabled)
			_, err := validator.Validate(context.Background(), deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_ValidateAggregatesErrors(t *testing.T) {
	deployment := betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
		spec.Restart = &nvidiacomv1beta1.Restart{}
		spec.Components[0].Replicas = k8sptr.To(int32(-1))
		spec.Components[1].Replicas = k8sptr.To(int32(-2))
		spec.Components[1].CompilationCache = &nvidiacomv1beta1.CompilationCacheConfig{}
	})
	deployment.Annotations = map[string]string{
		consts.KubeAnnotationDynamoOperatorOriginVersion: "not-semver",
		consts.KubeAnnotationDynamoKubeDiscoveryMode:     "bad-mode",
	}

	validator := NewDynamoGraphDeploymentValidator(nil, true)
	_, err := validator.Validate(context.Background(), deployment)
	for _, wantErr := range []string{
		"metadata.annotations[nvidia.com/dynamo-operator-origin-version]",
		"metadata.annotations[nvidia.com/dynamo-kube-discovery-mode]",
	} {
		assertBetaValidationError(t, err, wantErr)
	}
}

func TestDynamoGraphDeploymentValidator_AnnotationMatrix(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		value      string
		wantErr    string
	}{
		{
			name:       "origin version accepts semver",
			annotation: consts.KubeAnnotationDynamoOperatorOriginVersion,
			value:      "1.2.3",
		},
		{
			name:       "origin version rejects non-semver",
			annotation: consts.KubeAnnotationDynamoOperatorOriginVersion,
			value:      "not-semver",
			wantErr:    "metadata.annotations[nvidia.com/dynamo-operator-origin-version]",
		},
		{
			name:       "vLLM backend accepts mp",
			annotation: consts.KubeAnnotationVLLMDistributedExecutorBackend,
			value:      "mp",
		},
		{
			name:       "vLLM backend accepts ray case-insensitively",
			annotation: consts.KubeAnnotationVLLMDistributedExecutorBackend,
			value:      "RAY",
		},
		{
			name:       "vLLM backend rejects unknown value",
			annotation: consts.KubeAnnotationVLLMDistributedExecutorBackend,
			value:      "typo",
			wantErr:    "metadata.annotations[nvidia.com/vllm-distributed-executor-backend]",
		},
		{
			name:       "discovery mode accepts pod",
			annotation: consts.KubeAnnotationDynamoKubeDiscoveryMode,
			value:      "pod",
		},
		{
			name:       "discovery mode accepts container",
			annotation: consts.KubeAnnotationDynamoKubeDiscoveryMode,
			value:      "container",
		},
		{
			name:       "discovery mode rejects unknown value",
			annotation: consts.KubeAnnotationDynamoKubeDiscoveryMode,
			value:      "endpoint",
			wantErr:    "metadata.annotations[nvidia.com/dynamo-kube-discovery-mode]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := newBetaDGDForValidation()
			deployment.Annotations = map[string]string{tt.annotation: tt.value}

			validator := NewDynamoGraphDeploymentValidator(nil, true)
			_, err := validator.Validate(context.Background(), deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_ValidateAlphaCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*nvidiacomv1alpha1.DynamoGraphDeployment)
		wantErr string
	}{
		{
			name: "alpha PVC create requirements are owned by CEL",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.PVCs = []nvidiacomv1alpha1.PVC{
					{
						Name:   k8sptr.To("cache"),
						Create: k8sptr.To(true),
					},
				}
			},
		},
		{
			name: "alpha ingress requires host",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				className := "nginx"
				dgd.Spec.Services["frontend"] = &nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
					ComponentType: consts.ComponentTypeFrontend,
					Ingress: &nvidiacomv1alpha1.IngressSpec{
						Enabled:                    true,
						IngressControllerClassName: &className,
					},
				}
			},
			wantErr: "spec.services[frontend].ingress.host: Required value: is required when ingress is enabled",
		},
		{
			name: "alpha service annotations are validated",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["worker"].Annotations = map[string]string{
					consts.KubeAnnotationVLLMDistributedExecutorBackend: "typo",
				}
			},
			wantErr: "spec.services[worker].annotations[nvidia.com/vllm-distributed-executor-backend]: Invalid value",
		},
		{
			name: "alpha volume mounts require mount point unless used as compilation cache",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["worker"].VolumeMounts = []nvidiacomv1alpha1.VolumeMount{
					{
						Name: "cache",
					},
				}
			},
			wantErr: "spec.services[worker].volumeMounts[0].mountPoint: Required value: is required when useAsCompilationCache is false",
		},
		{
			name: "alpha sharedMemory size requirement is owned by CEL",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["worker"].SharedMemory = &nvidiacomv1alpha1.SharedMemorySpec{
					Disabled: false,
				}
			},
		},
		{
			name: "alpha frontend sidecar rejects generated container name conflict",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["frontend"] = &nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
					ComponentType: consts.ComponentTypeFrontend,
					FrontendSidecar: &nvidiacomv1alpha1.FrontendSidecarSpec{
						Image: "custom/frontend:latest",
					},
					ExtraPodSpec: &nvidiacomv1alpha1.ExtraPodSpec{
						PodSpec: &corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  consts.FrontendSidecarContainerName,
									Image: "custom/frontend:latest",
								},
							},
						},
					},
				}
			},
			wantErr: `spec.services[frontend].frontendSidecar: Invalid value`,
		},
		{
			name: "alpha GMS client container names are owned by the schema",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["worker"].GPUMemoryService = &nvidiacomv1alpha1.GPUMemoryServiceSpec{
					Enabled:               false,
					ExtraClientContainers: []string{"Bad_Name"},
				}
			},
		},
		{
			name: "nil alpha service entry is rejected",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["ghost"] = nil
			},
			wantErr: "spec.services[ghost]: Required value: must not be null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := betaDGDFromAlpha(t, tt.mutate)
			validator := NewDynamoGraphDeploymentValidator(nil, true)
			_, err := validator.Validate(context.Background(), deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_ValidateAlphaCompatibilityAdditionalEdges(t *testing.T) {
	t.Run("valid preserved alpha-only fields are accepted", func(t *testing.T) {
		deployment := betaDGDFromAlpha(t, func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
			host := "worker.example.com"
			dgd.Spec.PVCs = []nvidiacomv1alpha1.PVC{
				{
					Name:   k8sptr.To("cache"),
					Create: k8sptr.To(false),
				},
			}
			service := dgd.Spec.Services["worker"]
			service.Ingress = &nvidiacomv1alpha1.IngressSpec{
				Enabled: true,
				Host:    host,
			}
			service.Annotations = map[string]string{
				consts.KubeAnnotationVLLMDistributedExecutorBackend: "ray",
			}
			service.VolumeMounts = []nvidiacomv1alpha1.VolumeMount{
				{
					Name:                  "cache",
					UseAsCompilationCache: true,
				},
			}
			service.SharedMemory = &nvidiacomv1alpha1.SharedMemorySpec{Disabled: true}
			service.GPUMemoryService = &nvidiacomv1alpha1.GPUMemoryServiceSpec{
				Enabled:               false,
				ExtraClientContainers: []string{"metrics"},
			}
		})

		validator := NewDynamoGraphDeploymentValidator(nil, true)
		_, err := validator.Validate(context.Background(), deployment)
		assertBetaValidationError(t, err, "")
	})

	t.Run("alpha PVC name requirement is owned by the schema", func(t *testing.T) {
		deployment := betaDGDFromAlpha(t, func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
			dgd.Spec.PVCs = []nvidiacomv1alpha1.PVC{{}}
		})

		validator := NewDynamoGraphDeploymentValidator(nil, true)
		_, err := validator.Validate(context.Background(), deployment)
		assertBetaValidationError(t, err, "")
	})

	t.Run("alpha PVC create constraints are owned by CEL", func(t *testing.T) {
		deployment := betaDGDFromAlpha(t, func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
			dgd.Spec.PVCs = []nvidiacomv1alpha1.PVC{
				{
					Create: k8sptr.To(true),
				},
			}
		})

		validator := NewDynamoGraphDeploymentValidator(nil, true)
		_, err := validator.Validate(context.Background(), deployment)
		assertBetaValidationError(t, err, "")
	})
}

func TestDynamoGraphDeploymentValidator_ValidateAlphaCompatibilityWarnings(t *testing.T) {
	legacyNamespace := "legacy-namespace"
	deployment := betaDGDFromAlpha(t, func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
		service := dgd.Spec.Services["worker"]
		service.DynamoNamespace = &legacyNamespace
		//nolint:staticcheck // SA1019: Intentionally testing deprecated field warnings.
		service.Autoscaling = &nvidiacomv1alpha1.Autoscaling{Enabled: true}
	})

	validator := NewDynamoGraphDeploymentValidator(nil, true)
	warnings, err := validator.Validate(context.Background(), deployment)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	assertWarningsContain(t, warnings, "spec.services[worker].dynamoNamespace is deprecated and ignored")
	assertWarningsContain(t, warnings, "spec.services[worker].autoscaling is deprecated and ignored")
}

func TestDynamoGraphDeploymentValidator_ValidateConvertedAlphaResourceSemantics(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*nvidiacomv1alpha1.DynamoGraphDeployment)
		wantErr string
	}{
		{
			name: "GMS accepts GPU from alpha dedicated resources",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				service := dgd.Spec.Services["worker"]
				service.Resources = &nvidiacomv1alpha1.Resources{
					Limits: &nvidiacomv1alpha1.ResourceItem{GPU: "1"},
				}
				service.GPUMemoryService = &nvidiacomv1alpha1.GPUMemoryServiceSpec{
					Enabled: true,
					Mode:    nvidiacomv1alpha1.GMSModeIntraPod,
				}
			},
		},
		{
			name: "GMS accepts GPU from alpha extraPodSpec main container resources",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				service := dgd.Spec.Services["worker"]
				service.ExtraPodSpec = &nvidiacomv1alpha1.ExtraPodSpec{
					MainContainer: &corev1.Container{
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceName(consts.KubeResourceGPUNvidia): resource.MustParse("1"),
							},
						},
					},
				}
				service.GPUMemoryService = &nvidiacomv1alpha1.GPUMemoryServiceSpec{
					Enabled: true,
					Mode:    nvidiacomv1alpha1.GMSModeIntraPod,
				}
			},
		},
		{
			name: "GMS accepts alpha GPUType resource after conversion",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				service := dgd.Spec.Services["worker"]
				service.Resources = &nvidiacomv1alpha1.Resources{
					Limits: &nvidiacomv1alpha1.ResourceItem{
						GPU:     "1",
						GPUType: "example.com/gpu",
					},
				}
				service.GPUMemoryService = &nvidiacomv1alpha1.GPUMemoryServiceSpec{
					Enabled: true,
					Mode:    nvidiacomv1alpha1.GMSModeIntraPod,
				}
			},
		},
		{
			name: "converted alpha extraPodMetadata annotations get beta podTemplate validation",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["worker"].ExtraPodMetadata = &nvidiacomv1alpha1.ExtraPodMetadata{
					Annotations: map[string]string{
						consts.KubeAnnotationVLLMDistributedExecutorBackend: "typo",
					},
				}
			},
			wantErr: "spec.components[0].podTemplate.metadata.annotations[nvidia.com/vllm-distributed-executor-backend]",
		},
		{
			name: "converted component name uniqueness is owned by CEL",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["WORKER"] = &nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
					ComponentType: consts.ComponentTypeWorker,
				}
			},
		},
		{
			name: "converted compilation cache PVC requirement is owned by the schema",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["worker"].VolumeMounts = []nvidiacomv1alpha1.VolumeMount{
					{
						UseAsCompilationCache: true,
					},
				}
			},
		},
		{
			name: "converted empty component name is owned by the schema",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services = map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
					"": {
						ComponentType: consts.ComponentTypeWorker,
					},
				}
			},
		},
		{
			name: "converted alpha init containers must provide an image",
			mutate: func(dgd *nvidiacomv1alpha1.DynamoGraphDeployment) {
				dgd.Spec.Services["worker"].ExtraPodSpec = &nvidiacomv1alpha1.ExtraPodSpec{
					PodSpec: &corev1.PodSpec{
						InitContainers: []corev1.Container{{Name: "prep"}},
					},
				}
			},
			wantErr: `spec.components[0].podTemplate.spec.initContainers[0].image: Required value: is required for init container "prep"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := betaDGDFromAlpha(t, tt.mutate)
			validator := NewDynamoGraphDeploymentValidator(nil, true)
			_, err := validator.Validate(context.Background(), deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_RestartMatrix(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*nvidiacomv1beta1.DynamoGraphDeploymentSpec)
		wantErr string
	}{
		{
			name: "restart id requirement is owned by the schema",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = &nvidiacomv1beta1.Restart{}
			},
		},
		{
			name: "duplicate restart order",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = betaRestart(nvidiacomv1beta1.RestartStrategyTypeSequential, "frontend", "worker", "worker")
			},
			wantErr: "spec.restart.strategy.order: Invalid value",
		},
		{
			name: "unknown restart order component",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = betaRestart(nvidiacomv1beta1.RestartStrategyTypeSequential, "frontend", "ghost")
			},
			wantErr: "spec.restart.strategy.order[1]: Unsupported value: \"ghost\"",
		},
		{
			name: "restart order missing component",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = betaRestart(nvidiacomv1beta1.RestartStrategyTypeSequential, "worker")
			},
			wantErr: "spec.restart.strategy.order: Invalid value",
		},
		{
			name: "empty sequential restart order is valid",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = betaRestart(nvidiacomv1beta1.RestartStrategyTypeSequential)
			},
		},
		{
			name: "complete sequential restart order is valid",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = betaRestart(nvidiacomv1beta1.RestartStrategyTypeSequential, "frontend", "worker")
			},
		},
		{
			name: "parallel restart without order is valid",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = betaRestart(nvidiacomv1beta1.RestartStrategyTypeParallel)
			},
		},
		{
			name: "parallel restart rejects order",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = betaRestart(nvidiacomv1beta1.RestartStrategyTypeParallel, "frontend", "worker")
			},
			wantErr: "spec.restart.strategy.order: Forbidden: cannot be specified when strategy is parallel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := betaDGDWithSpec(tt.mutate)
			validator := NewDynamoGraphDeploymentValidator(nil, true)
			_, err := validator.Validate(context.Background(), deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_TopologyMatrix(t *testing.T) {
	topologyManager := newGroveTopologyTestManager(t, newTestClusterTopology())
	missingTopologyManager := newGroveTopologyTestManager(t)

	tests := []struct {
		name    string
		mgr     ctrl.Manager
		mutate  func(*nvidiacomv1beta1.DynamoGraphDeploymentSpec)
		wantErr string
	}{
		{
			name: "spec pack domain format is owned by the schema",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "grove-topology",
					PackDomain:          "Bad_Domain",
				}
			},
		},
		{
			name: "component topology requires deployment topology",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Components[1].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			},
			wantErr: "spec.topologyConstraint: Required value: is required when any component topology constraint is set",
		},
		{
			name: "component topology pack domain is owned by the schema",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{ClusterTopologyName: "grove-topology"}
				spec.Components[0].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{}
				spec.Components[1].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			},
		},
		{
			name: "deployment topology without pack domain requires every component topology",
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{ClusterTopologyName: "grove-topology"}
				spec.Components[1].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			},
			wantErr: "spec.components[0].topologyConstraint: Required value: is required because spec.topologyConstraint.packDomain is not set",
		},
		{
			name: "deployment pack domain can be inherited",
			mgr:  topologyManager,
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "grove-topology",
					PackDomain:          "rack",
				}
			},
		},
		{
			name: "deployment pack domain can be mixed with narrower component topology",
			mgr:  topologyManager,
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "grove-topology",
					PackDomain:          "zone",
				}
				spec.Components[1].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			},
		},
		{
			name: "component topology with deployment topology is valid",
			mgr:  topologyManager,
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{ClusterTopologyName: "grove-topology"}
				spec.Components[0].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "zone"}
				spec.Components[1].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			},
		},
		{
			name: "missing cluster topology is rejected",
			mgr:  missingTopologyManager,
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "missing-topology",
					PackDomain:          "rack",
				}
			},
			wantErr: `spec.topologyConstraint.clusterTopologyName: Invalid value: "missing-topology"`,
		},
		{
			name: "pack domain must exist in cluster topology",
			mgr:  topologyManager,
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "grove-topology",
					PackDomain:          "host",
				}
			},
			wantErr: `spec.topologyConstraint.packDomain: Invalid value: "host": does not exist in ClusterTopology "grove-topology"`,
		},
		{
			name: "component topology cannot be broader than spec topology",
			mgr:  topologyManager,
			mutate: func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "grove-topology",
					PackDomain:          "rack",
				}
				spec.Components[1].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "zone"}
			},
			wantErr: `spec.components[1].topologyConstraint.packDomain: Invalid value: "zone"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := betaDGDWithSpec(tt.mutate)
			validator := NewDynamoGraphDeploymentValidator(tt.mgr, true)
			_, err := validator.Validate(context.Background(), deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_KvTransferPolicyMatrix(t *testing.T) {
	topologyManager := newGroveTopologyTestManager(t, newTestClusterTopology())
	missingTopologyManager := newGroveTopologyTestManager(t)

	tests := []struct {
		name      string
		mgr       ctrl.Manager
		mutateDGD func(*nvidiacomv1beta1.DynamoGraphDeployment)
		policy    *nvidiacomv1beta1.KvTransferPolicy
		wantErr   string
	}{
		{
			name:   "topology selector requirement is owned by CEL",
			policy: &nvidiacomv1beta1.KvTransferPolicy{Domain: "zone"},
		},
		{
			name: "topology selector exclusivity is owned by CEL",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey:            "topology.kubernetes.io/zone",
				ClusterTopologyName: "grove-topology",
				Domain:              "zone",
			},
		},
		{
			name: "label key format is owned by the schema",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey: "bad prefix/zone",
				Domain:   "zone",
			},
		},
		{
			name: "label key name segment is owned by the schema",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey: "topology.kubernetes.io/-zone",
				Domain:   "zone",
			},
		},
		{
			name: "label key policy is valid",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey: "topology.kubernetes.io/zone",
				Domain:   "zone",
			},
		},
		{
			name: "cluster topology name format is owned by the schema",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				ClusterTopologyName: "Bad_Name",
				Domain:              "zone",
			},
		},
		{
			name: "cluster topology name requires Grove pathway",
			mutateDGD: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				dgd.Annotations = map[string]string{consts.KubeAnnotationEnableGrove: consts.KubeLabelValueFalse}
			},
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				ClusterTopologyName: "grove-topology",
				Domain:              "zone",
			},
			wantErr: "spec.experimental.kvTransferPolicy.clusterTopologyName: Forbidden: requires the Grove pathway",
		},
		{
			name: "domain requirement is owned by the schema",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey: "topology.kubernetes.io/zone",
			},
		},
		{
			name: "domain format is owned by the schema",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey: "topology.kubernetes.io/zone",
				Domain:   "Zone",
			},
		},
		{
			name: "enforcement enum is owned by the schema",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey:    "topology.kubernetes.io/zone",
				Domain:      "zone",
				Enforcement: "sometimes",
			},
		},
		{
			name: "preferred enforcement weight requirement is owned by CEL",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey:    "topology.kubernetes.io/zone",
				Domain:      "zone",
				Enforcement: nvidiacomv1beta1.KvTransferEnforcementPreferred,
			},
		},
		{
			name: "preferred weight range is owned by the schema",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey:        "topology.kubernetes.io/zone",
				Domain:          "zone",
				Enforcement:     nvidiacomv1beta1.KvTransferEnforcementPreferred,
				PreferredWeight: k8sptr.To(float32(1.2)),
			},
		},
		{
			name: "required enforcement weight exclusion is owned by CEL",
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				LabelKey:        "topology.kubernetes.io/zone",
				Domain:          "zone",
				Enforcement:     nvidiacomv1beta1.KvTransferEnforcementRequired,
				PreferredWeight: k8sptr.To(float32(0.5)),
			},
		},
		{
			name: "cluster topology policy is valid",
			mgr:  topologyManager,
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				ClusterTopologyName: "grove-topology",
				Domain:              "rack",
			},
		},
		{
			name: "cluster topology policy rejects missing topology",
			mgr:  missingTopologyManager,
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				ClusterTopologyName: "missing-topology",
				Domain:              "rack",
			},
			wantErr: `spec.experimental.kvTransferPolicy.clusterTopologyName: Invalid value: "missing-topology"`,
		},
		{
			name: "cluster topology policy rejects missing domain",
			mgr:  topologyManager,
			policy: &nvidiacomv1beta1.KvTransferPolicy{
				ClusterTopologyName: "grove-topology",
				Domain:              "host",
			},
			wantErr: `spec.experimental.kvTransferPolicy.domain: Invalid value: "host"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := betaDGDWithKvTransferPolicy(tt.policy)
			if tt.mutateDGD != nil {
				tt.mutateDGD(deployment)
			}
			validator := NewDynamoGraphDeploymentValidator(tt.mgr, true)
			_, err := validator.Validate(context.Background(), deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_GMSFailoverMatrix(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*nvidiacomv1beta1.DynamoGraphDeployment)
		wantErr string
	}{
		{
			name: "GMS rejects frontend component",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				enableBetaIntraPodGMS(&dgd.Spec.Components[0])
			},
			wantErr: "spec.components[0].experimental.gpuMemoryService: Forbidden",
		},
		{
			name: "GMS requires main container GPU",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				worker := betaWorkerComponent(dgd)
				worker.Experimental = &nvidiacomv1beta1.ExperimentalSpec{
					GPUMemoryService: &nvidiacomv1beta1.GPUMemoryServiceSpec{Mode: nvidiacomv1beta1.GMSModeIntraPod},
				}
			},
			wantErr: "spec.components[1].experimental.gpuMemoryService: Invalid value",
		},
		{
			name: "GMS client container names are owned by the schema",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				worker := betaWorkerComponent(dgd)
				enableBetaIntraPodGMS(worker)
				worker.Experimental.GPUMemoryService.ExtraClientContainers = []string{"Bad_Name"}
			},
		},
		{
			name: "inter-pod GMS client-container restriction is owned by CEL",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				worker := betaWorkerComponent(dgd)
				enableBetaInterPodGMS(worker)
				worker.Experimental.GPUMemoryService.ExtraClientContainers = []string{"metrics"}
			},
		},
		{
			name: "GMS extra client pod reservation is owned by CEL",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				worker := betaWorkerComponent(dgd)
				enableBetaInterPodGMS(worker)
				worker.Experimental.GPUMemoryService.ExtraClientPods = []nvidiacomv1beta1.GMSClientPodSpec{
					{Name: "client"},
				}
			},
		},
		{
			name: "intra-pod failover requires GMS",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				enableBetaContainerDiscovery(dgd)
				betaWorkerComponent(dgd).Experimental = &nvidiacomv1beta1.ExperimentalSpec{
					Failover: &nvidiacomv1beta1.FailoverSpec{Mode: nvidiacomv1beta1.GMSModeIntraPod},
				}
			},
			wantErr: "spec.components[1].experimental.failover: Invalid value",
		},
		{
			name: "failover mode must match GMS mode",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				worker := betaWorkerComponent(dgd)
				enableBetaIntraPodGMS(worker)
				worker.Experimental.Failover = &nvidiacomv1beta1.FailoverSpec{
					Mode:       nvidiacomv1beta1.GMSModeInterPod,
					NumShadows: 1,
				}
			},
			wantErr: `spec.components[1].experimental.failover.mode: Invalid value: "InterPod"`,
		},
		{
			name: "intra-pod failover shadow count is owned by the schema",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				enableBetaContainerDiscovery(dgd)
				worker := betaWorkerComponent(dgd)
				enableBetaIntraPodGMS(worker)
				worker.Experimental.Failover = &nvidiacomv1beta1.FailoverSpec{
					Mode:       nvidiacomv1beta1.GMSModeIntraPod,
					NumShadows: 2,
				}
			},
		},
		{
			name: "inter-pod failover requires GMS",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				betaWorkerComponent(dgd).Experimental = &nvidiacomv1beta1.ExperimentalSpec{
					Failover: &nvidiacomv1beta1.FailoverSpec{
						Mode:       nvidiacomv1beta1.GMSModeInterPod,
						NumShadows: 1,
					},
				}
			},
			wantErr: "spec.components[1].experimental.failover: Invalid value",
		},
		{
			name: "inter-pod failover shadow-count minimum is owned by the schema",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				worker := betaWorkerComponent(dgd)
				enableBetaInterPodGMS(worker)
				worker.Experimental.Failover = &nvidiacomv1beta1.FailoverSpec{
					Mode: nvidiacomv1beta1.GMSModeInterPod,
				}
			},
		},
		{
			name: "inter-pod failover rejects frontend component",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				frontend := &dgd.Spec.Components[0]
				frontend.Experimental = &nvidiacomv1beta1.ExperimentalSpec{
					Failover: &nvidiacomv1beta1.FailoverSpec{
						Mode:       nvidiacomv1beta1.GMSModeInterPod,
						NumShadows: 1,
					},
				}
			},
			wantErr: "spec.components[0].experimental.failover: Forbidden",
		},
		{
			name: "GMS snapshot combination requires env gate",
			mutate: func(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
				t.Setenv(consts.DynamoOperatorAllowGMSSnapshotEnvVar, "")
				worker := betaWorkerComponent(dgd)
				enableBetaIntraPodGMS(worker)
				worker.Experimental.Checkpoint = &nvidiacomv1beta1.ComponentCheckpointConfig{Enabled: true}
			},
			wantErr: "spec.components[1].experimental.checkpoint: Forbidden: GMS + Snapshot is temporarily disabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := newBetaDGDForValidation()
			tt.mutate(deployment)
			validator := NewDynamoGraphDeploymentValidator(nil, true)
			_, err := validator.Validate(context.Background(), deployment)
			assertBetaValidationError(t, err, tt.wantErr)
		})
	}
}

func TestDynamoGraphDeploymentValidator_ValidateUpdate(t *testing.T) {
	const operatorPrincipal = "system:serviceaccount:dynamo-system:dynamo-operator"

	tests := []struct {
		name      string
		oldDGD    *nvidiacomv1beta1.DynamoGraphDeployment
		newDGD    *nvidiacomv1beta1.DynamoGraphDeployment
		userInfo  *authenticationv1.UserInfo
		principal string
		wantErr   string
		wantWarns bool
	}{
		{
			name:   "component topology is immutable",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Components = append(spec.Components, nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{
					ComponentName: "extra",
					Replicas:      k8sptr.To(int32(1)),
				})
			}),
			wantErr: "component topology is immutable and cannot be modified after creation: components added: [extra]",
		},
		{
			name:   "component removal is immutable",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Components = spec.Components[:1]
			}),
			wantErr: "component topology is immutable and cannot be modified after creation: components removed: [worker]",
		},
		{
			name:   "component add and remove reports both sides",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Components = []nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{
					spec.Components[1],
					{
						ComponentName: "extra",
						Replicas:      k8sptr.To(int32(1)),
					},
				}
			}),
			wantErr: "component topology is immutable and cannot be modified after creation: components added: [extra], components removed: [frontend]",
		},
		{
			name:   "component reorder is allowed",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Components[0], spec.Components[1] = spec.Components[1], spec.Components[0]
			}),
		},
		{
			name:   "single-node to multinode transition is immutable",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.Multinode = &nvidiacomv1beta1.MultinodeSpec{NodeCount: 2}
			}),
			wantErr: "spec.components[1].multinode: Invalid value",
		},
		{
			name: "node count-only update remains allowed",
			oldDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.Multinode = &nvidiacomv1beta1.MultinodeSpec{NodeCount: 2}
			}),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.Multinode = &nvidiacomv1beta1.MultinodeSpec{NodeCount: 3}
			}),
		},
		{
			name:   "spec topology constraint is immutable",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "grove-topology",
					PackDomain:          "rack",
				}
			}),
			wantErr: "spec.topologyConstraint: Invalid value",
		},
		{
			name: "spec topology constraint change is immutable",
			oldDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "grove-topology",
					PackDomain:          "rack",
				}
			}),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{
					ClusterTopologyName: "grove-topology",
					PackDomain:          "zone",
				}
			}),
			wantErr: "spec.topologyConstraint: Invalid value",
		},
		{
			name: "unchanged topology constraints are allowed",
			oldDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			}),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			}),
		},
		{
			name:   "component topology constraint is immutable",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			}),
			wantErr: "spec.components[1].topologyConstraint: Invalid value",
		},
		{
			name: "component topology constraint change is immutable",
			oldDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{ClusterTopologyName: "grove-topology"}
				spec.Components[1].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "rack"}
			}),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.TopologyConstraint = &nvidiacomv1beta1.SpecTopologyConstraint{ClusterTopologyName: "grove-topology"}
				spec.Components[1].TopologyConstraint = &nvidiacomv1beta1.TopologyConstraint{PackDomain: "zone"}
			}),
			wantErr: "spec.components[1].topologyConstraint: Invalid value",
		},
		{
			name:   "kv transfer policy is immutable",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithKvTransferPolicy(&nvidiacomv1beta1.KvTransferPolicy{
				LabelKey: "topology.kubernetes.io/zone",
				Domain:   "zone",
			}),
			wantErr: "spec.experimental.kvTransferPolicy: Invalid value",
		},
		{
			name: "unchanged kv transfer policy is allowed",
			oldDGD: betaDGDWithKvTransferPolicy(&nvidiacomv1beta1.KvTransferPolicy{
				LabelKey: "topology.kubernetes.io/zone",
				Domain:   "zone",
			}),
			newDGD: betaDGDWithKvTransferPolicy(&nvidiacomv1beta1.KvTransferPolicy{
				LabelKey:    "topology.kubernetes.io/zone",
				Domain:      "zone",
				Enforcement: nvidiacomv1beta1.KvTransferEnforcementRequired,
			}),
		},
		{
			name:   "inter-pod GMS layout is immutable",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				enableBetaInterPodGMS(worker)
			}),
			wantErr: "spec.components[1].experimental.gpuMemoryService.mode: Invalid value",
		},
		{
			name: "inter-pod failover toggle is immutable",
			oldDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				enableBetaInterPodGMS(worker)
			}),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				enableBetaInterPodGMS(worker)
				enableBetaInterPodFailover(worker, 1)
			}),
			wantErr: "spec.components[1].experimental.failover: Invalid value",
		},
		{
			name: "inter-pod failover shadow count is immutable",
			oldDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				enableBetaInterPodGMS(worker)
				enableBetaInterPodFailover(worker, 1)
			}),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				enableBetaInterPodGMS(worker)
				enableBetaInterPodFailover(worker, 2)
			}),
			wantErr: "spec.components[1].experimental.failover.numShadows: Invalid value",
		},
		{
			name: "scaling adapter blocks direct replica changes",
			oldDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.ScalingAdapter = &nvidiacomv1beta1.ScalingAdapter{}
				worker.Replicas = k8sptr.To(int32(2))
			}),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.ScalingAdapter = &nvidiacomv1beta1.ScalingAdapter{}
				worker.Replicas = k8sptr.To(int32(3))
			}),
			userInfo: &authenticationv1.UserInfo{
				Username: "system:serviceaccount:default:regular-user",
			},
			principal: operatorPrincipal,
			wantErr:   "spec.components[1].replicas: Forbidden: cannot be modified directly when scaling adapter is enabled",
		},
		{
			name: "scaling adapter fails closed without user info",
			oldDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.ScalingAdapter = &nvidiacomv1beta1.ScalingAdapter{}
				worker.Replicas = k8sptr.To(int32(2))
			}),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.ScalingAdapter = &nvidiacomv1beta1.ScalingAdapter{}
				worker.Replicas = k8sptr.To(int32(3))
			}),
			principal: operatorPrincipal,
			wantErr:   "spec.components[1].replicas: Forbidden: cannot be modified directly when scaling adapter is enabled",
		},
		{
			name: "operator can change scaling-adapter-owned replicas",
			oldDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.ScalingAdapter = &nvidiacomv1beta1.ScalingAdapter{}
				worker.Replicas = k8sptr.To(int32(2))
			}),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.ScalingAdapter = &nvidiacomv1beta1.ScalingAdapter{}
				worker.Replicas = k8sptr.To(int32(3))
			}),
			userInfo: &authenticationv1.UserInfo{
				Username: operatorPrincipal,
			},
			principal: operatorPrincipal,
		},
		{
			name: "minAvailable immutability is owned by CEL",
			oldDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.MinAvailable = k8sptr.To(int32(1))
			}),
			newDGD: betaDGDWithWorker(func(worker *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
				worker.MinAvailable = k8sptr.To(int32(2))
			}),
		},
		{
			name:   "backend framework changes warn and fail",
			oldDGD: newBetaDGDForValidation(),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.BackendFramework = "sglang"
			}),
			wantErr:   "spec.backendFramework: Invalid value",
			wantWarns: true,
		},
		{
			name: "restart id cannot change during active rolling update",
			oldDGD: betaDGDWithStatus(
				func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
					spec.Restart = &nvidiacomv1beta1.Restart{ID: "old"}
				},
				func(status *nvidiacomv1beta1.DynamoGraphDeploymentStatus) {
					status.RollingUpdate = &nvidiacomv1beta1.RollingUpdateStatus{
						Phase: nvidiacomv1beta1.RollingUpdatePhaseInProgress,
					}
				},
			),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = &nvidiacomv1beta1.Restart{ID: "new"}
			}),
			wantErr: "spec.restart.id: Invalid value: \"new\": cannot be changed while a rolling update is InProgress",
		},
		{
			name: "restart id can stay unchanged during active rolling update",
			oldDGD: betaDGDWithStatus(
				func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
					spec.Restart = &nvidiacomv1beta1.Restart{ID: "same"}
				},
				func(status *nvidiacomv1beta1.DynamoGraphDeploymentStatus) {
					status.RollingUpdate = &nvidiacomv1beta1.RollingUpdateStatus{
						Phase: nvidiacomv1beta1.RollingUpdatePhaseInProgress,
					}
				},
			),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = &nvidiacomv1beta1.Restart{ID: "same"}
			}),
		},
		{
			name: "restart id can change after completed rolling update",
			oldDGD: betaDGDWithStatus(
				func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
					spec.Restart = &nvidiacomv1beta1.Restart{ID: "old"}
				},
				func(status *nvidiacomv1beta1.DynamoGraphDeploymentStatus) {
					status.RollingUpdate = &nvidiacomv1beta1.RollingUpdateStatus{
						Phase: nvidiacomv1beta1.RollingUpdatePhaseCompleted,
					}
				},
			),
			newDGD: betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
				spec.Restart = &nvidiacomv1beta1.Restart{ID: "new"}
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewDynamoGraphDeploymentValidator(nil, true)
			warnings, err := validator.ValidateUpdate(tt.oldDGD, tt.newDGD, tt.userInfo, tt.principal)
			assertBetaValidationError(t, err, tt.wantErr)
			if tt.wantWarns && len(warnings) == 0 {
				t.Fatal("ValidateUpdate() expected warnings but got none")
			}
			if !tt.wantWarns && len(warnings) != 0 {
				t.Fatalf("ValidateUpdate() unexpected warnings: %v", warnings)
			}
		})
	}
}

func newBetaDGDForValidation() *nvidiacomv1beta1.DynamoGraphDeployment {
	return &nvidiacomv1beta1.DynamoGraphDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-graph",
			Namespace: "default",
		},
		Spec: nvidiacomv1beta1.DynamoGraphDeploymentSpec{
			BackendFramework: "vllm",
			Components: []nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{
				{
					ComponentName: "frontend",
					ComponentType: nvidiacomv1beta1.ComponentTypeFrontend,
					Replicas:      k8sptr.To(int32(1)),
				},
				{
					ComponentName: "worker",
					ComponentType: nvidiacomv1beta1.ComponentTypeWorker,
					Replicas:      k8sptr.To(int32(2)),
				},
			},
		},
	}
}

func betaDGDFromAlpha(
	t *testing.T,
	mutate func(*nvidiacomv1alpha1.DynamoGraphDeployment),
) *nvidiacomv1beta1.DynamoGraphDeployment {
	t.Helper()

	alpha := newAlphaDGDForCompatibilityValidation()
	mutate(alpha)

	beta := &nvidiacomv1beta1.DynamoGraphDeployment{}
	if err := alpha.ConvertTo(beta); err != nil {
		t.Fatalf("ConvertTo() error = %v", err)
	}
	return beta
}

func newAlphaDGDForCompatibilityValidation() *nvidiacomv1alpha1.DynamoGraphDeployment {
	return &nvidiacomv1alpha1.DynamoGraphDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-graph",
			Namespace: "default",
		},
		Spec: nvidiacomv1alpha1.DynamoGraphDeploymentSpec{
			BackendFramework: "vllm",
			Services: map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
				"worker": {
					ComponentType: consts.ComponentTypeWorker,
					Replicas:      k8sptr.To(int32(1)),
				},
			},
		},
	}
}

func betaDGDWithSpec(
	mutate func(*nvidiacomv1beta1.DynamoGraphDeploymentSpec),
) *nvidiacomv1beta1.DynamoGraphDeployment {
	dgd := newBetaDGDForValidation()
	mutate(&dgd.Spec)
	return dgd
}

func betaDGDWithWorker(
	mutate func(*nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec),
) *nvidiacomv1beta1.DynamoGraphDeployment {
	return betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
		for i := range spec.Components {
			if spec.Components[i].ComponentName == "worker" {
				mutate(&spec.Components[i])
				return
			}
		}
	})
}

func betaDGDWithStatus(
	mutateSpec func(*nvidiacomv1beta1.DynamoGraphDeploymentSpec),
	mutateStatus func(*nvidiacomv1beta1.DynamoGraphDeploymentStatus),
) *nvidiacomv1beta1.DynamoGraphDeployment {
	dgd := betaDGDWithSpec(mutateSpec)
	mutateStatus(&dgd.Status)
	return dgd
}

func betaDGDWithKvTransferPolicy(
	policy *nvidiacomv1beta1.KvTransferPolicy,
) *nvidiacomv1beta1.DynamoGraphDeployment {
	return betaDGDWithSpec(func(spec *nvidiacomv1beta1.DynamoGraphDeploymentSpec) {
		spec.Experimental = &nvidiacomv1beta1.DynamoGraphDeploymentExperimentalSpec{
			KvTransferPolicy: policy,
		}
	})
}

func betaRestart(
	strategyType nvidiacomv1beta1.RestartStrategyType,
	order ...string,
) *nvidiacomv1beta1.Restart {
	return &nvidiacomv1beta1.Restart{
		ID: "roll",
		Strategy: &nvidiacomv1beta1.RestartStrategy{
			Type:  strategyType,
			Order: order,
		},
	}
}

func betaWorkerComponent(
	dgd *nvidiacomv1beta1.DynamoGraphDeployment,
) *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec {
	return dgd.GetComponentByName("worker")
}

func enableBetaContainerDiscovery(dgd *nvidiacomv1beta1.DynamoGraphDeployment) {
	dgd.Annotations = map[string]string{consts.KubeAnnotationDynamoKubeDiscoveryMode: "container"}
}

func enableBetaInterPodGMS(component *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
	component.Experimental = &nvidiacomv1beta1.ExperimentalSpec{
		GPUMemoryService: &nvidiacomv1beta1.GPUMemoryServiceSpec{
			Mode: nvidiacomv1beta1.GMSModeInterPod,
		},
	}
	component.PodTemplate = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: consts.MainContainerName,
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName(consts.KubeResourceGPUNvidia): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
}

func enableBetaIntraPodGMS(component *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec) {
	component.Experimental = &nvidiacomv1beta1.ExperimentalSpec{
		GPUMemoryService: &nvidiacomv1beta1.GPUMemoryServiceSpec{
			Mode: nvidiacomv1beta1.GMSModeIntraPod,
		},
	}
	component.PodTemplate = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: consts.MainContainerName,
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName(consts.KubeResourceGPUNvidia): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
}

func enableBetaInterPodFailover(
	component *nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec,
	numShadows int32,
) {
	if component.Experimental == nil {
		component.Experimental = &nvidiacomv1beta1.ExperimentalSpec{}
	}
	component.Experimental.Failover = &nvidiacomv1beta1.FailoverSpec{
		Mode:       nvidiacomv1beta1.GMSModeInterPod,
		NumShadows: numShadows,
	}
}

type fakeManager struct {
	ctrl.Manager // satisfies the rest of the interface; panics if unexpected methods are used
	client       client.Client
	config       *rest.Config
}

func (m *fakeManager) GetClient() client.Client { return m.client }
func (m *fakeManager) GetConfig() *rest.Config  { return m.config }

func newGroveTopologyTestManager(t *testing.T, objects ...runtime.Object) ctrl.Manager {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := grovev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Grove scheme: %v", err)
	}
	return &fakeManager{
		client: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build(),
		config: &rest.Config{},
	}
}

func newTestClusterTopology() *grovev1alpha1.ClusterTopology {
	return &grovev1alpha1.ClusterTopology{
		ObjectMeta: metav1.ObjectMeta{Name: "grove-topology"},
		Spec: grovev1alpha1.ClusterTopologySpec{
			Levels: []grovev1alpha1.TopologyLevel{
				{Domain: grovev1alpha1.TopologyDomainZone, Key: "topology.kubernetes.io/zone"},
				{Domain: grovev1alpha1.TopologyDomainRack, Key: "nvidia.com/rack"},
			},
		},
	}
}

func assertBetaValidationError(t *testing.T, err error, wantErr string) {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("expected error containing %q but got nil", wantErr)
	}
	statusErr, ok := err.(*k8serrors.StatusError)
	if !ok || !k8serrors.IsInvalid(err) {
		t.Fatalf("error = %T %v, want typed Kubernetes invalid error", err, err)
	}
	if statusErr.ErrStatus.Details == nil || len(statusErr.ErrStatus.Details.Causes) == 0 {
		t.Fatalf("error = %v, want at least one typed field cause", err)
	}
	for _, cause := range statusErr.ErrStatus.Details.Causes {
		if cause.Field == "" {
			t.Fatalf("error cause = %#v, want an exact field path", cause)
		}
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("error = %q, want to contain %q", err.Error(), wantErr)
	}
}

func assertWarningsContain(t *testing.T, warnings []string, want string) {
	t.Helper()
	for _, warning := range warnings {
		if strings.Contains(warning, want) {
			return
		}
	}
	t.Fatalf("warnings = %v, want one containing %q", warnings, want)
}
