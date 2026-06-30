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

	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestDynamoGraphDeploymentHandlerValidateCreate(t *testing.T) {
	handler := NewDynamoGraphDeploymentHandler(nil, "system:serviceaccount:dynamo:dynamo-operator", false)
	dgd := newBetaDGDForValidation()

	warnings, err := handler.ValidateCreate(dgdAdmissionContext(admissionv1.Create, nvidiacomv1beta1.DynamoGraphDeploymentGVK), dgd)
	if err != nil {
		t.Fatalf("ValidateCreate() error = %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("ValidateCreate() warnings = %v, want none", warnings)
	}

	_, err = handler.ValidateCreate(context.Background(), dgd)
	if err == nil || !strings.Contains(err.Error(), "admission request missing from context") {
		t.Fatalf("ValidateCreate() error = %v, want missing admission request", err)
	}

	_, err = handler.ValidateCreate(
		dgdAdmissionContext(admissionv1.Create, schema.GroupVersionKind{Group: "wrong.example.com", Version: "v1", Kind: "Wrong"}),
		dgd,
	)
	if err == nil || !strings.Contains(err.Error(), "admission requires") {
		t.Fatalf("ValidateCreate() error = %v, want GVK mismatch", err)
	}

	_, err = handler.ValidateCreate(
		dgdAdmissionContext(admissionv1.Create, nvidiacomv1beta1.DynamoGraphDeploymentGVK),
		&runtime.Unknown{},
	)
	if err == nil || !strings.Contains(err.Error(), "expected DynamoGraphDeployment") {
		t.Fatalf("ValidateCreate() error = %v, want type mismatch", err)
	}
}

func TestDynamoGraphDeploymentHandlerValidateUpdate(t *testing.T) {
	handler := NewDynamoGraphDeploymentHandler(nil, "system:serviceaccount:dynamo:dynamo-operator", false)
	ctx := dgdAdmissionContext(admissionv1.Update, nvidiacomv1beta1.DynamoGraphDeploymentGVK)

	t.Run("valid", func(t *testing.T) {
		oldDGD := newBetaDGDForValidation()
		newDGD := oldDGD.DeepCopy()
		warnings, err := handler.ValidateUpdate(ctx, oldDGD, newDGD)
		if err != nil {
			t.Fatalf("ValidateUpdate() error = %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("ValidateUpdate() warnings = %v, want none", warnings)
		}
	})

	t.Run("deleting", func(t *testing.T) {
		oldDGD := newBetaDGDForValidation()
		newDGD := oldDGD.DeepCopy()
		now := metav1.Now()
		newDGD.DeletionTimestamp = &now
		if _, err := handler.ValidateUpdate(ctx, &runtime.Unknown{}, newDGD); err != nil {
			t.Fatalf("ValidateUpdate() error = %v", err)
		}
	})

	t.Run("invalid new object", func(t *testing.T) {
		_, err := handler.ValidateUpdate(ctx, newBetaDGDForValidation(), &runtime.Unknown{})
		if err == nil || !strings.Contains(err.Error(), "expected DynamoGraphDeployment") {
			t.Fatalf("ValidateUpdate() error = %v, want new object type mismatch", err)
		}
	})

	t.Run("invalid old object", func(t *testing.T) {
		_, err := handler.ValidateUpdate(ctx, &runtime.Unknown{}, newBetaDGDForValidation())
		if err == nil || !strings.Contains(err.Error(), "expected DynamoGraphDeployment") {
			t.Fatalf("ValidateUpdate() error = %v, want old object type mismatch", err)
		}
	})

	t.Run("stateless validation failure", func(t *testing.T) {
		invalid := newBetaDGDForValidation()
		invalid.Spec.Components = nil
		_, err := handler.ValidateUpdate(ctx, newBetaDGDForValidation(), invalid)
		if err == nil || !strings.Contains(err.Error(), "must have at least one component") {
			t.Fatalf("ValidateUpdate() error = %v, want stateless validation failure", err)
		}
	})

	t.Run("stateful validation failure", func(t *testing.T) {
		oldDGD := newBetaDGDForValidation()
		newDGD := oldDGD.DeepCopy()
		oldDGD.Spec.BackendFramework = "vllm"
		newDGD.Spec.BackendFramework = sglangBackendFramework
		_, err := handler.ValidateUpdate(ctx, oldDGD, newDGD)
		if err == nil || !strings.Contains(err.Error(), "backendFramework") {
			t.Fatalf("ValidateUpdate() error = %v, want stateful validation failure", err)
		}
	})
}

func TestDynamoGraphDeploymentHandlerValidateDelete(t *testing.T) {
	handler := NewDynamoGraphDeploymentHandler(nil, "", false)
	ctx := dgdAdmissionContext(admissionv1.Delete, nvidiacomv1beta1.DynamoGraphDeploymentGVK)

	warnings, err := handler.ValidateDelete(ctx, newBetaDGDForValidation())
	if err != nil {
		t.Fatalf("ValidateDelete() error = %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("ValidateDelete() warnings = %v, want none", warnings)
	}

	_, err = handler.ValidateDelete(ctx, &runtime.Unknown{})
	if err == nil || !strings.Contains(err.Error(), "expected DynamoGraphDeployment") {
		t.Fatalf("ValidateDelete() error = %v, want type mismatch", err)
	}
}

func TestCastToDynamoGraphDeployment(t *testing.T) {
	dgd := newBetaDGDForValidation()
	got, err := castToDynamoGraphDeployment(dgd)
	if err != nil || got != dgd {
		t.Fatalf("castToDynamoGraphDeployment() = (%v, %v), want original DGD", got, err)
	}

	if _, err := castToDynamoGraphDeployment(nil); err == nil {
		t.Fatal("castToDynamoGraphDeployment() error = nil, want type mismatch")
	}
}

func dgdAdmissionContext(operation admissionv1.Operation, gvk schema.GroupVersionKind) context.Context {
	return admission.NewContextWithRequest(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: operation,
			Kind: metav1.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind,
			},
		},
	})
}
