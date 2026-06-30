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

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// sharedValidation carries request-wide dependencies and accumulation used by
// validation for API types shared by multiple resources.
type sharedValidation struct {
	ctx      context.Context
	mgr      ctrl.Manager
	warnings admission.Warnings
}

func (v *sharedValidation) warn(message string) {
	v.warnings = append(v.warnings, message)
}

func (v *sharedValidation) warnf(format string, args ...any) {
	v.warn(fmt.Sprintf(format, args...))
}
