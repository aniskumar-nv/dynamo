# Validation package

## Structural validation

- Follow the Kubernetes API validation style: typed validators compose
  `field.ErrorList` through `*field.Path` and report all independent errors.
- Validation must follow the Go API type tree. Every API struct with custom
  semantics has one validator named after its exact Go type, for example
  `validateDynamoGraphDeploymentSpec`,
  `validateDynamoComponentDeploymentSharedSpec`, and
  `validateKvTransferPolicy`.
- A parent validates its own scalar fields and calls child validators in API
  declaration order. Slice paths use `Index(i)` and map paths use `Key(key)`;
  sort map keys before emitting errors.
- Seed root validation with the actual top-level field paths, such as
  `field.NewPath("metadata")` and `field.NewPath("spec")`, matching upstream
  Kubernetes validation. Do not invent a synthetic resource path or start
  child validation from an empty path.
- Treat embedded `metav1.ObjectMeta` as a structural child. Object metadata
  and annotation rules belong in `validateObjectMeta`, called with the
  `metadata` path.
- Do not name validators after a policy or implementation detail. Start with
  the lowest common API-type ancestor of the fields a rule relates, then keep
  the rule there when it coordinates siblings or needs broad aggregation.
  A child may own a rule when its invalid field and most of its logic are local
  to that type, and the required ancestor facts are cheap and clear to pass
  explicitly. Helpers for lookup, sorting, normalization, or deriving context
  are not validators and must not use a `validate` name.
- For Kubernetes-owned nested types, delegate to their Kubernetes validator at
  the exact field path instead of reimplementing their schema validation.

## Validator signatures and context

- Keep structural values first, followed by `fldPath`.
- Every structural `validate...` function returns `field.ErrorList`.
- Accumulate warnings during that same structural traversal through
  request-scoped receiver helpers named `warn` and `warnf`; do not return
  warnings through every validator signature or implement a second warning
  traversal.
- The primary API value and `fldPath` passed to a validator are non-nil
  invariants and must be documented on the function. Do not add defensive nil
  checks for required validator arguments.
- For an optional child pointer, the parent checks for nil and only then calls
  the child validator. Update parents likewise handle removal before calling a
  child update validator; when an old value may legitimately be absent, state
  that explicitly in the child validator's contract.
- Pass up to three ancestor-derived contextual values as direct, typed
  parameters.
- If a validator needs four or more such values, use one final, sparse,
  type-specific options struct named after that validator's API type.
- Define an options struct only when it is needed. It contains only data the
  current API value cannot derive for itself.
- Construct each child options struct afresh at the call site. Never copy,
  embed, mutate, or extend a parent options struct. Do not use a generic,
  accumulating validation-context bag.
- Keep request-wide immutable dependencies on the validator receiver: context,
  API reader/client, feature configuration, and caller identity. The receiver
  may also carry the warnings accumulated by `warn` and `warnf` because it is
  created once per request. Do not store the current API node, field path,
  derived traversal data, or accumulated errors on the receiver.
- Dependencies required by a validation path, including its context and
  manager/client, are non-nil construction invariants. Document and satisfy
  those invariants at the boundary; do not add nil fallbacks inside helpers.
- Update validators take `new`, `old`, and `fldPath` as their structural
  inputs. Apply the same direct-context/typed-options threshold afterward.
- When otherwise identical Go type names from another API version need a
  distinct validator, suffix the type name with the version, for example
  `validateVolumeMountV1alpha1`; do not prefix the version.
- Use the standard `k8s.io/utils/ptr` helpers such as `ptr.Deref` and
  `ptr.Equal` for simple pointer defaults and equality. Do not add one-line
  dereference or pointer-comparison helpers.

## Errors, warnings, and compatibility

- All `validate...` functions return `field.ErrorList`; do not return `error`,
  use `errors.Join`, or build field paths with `fmt.Sprintf`.
- Use typed Kubernetes errors (`field.Required`, `field.Invalid`,
  `field.Forbidden`, `field.NotSupported`, and immutable-field validation).
  The admission boundary converts the final error list to an API invalid error.
- Keep warnings outside the structural error-validation recursion.
- Keep v1beta1 and v1alpha1 validation recursions separate. Conversion
  compatibility is a separate boundary with explicit conversion/fidelity tests;
  do not build a parallel cross-version validator.

## DCD and DGD shared fields

- Intrinsic v1beta1 `DynamoComponentDeploymentSharedSpec` validation has one
  structural validator, used by both DCD and DGD recursion. Do not introduce a
  `SharedSpecValidator` wrapper or constructor.
- Rules involving parent-only data stay with the parent validator. For example,
  DGD generated-name limits, DGD backend selection, and graph-level topology
  constraints belong to the DGD tree, not to the shared-spec validator.

## Tests

- Test files mirror the API type tree and test structural validators directly.
- Assert typed errors and exact field paths, aggregation of independent errors,
  and deterministic ordering. Do not assert only rendered error strings.
- Every newly added nested API type must be directly checked by its parent or
  delegated to its exact-type validator.
