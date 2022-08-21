/*
Copyright 2021 The Flux authors

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

package reconcile

import (
	"errors"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/runtime/conditions"
	"github.com/fluxcd/pkg/runtime/patch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	serror "github.com/fluxcd/source-controller/internal/error"
)

// Result is a type for creating an abstraction for the controller-runtime
// reconcile Result to simplify the Result values.
type Result int

const (
	// ResultEmpty indicates a reconcile result which does not requeue. It is
	// also used when returning an error, since the error overshadows result.
	ResultEmpty Result = iota
	// ResultRequeue indicates a reconcile result which should immediately
	// requeue.
	ResultRequeue
	// ResultSuccess indicates a reconcile success result.
	// For a reconciler that requeues regularly at a fixed interval, runtime
	// result with a fixed RequeueAfter is success result.
	// For a reconciler that doesn't requeue on successful reconciliation,
	// an empty runtime result is success result.
	// It is usually returned at the end of a reconciler/sub-reconciler.
	ResultSuccess
)

// RuntimeResultBuilder defines an interface for runtime result builders. This
// can be implemented to build custom results based on the context of the
// reconciler.
type RuntimeResultBuilder interface {
	// BuildRuntimeResult analyzes the result and error to return a runtime
	// result.
	BuildRuntimeResult(rr Result, err error) ctrl.Result
	// IsSuccess returns if a given runtime result is success for a
	// RuntimeResultBuilder.
	IsSuccess(ctrl.Result) bool
}

// AlwaysRequeueResultBuilder implements a RuntimeResultBuilder for always
// requeuing reconcilers. A successful reconciliation result for such
// reconcilers contains a fixed RequeueAfter value.
type AlwaysRequeueResultBuilder struct {
	// RequeueAfter is the fixed period at which the reconciler requeues on
	// successful execution.
	RequeueAfter time.Duration
}

// BuildRuntimeResult converts a given Result and error into the
// return values of a controller's Reconcile function.
func (r AlwaysRequeueResultBuilder) BuildRuntimeResult(rr Result, err error) ctrl.Result {
	// Handle special errors that contribute to expressing the result.
	switch e := err.(type) {
	case *serror.Waiting:
		// Safeguard: If no RequeueAfter is set, use the default success
		// RequeueAfter value to ensure a requeue takes place after some time.
		if e.RequeueAfter == 0 {
			return ctrl.Result{RequeueAfter: r.RequeueAfter}
		}
		return ctrl.Result{RequeueAfter: e.RequeueAfter}
	case *serror.Generic:
		// no-op error, reconcile at success interval.
		if e.Ignore {
			return ctrl.Result{RequeueAfter: r.RequeueAfter}
		}
	}

	switch rr {
	case ResultRequeue:
		return ctrl.Result{Requeue: true}
	case ResultSuccess:
		return ctrl.Result{RequeueAfter: r.RequeueAfter}
	default:
		return ctrl.Result{}
	}
}

// IsSuccess returns true if the given Result has the same RequeueAfter value
// as of the AlwaysRequeueResultBuilder.
func (r AlwaysRequeueResultBuilder) IsSuccess(result ctrl.Result) bool {
	return result.RequeueAfter == r.RequeueAfter
}

// ComputeReconcileResult analyzes the reconcile results (result + error),
// updates the status conditions of the object with any corrections and returns
// object patch configuration, runtime result and runtime error. The caller is
// responsible for using the patch configuration while patching the object in
// the API server.
// The RuntimeResultBuilder is used to define how the ctrl.Result is computed.
func ComputeReconcileResult(obj conditions.Setter, res Result, recErr error, rb RuntimeResultBuilder) ([]patch.Option, ctrl.Result, error) {
	var pOpts []patch.Option

	// Compute the runtime result.
	var result ctrl.Result
	if rb != nil {
		result = rb.BuildRuntimeResult(res, recErr)
	}

	// Remove reconciling condition on successful reconciliation.
	if recErr == nil && res == ResultSuccess {
		conditions.Delete(obj, meta.ReconcilingCondition)
	}

	// Analyze the reconcile error.
	switch t := recErr.(type) {
	case *serror.Stalling:
		if res == ResultEmpty {
			conditions.MarkStalled(obj, t.Reason, t.Error())
			// The current generation has been reconciled successfully and it
			// has resulted in a stalled state. Return no error to stop further
			// requeuing.
			pOpts = addPatchOptionWithStatusObservedGeneration(obj, pOpts)
			return pOpts, result, nil
		}
		// NOTE: Non-empty result with stalling error indicates that the
		// returned result is incorrect.
	case *serror.Waiting:
		// The reconcile resulted in waiting error, remove stalled condition if
		// present.
		conditions.Delete(obj, meta.StalledCondition)
		// The reconciler needs to wait and retry. Return no error.
		return pOpts, result, nil
	case *serror.Generic:
		conditions.Delete(obj, meta.StalledCondition)
		// If ignore, it's a no-op error, return no error, remove reconciling
		// condition.
		if t.Ignore {
			// The current generation has been reconciled successfully with
			// no-op result. Update status observed generation.
			pOpts = addPatchOptionWithStatusObservedGeneration(obj, pOpts)
			conditions.Delete(obj, meta.ReconcilingCondition)
			return pOpts, result, nil
		}
	case nil:
		// The reconcile didn't result in any error, we are not in stalled
		// state. If a requeue is requested, the current generation has not been
		// reconciled successfully.
		if res != ResultRequeue {
			pOpts = addPatchOptionWithStatusObservedGeneration(obj, pOpts)
		}
		conditions.Delete(obj, meta.StalledCondition)
	default:
		// The reconcile resulted in some error, but we are not in stalled
		// state.
		conditions.Delete(obj, meta.StalledCondition)
	}

	return pOpts, result, recErr
}

// LowestRequeuingResult returns the ReconcileResult with the lowest requeue
// period.
// Weightage:
//  ResultRequeue - immediate requeue (lowest)
//  ResultSuccess - requeue at an interval
//  ResultEmpty - no requeue
func LowestRequeuingResult(i, j Result) Result {
	switch {
	case i == ResultEmpty:
		return j
	case j == ResultEmpty:
		return i
	case i == ResultRequeue:
		return i
	case j == ResultRequeue:
		return j
	default:
		return j
	}
}

// FailureRecovery finds out if a failure recovery occurred by checking the fail
// conditions in the old object and the new object.
func FailureRecovery(oldObj, newObj conditions.Getter, failConditions []string) bool {
	failuresBefore := 0
	for _, failCondition := range failConditions {
		if conditions.Get(oldObj, failCondition) != nil {
			failuresBefore++
		}
		if conditions.Get(newObj, failCondition) != nil {
			// Short-circuit, there is failure now, can't be a recovery.
			return false
		}
	}
	return failuresBefore > 0
}

// addPatchOptionWithStatusObservedGeneration adds patch option
// WithStatusObservedGeneration to the provided patch option slice only if there
// is any condition present on the object, and returns it. This is necessary to
// prevent setting status observed generation without any effectual observation.
// An object must have some condition in the status if it has been observed.
// TODO: Move this to fluxcd/pkg/runtime/patch package after it has proven its
// need.
func addPatchOptionWithStatusObservedGeneration(obj conditions.Setter, opts []patch.Option) []patch.Option {
	if len(obj.GetConditions()) > 0 {
		opts = append(opts, patch.WithStatusObservedGeneration{})
	}
	return opts
}

// IsResultSuccess defines if a given ctrl.Result and error result in a
// successful reconciliation result.
type IsResultSuccess func(ctrl.Result, error) bool

// ComputeReconcileResultV2 computes the result of reconciliation. It takes
// ctrl.Result, error from the reconciliation, and a conditions.Setter with
// conditions, and analyzes them to return a reconciliation error. It mutates
// the object status conditions based on the input to ensure the conditions are
// summarized based on kstatus.
func ComputeReconcileResultV2(obj conditions.Setter, res ctrl.Result, recErr error, isSuccess IsResultSuccess, readySuccessMsg string) error {
	// If reconcile error isn't nil, a retry needs to be attempted. Since
	// it's not stalled situation, ensure Stalled condition is removed.
	if recErr != nil {
		conditions.Delete(obj, meta.StalledCondition)
	}

	// If Stalled=True, ensure Reconciling is removed.
	if sc := conditions.Get(obj, meta.StalledCondition); sc != nil && sc.Status == metav1.ConditionTrue {
		conditions.Delete(obj, meta.ReconcilingCondition)
	}

	// Since conditions.IsReady() depends on the values of Stalled and
	// Reconciling conditions, after resolving their values above, update Ready
	// condition based on the reconcile error.
	// 1. If there's an error and Ready condition is not present in the status,
	// 		set Ready=False with the error.
	// 2. If there's an error and Ready=True, mark Ready=False with the error.
	// This ensure any existing Ready=False value is not overwritten which may
	// contain precise reason for the failure condition. In absence of precise
	// reason, set a generic meta.FailedReason in the aforementioned conditions.
	if recErr != nil {
		if rd := conditions.Get(obj, meta.ReadyCondition); rd == nil {
			conditions.MarkFalse(obj, meta.ReadyCondition, meta.FailedReason, recErr.Error())
		} else if conditions.IsReady(obj) {
			conditions.MarkFalse(obj, meta.ReadyCondition, meta.FailedReason, recErr.Error())
		}
	}

	// If the result is success, ensure Reconciling is removed.
	// But if Ready!=True, set error value to be the Ready failure message.
	if isSuccess(res, recErr) {
		conditions.Delete(obj, meta.ReconcilingCondition)
		if ready := conditions.Get(obj, meta.ReadyCondition); ready != nil &&
			ready.Status == metav1.ConditionFalse && !conditions.IsStalled(obj) {
			recErr = errors.New(conditions.GetMessage(obj, meta.ReadyCondition))
		}
	} else {
		// ctrl.Result is expected to be zero when stalled. If the result isn't
		// zero and not success even without considering the error value, a
		// requeue is requested in the ctrl.Result, not a stalled situation.
		// Ensure Stalled condition is removed.
		if !res.IsZero() && !isSuccess(res, nil) {
			conditions.Delete(obj, meta.StalledCondition)
		}
		// If it's still Stalled, ensure Ready value matches with Stalled.
		if conditions.IsStalled(obj) {
			sc := conditions.Get(obj, meta.StalledCondition)
			conditions.MarkFalse(obj, meta.ReadyCondition, sc.Reason, sc.Message)
		}
		// TODO: When the Result requests a requeue and no Ready condition value
		// is set, the status condition won't have any Ready condition value.
		// It's difficult to assign a Ready condition value without an error or
		// an existing Reconciling condition.
		// Maybe add a default Ready=False value for safeguard in case this
		// situation becomes common.
	}

	// After the above, if it's still a successful reconciliation and it's not
	// reconciling or stalled, mark Ready=True.
	if isSuccess(res, recErr) && !conditions.IsReconciling(obj) && !conditions.IsStalled(obj) {
		conditions.MarkTrue(obj, meta.ReadyCondition, meta.SucceededReason, readySuccessMsg)
	}
	return recErr
}
