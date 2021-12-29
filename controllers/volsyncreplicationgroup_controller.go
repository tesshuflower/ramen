/*
Copyright 2021 The RamenDR authors.

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

package controllers

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"

	volrep "github.com/csi-addons/volume-replication-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	rmnutil "github.com/ramendr/ramen/controllers/util"
	"github.com/ramendr/ramen/controllers/volsync"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const vsrgFinalizerName = "volumereplicationgroups.ramendr.openshift.io/vsrg-protection"

// VolSyncReplicationGroupReconciler reconciles a VolSyncReplicationGroup object
type VolSyncReplicationGroupReconciler struct {
	client.Client
	APIReader     client.Reader
	Log           logr.Logger
	Scheme        *runtime.Scheme
	eventRecorder *rmnutil.EventReporter
}

// SetupWithManager sets up the controller with the Manager.
func (r *VolSyncReplicationGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	pvcPredicate := pvcPredicateFuncFoVolSync()
	pvcMapFun := handler.EnqueueRequestsFromMapFunc(handler.MapFunc(func(obj client.Object) []reconcile.Request {
		log := ctrl.Log.WithName("pvcmap").WithName("VolSyncReplicationGroup")

		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			log.Info("PersistentVolumeClaim(PVC) map function received non-PVC resource")

			return []reconcile.Request{}
		}

		return filterPVC(mgr, pvc,
			log.WithValues("pvc", types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}))
	}))

	r.eventRecorder = rmnutil.NewEventReporter(mgr.GetEventRecorderFor("controller_VolumeReplicationGroup"))

	r.Log.Info("Adding VolSyncReplicationGroup controller")

	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: getMaxConcurrentReconciles()}).
		For(&ramendrv1alpha1.VolSyncReplicationGroup{}).
		Watches(&source.Kind{Type: &corev1.PersistentVolumeClaim{}}, pvcMapFun, builder.WithPredicates(pvcPredicate)).
		Owns(&volsyncv1alpha1.ReplicationDestination{}).
		Owns(&volsyncv1alpha1.ReplicationSource{}).
		Complete(r)
}

func init() {
	// Register custom metrics with the global Prometheus registry here
}

// pvcPredicateFunc sends reconcile requests for create and delete events.
// For them the filtering of whether the pvc belongs to the any of the
// VolumeReplicationGroup CRs and identifying such a CR is done in the
// map function by comparing namespaces and labels.
// But for update of pvc, the reconcile request should be sent only for
// specific changes. Do that comparison here.
func pvcPredicateFuncFoVolSync() predicate.Funcs {
	pvcPredicate := predicate.Funcs{
		// NOTE: Create predicate is retained, to help with logging the event
		CreateFunc: func(e event.CreateEvent) bool {
			log := ctrl.Log.WithName("pvcmap").WithName("VolumeReplicationGroup")

			log.Info("Create event for PersistentVolumeClaim")

			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			log := ctrl.Log.WithName("pvcmap").WithName("VolumeReplicationGroup")
			oldPVC, ok := e.ObjectOld.DeepCopyObject().(*corev1.PersistentVolumeClaim)
			if !ok {
				log.Info("Failed to deep copy older PersistentVolumeClaim")

				return false
			}
			newPVC, ok := e.ObjectNew.DeepCopyObject().(*corev1.PersistentVolumeClaim)
			if !ok {
				log.Info("Failed to deep copy newer PersistentVolumeClaim")

				return false
			}

			log.Info("Update event for PersistentVolumeClaim")

			return updateEventDecisionForVolSync(oldPVC, newPVC, log)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// PVC deletion is held back till VRG deletion. This is to
			// avoid races between subscription deletion and updating
			// VRG state. If VRG state is not updated prior to subscription
			// cleanup, then PVC deletion (triggered by subscription
			// cleanup) would leaving behind VolRep resource with stale
			// state (as per the current VRG state).
			return false
		},
	}

	return pvcPredicate
}

func updateEventDecisionForVolSync(oldPVC *corev1.PersistentVolumeClaim,
	newPVC *corev1.PersistentVolumeClaim,
	log logr.Logger) bool {
	const requeue bool = true

	pvcNamespacedName := types.NamespacedName{Name: newPVC.Name, Namespace: newPVC.Namespace}
	predicateLog := log.WithValues("pvc", pvcNamespacedName.String())
	// If finalizers change then deep equal of spec fails to catch it, we may want more
	// conditions here, compare finalizers and also status.phase to catch bound PVCs
	if !reflect.DeepEqual(oldPVC.Spec, newPVC.Spec) {
		predicateLog.Info("Reconciling due to change in spec")

		return requeue
	}

	if oldPVC.Status.Phase != corev1.ClaimBound && newPVC.Status.Phase == corev1.ClaimBound {
		predicateLog.Info("Reconciling due to phase change", "oldPhase", oldPVC.Status.Phase,
			"newPhase", newPVC.Status.Phase)

		return requeue
	}

	predicateLog.Info("Not Requeuing", "oldPVC Phase", oldPVC.Status.Phase,
		"newPVC phase", newPVC.Status.Phase)

	return !requeue
}

//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=volsyncreplicationgroups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=volsyncreplicationgroups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=volsyncreplicationgroups/finalizers,verbs=update

//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots/finalizers,verbs=update

// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumereplicationclasses,verbs=get;list;watch

// +kubebuilder:rbac:groups=volsync.backube,resources=replicationdestinations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=events,verbs=get;create;patch;update
// +kubebuilder:rbac:groups="",namespace=system,resources=secrets,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VolSyncReplicationGroup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.10.0/pkg/reconcile
func (r *VolSyncReplicationGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("VolSyncReplicationGroup", req.NamespacedName)

	log.Info("Entering reconcile loop")

	defer log.Info("Exiting reconcile loop")

	v := VSRGInstance{
		reconciler:     r,
		ctx:            ctx,
		log:            log,
		instance:       &ramendrv1alpha1.VolSyncReplicationGroup{},
		pvcList:        &corev1.PersistentVolumeClaimList{},
		replClassList:  &volrep.VolumeReplicationClassList{},
		namespacedName: req.NamespacedName.String(),
	}

	// Fetch the VolSyncReplicationGroup instance
	if err := r.APIReader.Get(ctx, req.NamespacedName, v.instance); err != nil {
		if errors.IsNotFound(err) {
			log.Info("Resource not found")

			return ctrl.Result{}, nil
		}

		log.Error(err, "Failed to get resource")

		return ctrl.Result{}, fmt.Errorf("failed to reconcile VolSyncReplicationGroup (%v), %w",
			req.NamespacedName, err)
	}

	v.volSyncHandler = volsync.NewVSHandler(
		ctx, r.Client, log, v.instance, v.instance.Spec.SchedulingInterval)

	// Save a copy of the instance status to be used for the VolSync status update comparison
	v.instance.Status.DeepCopyInto(&v.savedInstanceStatus)

	if v.savedInstanceStatus.VolSyncPVCs == nil {
		v.savedInstanceStatus.VolSyncPVCs = []ramendrv1alpha1.VolSyncPVCInfo{}
	}

	return v.processVolSync()
}

type VSRGInstance struct {
	reconciler          *VolSyncReplicationGroupReconciler
	ctx                 context.Context
	log                 logr.Logger
	instance            *ramendrv1alpha1.VolSyncReplicationGroup
	savedInstanceStatus ramendrv1alpha1.VolSyncReplicationGroupStatus
	pvcList             *corev1.PersistentVolumeClaimList
	replClassList       *volrep.VolumeReplicationClassList
	//vrcUpdated          bool
	namespacedName string
	volSyncHandler *volsync.VSHandler
}

//TODO: rename processVSRG? or even just process() ?
func (v *VSRGInstance) processVolSync() (ctrl.Result, error) {
	v.initializeStatus()

	if err := v.validateVRGState(); err != nil {
		// record the event
		v.log.Error(err, "Failed to validate the spec state")
		rmnutil.ReportIfNotPresent(v.reconciler.eventRecorder, v.instance, corev1.EventTypeWarning,
			rmnutil.EventReasonValidationFailed, err.Error())

		msg := "VolSyncReplicationGroup state is invalid"
		setVRGDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVSRGStatus(); err != nil {
			v.log.Error(err, "Status update failed")
			// Since updating status failed, reconcile
			return ctrl.Result{Requeue: true}, nil
		}
		// No requeue, as there is no reconcile till user changes desired spec to a valid value
		return ctrl.Result{}, nil
	}

	if err := v.updatePVCList(); err != nil {
		v.log.Error(err, "Failed to update PersistentVolumeClaims for resource")

		rmnutil.ReportIfNotPresent(v.reconciler.eventRecorder, v.instance, corev1.EventTypeWarning,
			rmnutil.EventReasonValidationFailed, err.Error())

		msg := "Failed to get list of pvcs"
		setVRGDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVSRGStatus(); err != nil {
			v.log.Error(err, "VolSync Status update failed")
		}

		return ctrl.Result{Requeue: true}, nil
	}

	return v.processVSRGActions()
}

func (v *VSRGInstance) initializeStatus() {
	// create ProtectedPVCs map for status
	if v.instance.Status.VolSyncPVCs == nil {
		v.instance.Status.VolSyncPVCs = []ramendrv1alpha1.VolSyncPVCInfo{}

		// Set the VolSync conditions to unknown as nothing is known at this point
		msg := "Initializing VolSyncReplicationGroup"
		setVRGInitialCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)
	}
}

func (v *VSRGInstance) validateVRGState() error {
	if v.instance.Spec.ReplicationState != ramendrv1alpha1.Primary &&
		v.instance.Spec.ReplicationState != ramendrv1alpha1.Secondary {
		err := fmt.Errorf("invalid or unknown replication state detected (deleted %v, desired replicationState %v)",
			!v.instance.GetDeletionTimestamp().IsZero(),
			v.instance.Spec.ReplicationState)

		v.log.Error(err, "Invalid request detected")

		return err
	}

	return nil
}

func (v *VSRGInstance) processVSRGActions() (ctrl.Result, error) {
	v.log = v.log.WithName("VSRGInstance").WithValues("State", v.instance.Spec.ReplicationState)

	switch {
	case !v.instance.GetDeletionTimestamp().IsZero():
		v.log = v.log.WithValues("Finalize", true)

		return v.processForDeletion()
	case v.instance.Spec.ReplicationState == ramendrv1alpha1.Primary:
		return v.processAsPrimary()
	default: // Secondary, not primary and not deleted
		return v.processAsSecondary()
	}
}

func (v *VSRGInstance) validatePVExistence(pv *corev1.PersistentVolume) error {
	existingPV := &corev1.PersistentVolume{}

	err := v.reconciler.Get(v.ctx, types.NamespacedName{Name: pv.Name}, existingPV)
	if err != nil {
		return fmt.Errorf("failed to get existing PV (%w)", err)
	}

	if existingPV.ObjectMeta.Annotations == nil ||
		existingPV.ObjectMeta.Annotations[PVRestoreAnnotation] == "" {
		return fmt.Errorf("found PV object not restored by Ramen for PV %s", existingPV.Name)
	}

	// Should we check and see if PV in being deleted? Should we just treat it as exists
	// and then we don't care if deletion takes place later, which is what we do now?
	v.log.Info("PV exists and managed by Ramen", "PV", existingPV)

	return nil
}

// addPVRestoreAnnotation adds annotation to the PV indicating that the PV is restored by Ramen
func (v *VSRGInstance) addPVRestoreAnnotation(pv *corev1.PersistentVolume) {
	if pv.ObjectMeta.Annotations == nil {
		pv.ObjectMeta.Annotations = map[string]string{}
	}

	pv.ObjectMeta.Annotations[PVRestoreAnnotation] = "True"
}

// updatePVCList fetches and updates the PVC list to process for the current instance of VolSync
func (v *VSRGInstance) updatePVCList() error {
	labelSelector := v.instance.Spec.PVCSelector

	v.log.Info("Fetching PersistentVolumeClaims", "labeled", labels.Set(labelSelector.MatchLabels))
	listOptions := []client.ListOption{
		client.InNamespace(v.instance.Namespace),
		client.MatchingLabels(labelSelector.MatchLabels),
	}

	if err := v.reconciler.List(v.ctx, v.pvcList, listOptions...); err != nil {
		v.log.Error(err, "Failed to list PersistentVolumeClaims",
			"labeled", labels.Set(labelSelector.MatchLabels))

		return fmt.Errorf("failed to list PersistentVolumeClaims, %w", err)
	}

	v.log.Info("Found PersistentVolumeClaims", "count", len(v.pvcList.Items))

	return nil
}

func (v *VSRGInstance) updateReplicationClassList() error {
	labelSelector := v.instance.Spec.ReplicationClassSelector

	v.log.Info("Fetching VolumeReplicationClass", "labeled", labels.Set(labelSelector.MatchLabels))
	listOptions := []client.ListOption{
		client.MatchingLabels(labelSelector.MatchLabels),
	}

	if err := v.reconciler.List(v.ctx, v.replClassList, listOptions...); err != nil {
		v.log.Error(err, "Failed to list Replication Classes",
			"labeled", labels.Set(labelSelector.MatchLabels))

		return fmt.Errorf("failed to list Replication Classes, %w", err)
	}

	v.log.Info("Number of Replication Classes", "count", len(v.replClassList.Items))

	return nil
}

// finalizeVRG cleans up managed resources and removes the VolSync finalizer for resource deletion
func (v *VSRGInstance) processForDeletion() (ctrl.Result, error) {
	v.log.Info("Entering processing VolSyncReplicationGroup")

	defer v.log.Info("Exiting processing VolSyncReplicationGroup")

	if !containsString(v.instance.ObjectMeta.Finalizers, vsrgFinalizerName) {
		v.log.Info("Finalizer missing from resource", "finalizer", vsrgFinalizerName)

		return ctrl.Result{}, nil
	}

	if err := v.removeFinalizer(vsrgFinalizerName); err != nil {
		v.log.Info("Failed to remove finalizer", "finalizer", vsrgFinalizerName, "errorValue", err)

		return ctrl.Result{Requeue: true}, nil
	}

	rmnutil.ReportIfNotPresent(v.reconciler.eventRecorder, v.instance, corev1.EventTypeNormal,
		rmnutil.EventReasonDeleteSuccess, "Deletion Success")

	return ctrl.Result{}, nil
}

// reconcileVRsForDeletion cleans up VR resources managed by VolSync and also cleans up changes made to PVCs
// TODO: Currently removes VR requests unconditionally, needs to ensure it is managed by VolSync
func (v *VSRGInstance) reconcileVRsForDeletion() bool {
	v.log.Info("Successfully reconciled VolSync as Primary")

	return true
}

// processAsPrimary reconciles the current instance of VolSync as primary
func (v *VSRGInstance) processAsPrimary() (ctrl.Result, error) {
	v.log.Info("Entering processing VolSyncReplicationGroup")

	defer v.log.Info("Exiting processing VolSyncReplicationGroup")

	//TODO: this is done for processAsSecondary too - move to processVolSync() (parent func)
	if err := v.addFinalizer(vsrgFinalizerName); err != nil {
		v.log.Error(err, "Failed to add finalizer", "finalizer", vsrgFinalizerName)

		msg := "Failed to add finalizer to VolSyncReplicationGroup"
		setVRGDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVSRGStatus(); err != nil {
			v.log.Error(err, "VolSync Status update failed")
		}

		return ctrl.Result{}, err
	}

	// In failover scenario where this VSRG instance was previously a secondary - we will have RDSpec[] with current
	// replication destinations.  Now that this VSRG instance is primary - restore PVCS by examining the RDSpec[]
	if err := v.restorePVCs(); err != nil {
		v.log.Error(err, "Restoring PVCs failed")

		msg := fmt.Sprintf("Failed to restore PVCs (%v)", err.Error())
		setVRGClusterDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVSRGStatus(); err != nil {
			v.log.Error(err, "VolSync Status update failed")
		}

		return ctrl.Result{}, err
	}

	// Process as primary as per initial deploy
	if err := v.reconcileVolSyncAsPrimary(); err != nil {
		v.log.Error(err, "Failed to reconcile VolSyncAsPrimary")
		return ctrl.Result{}, err
	}

	requeue := false
	// If requeue is false, then VolSync was successfully processed as primary.
	// Hence the event to be generated is Success of type normal.
	// Expectation is that, if something failed and requeue is true, then
	// appropriate event might have been captured at the time of failure.
	if !requeue {
		rmnutil.ReportIfNotPresent(v.reconciler.eventRecorder, v.instance, corev1.EventTypeNormal,
			rmnutil.EventReasonPrimarySuccess, "Primary Success")
	}

	if err := v.updateVSRGStatus(); err != nil {
		requeue = true
	}

	if requeue {
		v.log.Info("Requeuing resource")

		return ctrl.Result{Requeue: requeue}, nil
	}

	return ctrl.Result{}, nil
}

func (v *VSRGInstance) restorePVCs() error {
	// TODO: refactor this per this comment: https://github.com/RamenDR/ramen/pull/197#discussion_r687246692
	clusterDataReady := findCondition(v.instance.Status.Conditions, VRGConditionTypeClusterDataReady)
	if clusterDataReady != nil && clusterDataReady.Status == metav1.ConditionTrue &&
		clusterDataReady.ObservedGeneration == v.instance.Generation {
		v.log.Info("VolSyncReplicationGroup's ClusterDataReady condition found. PVC restore must have already been applied")

		return nil
	}

	if len(v.instance.Spec.RDSpec) == 0 {
		v.log.Info("VolSyncReplicationGroup no RDSpec entries, no PVCs to restore")
		// No ReplicationDestinations (i.e. no PVCs) to restore
		return nil
	}

	msg := "Restoring PVC cluster data"
	setVRGClusterDataProgressingCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)
	v.log.Info("Restoring PVCs to this managed cluster.", "RDSpec", v.instance.Spec.RDSpec)

	success := true
	for _, rdSpec := range v.instance.Spec.RDSpec {
		//TODO: Restore volume - if failure, set success=false
		err := v.volSyncHandler.EnsurePVCfromRD(rdSpec)
		if err != nil {
			v.log.Error(err, "Unable to ensure PVC", "rdSpec", rdSpec)
			success = false
			continue // Keep trying to ensure PVCs for other rdSpec
		}

		//TODO: Need any status to indicate which PVCs we've restored? - overall clusterDataReady is set below already
	}

	if !success {
		return fmt.Errorf("failed to restorePVCs using RDInfos (%v)", v.instance.Spec.RDSpec)
	}

	msg = "PVC cluster data restored"
	setVRGClusterDataReadyCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)
	v.log.Info(msg, "RDSpec", v.instance.Spec.RDSpec)

	return nil
}

func (v *VSRGInstance) reconcileVolSyncAsPrimary() error {
	// Make sure status.VolSyncPVCs matches our pvcList (bound pvcs to protect)
	if len(v.pvcList.Items) != len(v.instance.Status.VolSyncPVCs) {
		v.updateInstanceStatus()
	}

	// Reconcile RSSpec (deletion or replication)
	for _, rsSpec := range v.instance.Spec.RSSpec {
		err := v.volSyncHandler.ReconcileRS(rsSpec)
		if err != nil {
			return err
		}
	}
	//TODO: cleanup any RS that is not in rsSpec

	v.log.Info("Successfully reconciled VolSync as Primary")
	return nil
}

func (v *VSRGInstance) updateInstanceStatus() {
	updateNeeded := false
	for idx := range v.pvcList.Items {
		pvc := &v.pvcList.Items[idx]

		if pvc.Status.Phase == corev1.ClaimBound {
			volSyncPVC := v.findProtectedPVC(pvc.Name)
			if volSyncPVC == nil {
				volSyncPVC := &ramendrv1alpha1.VolSyncPVCInfo{Name: pvc.Name,
					Capacity: pvc.Status.Capacity.Storage(), StorageClassName: pvc.Spec.StorageClassName}
				v.instance.Status.VolSyncPVCs = append(v.instance.Status.VolSyncPVCs, *volSyncPVC)
				updateNeeded = true
			}
		}
	}

	if len(v.pvcList.Items) == len(v.instance.Status.VolSyncPVCs) {
		msg := "PVCs in the VolSyncReplicationGroup are ready for use"
		setVRGAsPrimaryReadyCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)
	}

	if updateNeeded {
		v.updateVSRGStatus()
	}
}

// findProtectedPVC returns the &VRG.Status.ProtectedPVC[x] for the given pvcName
func (v *VSRGInstance) findProtectedPVC(pvcName string) *ramendrv1alpha1.VolSyncPVCInfo {
	for index := range v.instance.Status.VolSyncPVCs {
		protectedPVC := &v.instance.Status.VolSyncPVCs[index]
		if protectedPVC.Name == pvcName {
			return protectedPVC
		}
	}

	return nil
}

// processAsSecondary reconciles the current instance of VolSync as secondary
func (v *VSRGInstance) processAsSecondary() (ctrl.Result, error) {
	v.log.Info("Entering processing VolSyncReplicationGroup")

	defer v.log.Info("Exiting processing VolSyncReplicationGroup")

	if err := v.addFinalizer(vsrgFinalizerName); err != nil {
		v.log.Info("Failed to add finalizer", "finalizer", vsrgFinalizerName, "errorValue", err)

		msg := "Failed to add finalizer to VolSyncReplicationGroup"
		setVRGDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVSRGStatus(); err != nil {
			v.log.Error(err, "VolSync Status update failed")
		}

		return ctrl.Result{Requeue: true}, nil
	}

	if err := v.reconcileVolSyncAsSecondary(); err != nil {
		v.log.Error(err, "Failed to reconcile VolSyncAsSecondary")
		return ctrl.Result{}, err
	}

	requeue := false //FIXME: requeue stuff
	// If requeue is false, then VolSync was successfully processed as Secondary.
	// Hence the event to be generated is Success of type normal.
	// Expectation is that, if something failed and requeue is true, then
	// appropriate event might have been captured at the time of failure.
	if !requeue {
		rmnutil.ReportIfNotPresent(v.reconciler.eventRecorder, v.instance, corev1.EventTypeNormal,
			rmnutil.EventReasonSecondarySuccess, "Secondary Success")
	}

	if err := v.updateVSRGStatus(); err != nil {
		requeue = true
	}

	if requeue {
		v.log.Info("Requeuing resource")

		return ctrl.Result{Requeue: requeue}, nil
	}

	return ctrl.Result{}, nil
}

// reconcileVRsAsSecondary reconciles VolumeReplication resources for the VolSync as secondary
func (v *VSRGInstance) reconcileVolSyncAsSecondary() error {
	// Reconcile RDSpec (deletion or replication)
	for _, rdSpec := range v.instance.Spec.RDSpec {
		rdInfoForStatus, err := v.volSyncHandler.ReconcileRD(rdSpec)
		if err != nil {
			return err
		}
		if rdInfoForStatus != nil {
			// Update the VSRG status with this rdInfo
			v.updateStatusWithRDInfo(*rdInfoForStatus)
		}
	}
	//TODO: cleanup any RD that is not in rdSpec

	v.log.Info("Successfully reconciled VolSync as Secondary")
	return nil
}

func (v *VSRGInstance) updateStatusWithRDInfo(rdInfoForStatus ramendrv1alpha1.ReplicationDestinationInfo) {
	if v.instance.Status.RDInfo == nil {
		v.instance.Status.RDInfo = []ramendrv1alpha1.ReplicationDestinationInfo{}
	}

	found := false
	for i := range v.instance.Status.RDInfo {
		if v.instance.Status.RDInfo[i].PVCName == rdInfoForStatus.PVCName {
			// blindly replace with our updated RDInfo status
			v.instance.Status.RDInfo[i] = rdInfoForStatus
			found = true
			break
		}
	}
	if !found {
		// Append the new RDInfo to the status
		v.instance.Status.RDInfo = append(v.instance.Status.RDInfo, rdInfoForStatus)
	}
}

func (v *VSRGInstance) addFinalizer(finalizer string) error {
	if !controllerutil.ContainsFinalizer(v.instance, finalizer) {
		controllerutil.AddFinalizer(v.instance, finalizer)

		if err := v.reconciler.Update(v.ctx, v.instance); err != nil {
			v.log.Error(err, "Failed to add finalizer", "finalizer", finalizer)

			return fmt.Errorf("%w", err)
		}
	}

	return nil
}

// removeFinalizer removes VolSync finalizer form the resource
func (v *VSRGInstance) removeFinalizer(finalizer string) error {
	v.instance.ObjectMeta.Finalizers = removeString(v.instance.ObjectMeta.Finalizers, finalizer)
	if err := v.reconciler.Update(v.ctx, v.instance); err != nil {
		v.log.Error(err, "Failed to remove finalizer", "finalizer", finalizer)

		return fmt.Errorf("failed to remove finalizer from VolSyncReplicationGroup resource (%s/%s), %w",
			v.instance.Namespace, v.instance.Name, err)
	}

	return nil
}

func (v *VSRGInstance) updateVSRGStatus() error {
	v.log.Info("Updating VSRG status")

	v.instance.Status.State = getStatusStateFromSpecState(v.instance.Spec.ReplicationState)
	v.instance.Status.ObservedGeneration = v.instance.Generation

	if !reflect.DeepEqual(v.savedInstanceStatus, v.instance.Status) {
		v.instance.Status.LastUpdateTime = metav1.Now()
		if err := v.reconciler.Status().Update(v.ctx, v.instance); err != nil {
			v.log.Info(fmt.Sprintf("Failed to update VSRG status (%s/%s/%v)",
				v.instance.Name, v.instance.Namespace, err))

			return fmt.Errorf("failed to update VSRG status (%s/%s)", v.instance.Name, v.instance.Namespace)
		}

		v.log.Info(fmt.Sprintf("Updated VSRG Status %+v", v.instance.Status))

		return nil
	}

	v.log.Info(fmt.Sprintf("Nothing to update %+v", v.instance.Status))

	return nil
}
