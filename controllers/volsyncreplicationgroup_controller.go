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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	rmnutil "github.com/ramendr/ramen/controllers/util"
	"github.com/ramendr/ramen/controllers/volsync"
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
	pvcPredicate := pvcPredicateFunc()
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

	r.Log.Info("Adding VolumeReplicationGroup controller")

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

//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=volsyncreplicationgroups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=volsyncreplicationgroups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=volsyncreplicationgroups/finalizers,verbs=update

// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumereplicationclasses,verbs=get;list;watch
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

	v.volsyncReconciler = volsync.NewVolSyncReconciler(ctx, r.Client, log, v.instance)

	// Save a copy of the instance status to be used for the VolSync status update comparison
	v.instance.Status.DeepCopyInto(&v.savedInstanceStatus)

	if v.savedInstanceStatus.ProtectedPVCs == nil {
		v.savedInstanceStatus.ProtectedPVCs = []ramendrv1alpha1.ProtectedPVC{}
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
	namespacedName    string
	volsyncReconciler *volsync.VolSyncReconciler
}

func (v *VSRGInstance) processVolSync() (ctrl.Result, error) {
	v.initializeStatus()

	if err := v.validateVRGState(); err != nil {
		// record the event
		v.log.Error(err, "Failed to validate the spec state")
		rmnutil.ReportIfNotPresent(v.reconciler.eventRecorder, v.instance, corev1.EventTypeWarning,
			rmnutil.EventReasonValidationFailed, err.Error())

		msg := "VolSyncReplicationGroup state is invalid"
		setVRGDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVRGStatus(false); err != nil {
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

		if err = v.updateVRGStatus(false); err != nil {
			v.log.Error(err, "VolSync Status update failed")
		}

		return ctrl.Result{Requeue: true}, nil
	}

	return v.processVRGActions()
}

func (v *VSRGInstance) processVRGActions() (ctrl.Result, error) {
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

func (v *VSRGInstance) restorePVCs() error {
	// TODO: refactor this per this comment: https://github.com/RamenDR/ramen/pull/197#discussion_r687246692
	clusterDataReady := findCondition(v.instance.Status.Conditions, VRGConditionTypeClusterDataReady)
	if clusterDataReady != nil && clusterDataReady.Status == metav1.ConditionTrue &&
		clusterDataReady.ObservedGeneration == v.instance.Generation {
		v.log.Info("VolSyncReplicationGroup's ClusterDataReady condition found. PV restore must have already been applied")

		return nil
	}

	// if len(v.instance.Spec.S3Profiles) == 0 {
	// return fmt.Errorf("no S3Profiles configured")
	// }

	msg := "Restoring PV cluster data"
	setVRGClusterDataProgressingCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

	v.log.Info("Restoring PVCs to this managed cluster.", "RDSpec", v.instance.Spec.RDSpec)

	success := false

	for _, rdInfo := range v.instance.Spec.RDSpec {

		setVRGClusterDataReadyCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		v.log.Info("Restored PVC", "rdInfo", rdInfo)

		success = true

		break
	}

	if !success {
		return fmt.Errorf("failed to restorePVCs using RDSpec (%v)", v.instance.Spec.RDSpec)
	}

	return nil
}

// sanityCheckPVClusterData returns an error if there are PVs in the input
// pvList that have conflicting claimRefs that point to the same PVC name but
// different PVC UID.
//
// Under normal circumstances, each PV in the S3 store will point to a unique
// PVC and the sanity check will succeed.  In the case of failover related
// split-brain error scenarios, there can be multiple clusters that concurrently
// have the same VolSync in primary state.  During the split-brain scenario, if the
// VolSync is configured to use the same S3 store for both download and upload of
// cluster data and, if the application added a new PVC to the application on
// each cluster after failover, the S3 store could end up with multiple PVs for
// the same PVC because each of the clusters uploaded its unique PV to the S3
// store, thus resulting in ambiguous PVs for the same PVC.  If the S3 store
// ends up in such a situation, Ramen cannot determine with certainty which PV
// among the conflicting PVs should be restored to the cluster, and thus fails
// the sanity check.
func (v *VSRGInstance) sanityCheckPVClusterData(pvList []corev1.PersistentVolume) error {
	pvMap := map[string]corev1.PersistentVolume{}
	// Scan the PVs and create a map of PVs that have conflicting claimRefs
	for _, thisPV := range pvList {
		claimRef := thisPV.Spec.ClaimRef
		claimKey := fmt.Sprintf("%s/%s", claimRef.Namespace, claimRef.Name)

		prevPV, found := pvMap[claimKey]
		if !found {
			pvMap[claimKey] = thisPV

			continue
		}

		msg := fmt.Sprintf("when restoring PV cluster data, detected conflicting claimKey %s in PVs %s and %s",
			claimKey, prevPV.Name, thisPV.Name)
		v.log.Info(msg)

		return fmt.Errorf(msg)
	}

	return nil
}

func (v *VSRGInstance) restorePVClusterData(pvList []corev1.PersistentVolume) error {
	numRestored := 0

	for idx := range pvList {
		pv := &pvList[idx]
		v.cleanupPVForRestore(pv)
		v.addPVRestoreAnnotation(pv)

		if err := v.reconciler.Create(v.ctx, pv); err != nil {
			if errors.IsAlreadyExists(err) {
				err := v.validatePVExistence(pv)
				if err != nil {
					v.log.Info("PV exists. Ignoring and moving to next PV", "error", err.Error())
					// ignoring any errors
					continue
				}

				// Valid PV exists and it is managed by Ramen
				numRestored++

				continue
			}

			v.log.Info("Failed to restore PV", "name", pv.Name, "Error", err)

			continue
		}

		numRestored++
	}

	if numRestored != len(pvList) {
		return fmt.Errorf("failed to restore all PVs. Total %d. Restored %d", len(pvList), numRestored)
	}

	v.log.Info("Success restoring PVs", "Total", numRestored)

	return nil
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

// cleanupPVForRestore cleans up required PV fields, to ensure restore succeeds to a new cluster, and
// rebinding the PV to a newly created PVC with the same claimRef succeeds
func (v *VSRGInstance) cleanupPVForRestore(pv *corev1.PersistentVolume) {
	pv.ResourceVersion = ""
	if pv.Spec.ClaimRef != nil {
		pv.Spec.ClaimRef.UID = ""
		pv.Spec.ClaimRef.ResourceVersion = ""
		pv.Spec.ClaimRef.APIVersion = ""
	}
}

// addPVRestoreAnnotation adds annotation to the PV indicating that the PV is restored by Ramen
func (v *VSRGInstance) addPVRestoreAnnotation(pv *corev1.PersistentVolume) {
	if pv.ObjectMeta.Annotations == nil {
		pv.ObjectMeta.Annotations = map[string]string{}
	}

	pv.ObjectMeta.Annotations[PVRestoreAnnotation] = "True"
}

func (v *VSRGInstance) initializeStatus() {
	// create ProtectedPVCs map for status
	if v.instance.Status.ProtectedPVCs == nil {
		v.instance.Status.ProtectedPVCs = []ramendrv1alpha1.ProtectedPVC{}

		// Set the VolSync conditions to unknown as nothing is known at this point
		msg := "Initializing VolSyncReplicationGroup"
		setVRGInitialCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)
	}
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

	if !containsString(v.instance.ObjectMeta.Finalizers, vrgFinalizerName) {
		v.log.Info("Finalizer missing from resource", "finalizer", vrgFinalizerName)

		return ctrl.Result{}, nil
	}

	if err := v.removeFinalizer(vrgFinalizerName); err != nil {
		v.log.Info("Failed to remove finalizer", "finalizer", vrgFinalizerName, "errorValue", err)

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

// processAsPrimary reconciles the current instance of VolSync as primary
func (v *VSRGInstance) processAsPrimary() (ctrl.Result, error) {
	v.log.Info("Entering processing VolSyncReplicationGroup")

	defer v.log.Info("Exiting processing VolSyncReplicationGroup")

	if err := v.addFinalizer(vrgFinalizerName); err != nil {
		v.log.Info("Failed to add finalizer", "finalizer", vsrgFinalizerName, "errorValue", err)

		msg := "Failed to add finalizer to VolSyncReplicationGroup"
		setVRGDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVRGStatus(false); err != nil {
			v.log.Error(err, "VolSync Status update failed")
		}

		return ctrl.Result{Requeue: true}, nil
	}

	if err := v.restorePVCs(); err != nil {
		v.log.Info("Restoring PVCs failed", "errorValue", err)

		msg := fmt.Sprintf("Failed to restore PVCs (%v)", err.Error())
		setVRGClusterDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVRGStatus(false); err != nil {
			v.log.Error(err, "VolSync Status update failed")
		}

		// Since updating status failed, reconcile
		return ctrl.Result{Requeue: true}, nil
	}

	requeue := v.reconcileVolSyncAsPrimary()

	// If requeue is false, then VolSync was successfully processed as primary.
	// Hence the event to be generated is Success of type normal.
	// Expectation is that, if something failed and requeue is true, then
	// appropriate event might have been captured at the time of failure.
	if !requeue {
		rmnutil.ReportIfNotPresent(v.reconciler.eventRecorder, v.instance, corev1.EventTypeNormal,
			rmnutil.EventReasonPrimarySuccess, "Primary Success")
	}

	if err := v.updateVRGStatus(true); err != nil {
		requeue = true
	}

	if requeue {
		v.log.Info("Requeuing resource")

		return ctrl.Result{Requeue: requeue}, nil
	}

	return ctrl.Result{}, nil
}

func (v *VSRGInstance) reconcileVolSyncAsPrimary() bool {
	// 3. Reconcile RSInfo (deletion or Replication)

	v.log.Info("Successfully reconciled VolSync as Primary")

	return true
}

// processAsSecondary reconciles the current instance of VolSync as secondary
func (v *VSRGInstance) processAsSecondary() (ctrl.Result, error) {
	v.log.Info("Entering processing VolSyncReplicationGroup")

	defer v.log.Info("Exiting processing VolSyncReplicationGroup")

	if err := v.addFinalizer(vrgFinalizerName); err != nil {
		v.log.Info("Failed to add finalizer", "finalizer", vrgFinalizerName, "errorValue", err)

		msg := "Failed to add finalizer to VolSyncReplicationGroup"
		setVRGDataErrorCondition(&v.instance.Status.Conditions, v.instance.Generation, msg)

		if err = v.updateVRGStatus(false); err != nil {
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

	if err := v.updateVRGStatus(true); err != nil {
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
	// Reconcile RSInfo (deletion or replication)
	for _, rdSpec := range v.instance.Spec.RDSpec {
		rdInfoForStatus, err := v.volsyncReconciler.ReconcileRD(rdSpec)
		if err != nil {
			return err
		}
		if rdInfoForStatus != nil {
			// Update the VSRG status with this rdInfo
			v.updateStatusWithRDInfo(rdInfoForStatus)
		}
	}
	//TODO: cleanup any RD that is not in rdInfoSpec

	v.log.Info("Successfully reconciled VolSync as Secondary")

	return nil
}

func (v *VSRGInstance) updateVRGStatus(updateConditions bool) error {
	//TODO:
	return nil
}

func (v *VSRGInstance) addFinalizer(finalizer string) error {
	if !containsString(v.instance.ObjectMeta.Finalizers, finalizer) {
		v.instance.ObjectMeta.Finalizers = append(v.instance.ObjectMeta.Finalizers, finalizer)
		if err := v.reconciler.Update(v.ctx, v.instance); err != nil {
			v.log.Error(err, "Failed to add finalizer", "finalizer", finalizer)

			return fmt.Errorf("failed to add finalizer to VolSyncReplicationGroup resource (%s/%s), %w",
				v.instance.Namespace, v.instance.Name, err)
		}
	}

	return nil
}

func (v *VSRGInstance) updateStatusWithRDInfo(rdInfoForStatus *ramendrv1alpha1.ReplicationDestinationInfo) {
	if v.instance.Status.RDInfo == nil {
		v.instance.Status.RDInfo = []ramendrv1alpha1.ReplicationDestinationInfo{}
	}

	found := false
	for i := range v.instance.Status.RDInfo {
		if v.instance.Status.RDInfo[i].PVCName == rdInfoForStatus.PVCName {
			// blindly replace with our updated RDInfo status
			v.instance.Status.RDInfo[i] = *rdInfoForStatus
			found = true
			break
		}
	}
	if !found {
		// Append the new RDInfo to the status
		v.instance.Status.RDInfo = append(v.instance.Status.RDInfo, *rdInfoForStatus)
	}
}

func (v *VSRGInstance) updateStatusWithRSInfo(rsInfoForStatus *ramendrv1alpha1.ReplicationSourceInfo) {
	if v.instance.Status.RSInfo == nil {
		v.instance.Status.RSInfo = []ramendrv1alpha1.ReplicationSourceInfo{}
	}

	found := false
	for i := range v.instance.Status.RSInfo {
		if v.instance.Status.RSInfo[i].PVCName == rsInfoForStatus.PVCName {
			// blindly replace with our updated RSInfo status
			v.instance.Status.RSInfo[i] = *rsInfoForStatus
			found = true
			break
		}
	}
	if !found {
		// Append the new RSInfo to the status
		v.instance.Status.RSInfo = append(v.instance.Status.RSInfo, *rsInfoForStatus)
	}
}
