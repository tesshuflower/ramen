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

package volsync

import (
	"context"
	"fmt"
	"strings"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/go-logr/logr"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	VolumeSnapshotKind    string = "VolumeSnapshot"
	VolumeSnapshotGroup   string = "snapshot.storage.k8s.io"
	VolumeSnapshotVersion string = "v1"
	ServiceExportKind     string = "ServiceExport"
	ServiceExportGroup    string = "multicluster.x-k8s.io"
	ServiceExportVersion  string = "v1alpha1"
)

const (
	VolumeSnapshotProtectFinalizerName string = "volsyncreplicationgroups.ramendr.openshift.io/volumesnapshot-protection"
	VSRGReplicationSourceLabel         string = "volsyncreplicationgroup-owner"
	FinalSyncTriggerString             string = "vsrg-final-sync"
)

type VSHandler struct {
	ctx                context.Context
	client             client.Client
	log                logr.Logger
	owner              metav1.Object
	schedulingInterval string
	volSyncProfile     *ramendrv1alpha1.VolSyncProfile
	volSyncPVCs        []corev1.PersistentVolumeClaim
}

func NewVSHandler(ctx context.Context, client client.Client, log logr.Logger, owner metav1.Object,
	schedulingInterval string, volSyncProfile *ramendrv1alpha1.VolSyncProfile) *VSHandler {
	return &VSHandler{
		ctx:                ctx,
		client:             client,
		log:                log,
		owner:              owner,
		schedulingInterval: schedulingInterval,
		volSyncProfile:     volSyncProfile,
		volSyncPVCs:        []corev1.PersistentVolumeClaim{},
	}
}

func (v *VSHandler) ReconcileRD(
	rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec) (*ramendrv1alpha1.VolSyncReplicationDestinationInfo, error) {

	l := v.log.WithValues("rdSpec", rdSpec)

	if !rdSpec.ProtectedPVC.ProtectedByVolSync {
		return nil, fmt.Errorf("protectedPVC %s is not VolSync Enabled", rdSpec.ProtectedPVC.Name)
	}

	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getReplicationDestinationName(rdSpec),
			Namespace: v.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, rd, func() error {
		if err := ctrl.SetControllerReference(v.owner, rd, v.client.Scheme()); err != nil {
			l.Error(err, "unable to set controller reference")
			return err
		}

		addVSRGOwnerLabel(v.owner, rd)

		// Pre-allocated shared secret
		var sshKeys *string
		if rdSpec.SSHKeys != "" {
			// If SSHKeys is not specified, RD will create its own secret
			sshKeys = &rdSpec.SSHKeys
		}

		//TODO: VolumeSnapshotClassName

		rd.Spec.Rsync = &volsyncv1alpha1.ReplicationDestinationRsyncSpec{
			ServiceType: v.getRsyncServiceType(),
			SSHKeys:     sshKeys,

			ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
				CopyMethod:       volsyncv1alpha1.CopyMethodSnapshot,
				Capacity:         rdSpec.ProtectedPVC.Resources.Requests.Storage(),
				StorageClassName: rdSpec.ProtectedPVC.StorageClassName,
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	l.V(1).Info("ReplicationDestination createOrUpdate Complete", "op", op)

	err = v.reconcileServiceExportForRD(rd)
	if err != nil {
		return nil, err
	}

	//
	// Now check status - only return an RDInfo if we have an address filled out in the ReplicationDestination Status
	//
	if rd.Status == nil || rd.Status.Rsync == nil || rd.Status.Rsync.Address == nil {
		l.V(1).Info("ReplicationDestination waiting for Address ...")
		return nil, nil
	}

	l.V(1).Info("ReplicationDestination Reconcile Complete")
	return &ramendrv1alpha1.VolSyncReplicationDestinationInfo{
		PVCName: rdSpec.ProtectedPVC.Name,
		Address: *rd.Status.Rsync.Address,
	}, nil
}

// Returns true only if runFinalSynchronization was true and the final sync is done
func (v *VSHandler) ReconcileRS(
	rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec, runFinalSynchronization bool) (bool, error) {
	l := v.log.WithValues("rsSpec", rsSpec)

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getReplicationSourceName(rsSpec),
			Namespace: v.owner.GetNamespace(),
		},
	}

	//FIXME: getVolumeSnapshotClass
	//rsSpec

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, rs, func() error {
		if err := ctrl.SetControllerReference(v.owner, rs, v.client.Scheme()); err != nil {
			l.Error(err, "unable to set controller reference")
			return err
		}

		addVSRGOwnerLabel(v.owner, rs)

		rs.Spec.SourcePVC = rsSpec.PVCName

		if runFinalSynchronization {
			l.V(1).Info("ReplicationSource - final sync")
			// Change the schedule to instead use a keyword trigger - to trigger
			// a final sync to happen
			rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{
				Manual: FinalSyncTriggerString,
			}
		} else {
			// Set schedule
			scheduleCronSpec, err := v.getScheduleCronSpec()
			if err != nil {
				l.Error(err, "unable to parse schedulingInterval")
				return err
			}
			rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{
				Schedule: scheduleCronSpec,
			}
		}

		//TODO: VolumeSnapshotClassName or potentially Clone in some cases?

		rs.Spec.Rsync = &volsyncv1alpha1.ReplicationSourceRsyncSpec{
			SSHKeys: &rsSpec.SSHKeys,
			Address: &rsSpec.Address,

			ReplicationSourceVolumeOptions: volsyncv1alpha1.ReplicationSourceVolumeOptions{
				CopyMethod: volsyncv1alpha1.CopyMethodSnapshot,
			},
		}

		return nil
	})

	l.V(1).Info("ReplicationSource createOrUpdate Complete", "op", op)
	if err != nil {
		return false, err
	}

	//
	// For final sync only - check status to make sure the final sync is complete
	//
	if runFinalSynchronization {
		if rs.Status == nil || rs.Status.LastManualSync != FinalSyncTriggerString {
			l.V(1).Info("ReplicationSource running final sync - waiting for status to mark completion ...")
			return false, nil
		}
		l.V(1).Info("ReplicationSource final sync comple")
		return true, nil
	}

	l.V(1).Info("ReplicationSource Reconcile Complete")
	return false, nil
}

func (v *VSHandler) DeleteRS(rsName string) error {
	// Remove any ReplicationSource owned (by parent vsrg owner) that is not in the provided rsSpecList
	currentRSListByOwner, err := v.listRSByOwner()
	if err != nil {
		return err
	}

	for _, rs := range currentRSListByOwner.Items {
		if rs.GetName() == rsName {
			// Delete the ReplicationSource, log errors with cleanup but continue on
			if err := v.client.Delete(v.ctx, &rs); err != nil {
				v.log.Error(err, "Error cleaning up ReplicationSource", "name", rs.GetName())
			} else {
				v.log.Info("Deleted ReplicationSource", "name", rs.GetName())
			}
		}
	}

	return nil
}

func (v *VSHandler) CleanupRDNotInSpecList(rdSpecList []ramendrv1alpha1.VolSyncReplicationDestinationSpec) error {
	// Remove any ReplicationDestination owned (by parent vsrg owner) that is not in the provided rdSpecList
	currentRDListByOwner, err := v.listRDByOwner()
	if err != nil {
		return err
	}
	for _, rd := range currentRDListByOwner.Items {
		foundInSpecList := false
		for _, rdSpec := range rdSpecList {
			if rd.GetName() == getReplicationDestinationName(rdSpec) {
				foundInSpecList = true
				break
			}
		}
		if !foundInSpecList {
			// Delete the ReplicationDestination, log errors with cleanup but continue on
			if err := v.client.Delete(v.ctx, &rd); err != nil {
				v.log.Error(err, "Error cleaning up ReplicationDestination", "name", rd.GetName())
			} else {
				v.log.Info("Deleted ReplicationDestination", "name", rd.GetName())
			}
		}
	}

	return nil
}

// Make sure a ServiceExport exists to export the service for this RD to remote clusters
// See: https://access.redhat.com/documentation/en-us/red_hat_advanced_cluster_management_for_kubernetes/2.4/html/services/services-overview#enable-service-discovery-submariner
func (v *VSHandler) reconcileServiceExportForRD(rd *volsyncv1alpha1.ReplicationDestination) error {
	// Using unstructured to avoid needing to require serviceexport in client scheme
	svcExport := &unstructured.Unstructured{}
	svcExport.Object = map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      getLocalServiceNameForRD(rd.GetName()), // Get name of the local service (this needs to be exported)
			"namespace": rd.GetNamespace(),
		},
	}
	svcExport.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   ServiceExportGroup,
		Kind:    ServiceExportKind,
		Version: ServiceExportVersion,
	})

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, svcExport, func() error {
		// Make this ServiceExport owned by the replication destination itself rather than the VRG
		// This way on relocate scenarios or failover/failback, when the RD is cleaned up the associated
		// ServiceExport will get cleaned up with it.
		if err := ctrl.SetControllerReference(v.owner, svcExport, v.client.Scheme()); err != nil {
			v.log.Error(err, "unable to set controller reference", "resource", svcExport)
			return err
		}

		return nil
	})

	v.log.V(1).Info("ServiceExport createOrUpdate Complete", "op", op)
	if err != nil {
		v.log.Error(err, "error creating or updating ServiceExport", "replication destination name", rd.GetName(),
			"namespace", rd.GetNamespace())
		return err
	}

	v.log.V(1).Info("ServiceExport Reconcile Complete")
	return nil
}

func (v *VSHandler) listRSByOwner() (volsyncv1alpha1.ReplicationSourceList, error) {
	rsList := volsyncv1alpha1.ReplicationSourceList{}
	if err := v.listByOwner(&rsList); err != nil {
		v.log.Error(err, "Failed to list ReplicationSources for VSRG", "vsrg name", v.owner.GetName())
		return rsList, err
	}
	return rsList, nil
}

func (v *VSHandler) listRDByOwner() (volsyncv1alpha1.ReplicationDestinationList, error) {
	rdList := volsyncv1alpha1.ReplicationDestinationList{}
	if err := v.listByOwner(&rdList); err != nil {
		v.log.Error(err, "Failed to list ReplicationDestinations for VSRG", "vsrg name", v.owner.GetName())
		return rdList, err
	}
	return rdList, nil
}

// Lists only RS/RD with VSRGReplicationSourceLabel that matches the owner
func (v *VSHandler) listByOwner(list client.ObjectList) error {
	matchLabels := map[string]string{
		VSRGReplicationSourceLabel: v.owner.GetName(),
	}
	listOptions := []client.ListOption{
		client.InNamespace(v.owner.GetNamespace()),
		client.MatchingLabels(matchLabels),
	}

	if err := v.client.List(v.ctx, list, listOptions...); err != nil {
		v.log.Error(err, "Failed to list by label", "matchLabels", matchLabels)
		return err
	}

	return nil
}

func (v *VSHandler) EnsurePVCfromRD(rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec) error {
	l := v.log.WithValues("rdSpec", rdSpec)

	// Get RD instance
	rdInst := &volsyncv1alpha1.ReplicationDestination{}
	err := v.client.Get(v.ctx,
		types.NamespacedName{
			Name:      getReplicationDestinationName(rdSpec),
			Namespace: v.owner.GetNamespace(),
		}, rdInst)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			l.Error(err, "Failed to get ReplicationDestination")
			return err
		}
		// If not found, nothing to restore
		l.Info("No ReplicationDestination found, not restoring PVC for this rdSpec")
		return nil
	}

	var latestImage *corev1.TypedLocalObjectReference
	if rdInst.Status != nil {
		latestImage = rdInst.Status.LatestImage
	}
	if latestImage == nil || latestImage.Name == "" || latestImage.Kind != VolumeSnapshotKind {
		noSnapErr := fmt.Errorf("unable to find LatestImage from ReplicationDestination %s", rdInst.GetName())
		l.Error(noSnapErr, "No latestImage")
		return noSnapErr
	}

	// Make copy of the ref and make sure API group is filled out correctly (shouldn't really need this part)
	vsImageRef := latestImage.DeepCopy()
	if vsImageRef.APIGroup == nil || *vsImageRef.APIGroup == "" {
		vsGroup := VolumeSnapshotGroup
		vsImageRef.APIGroup = &vsGroup
	}
	l.V(1).Info("Latest Image for ReplicationDestination", "latestImage	", vsImageRef)

	if err := v.validateSnapshotAndAddFinalizer(*vsImageRef); err != nil {
		return err
	}

	return v.ensurePVCFromSnapshot(rdSpec, *vsImageRef)
}

func (v *VSHandler) ensurePVCFromSnapshot(rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec,
	snapshotRef corev1.TypedLocalObjectReference) error {
	l := v.log.WithValues("pvcName", rdSpec.ProtectedPVC.Name, "snapshotRef", snapshotRef)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rdSpec.ProtectedPVC.Name,
			Namespace: v.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, pvc, func() error {
		//TODO: confirm we want to do this - likely we want the users app to take over ownership
		if err := ctrl.SetControllerReference(v.owner, pvc, v.client.Scheme()); err != nil {
			v.log.Error(err, "unable to set controller reference")
			return err
		}

		//TODO: needs finalizer?  r.addFinalizer(pvc, pvcFinalizerName)

		if pvc.Status.Phase == corev1.ClaimBound {
			// Assume no changes are required
			l.V(1).Info("PVC already bound")
			return nil
		}

		//TODO: pvc.Labels = rdSpec.Labels

		accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce} // Default value
		if len(rdSpec.ProtectedPVC.AccessModes) > 0 {
			accessModes = rdSpec.ProtectedPVC.AccessModes
		}

		if pvc.CreationTimestamp.IsZero() { // set immutable fields
			pvc.Spec.AccessModes = accessModes
			pvc.Spec.StorageClassName = rdSpec.ProtectedPVC.StorageClassName

			// Only set when initially creating
			pvc.Spec.DataSource = &snapshotRef
		}

		pvc.Spec.Resources = rdSpec.ProtectedPVC.Resources

		return nil
	})

	if err != nil {
		l.Error(err, "Unable to createOrUpdate PVC from snapshot")
		return err
	}

	l.V(1).Info("PVC createOrUpdate Complete", "op", op)
	return nil
}

func (v *VSHandler) validateSnapshotAndAddFinalizer(volumeSnapshotRef corev1.TypedLocalObjectReference) error {
	// Using unstructured to avoid needing to require VolumeSnapshot in client scheme
	volSnap := &unstructured.Unstructured{}
	volSnap.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   *volumeSnapshotRef.APIGroup,
		Kind:    volumeSnapshotRef.Kind,
		Version: VolumeSnapshotVersion,
	})
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      volumeSnapshotRef.Name,
		Namespace: v.owner.GetNamespace(),
	}, volSnap)

	if err != nil {
		v.log.Error(err, "Unable to get VolumeSnapshot", "volumeSnapshotRef", volumeSnapshotRef)
		return err
	}

	if err := v.addFinalizerAndUpdate(volSnap, VolumeSnapshotProtectFinalizerName); err != nil {
		v.log.Error(err, "Unable to add finalizer to VolumeSnapshot", "volumeSnapshotRef", volumeSnapshotRef)
		return err
	}

	v.log.V(1).Info("VolumeSnapshot validated and protected with finalizer", "volumeSnapshotRef", volumeSnapshotRef)
	return nil
}

func (v *VSHandler) addFinalizer(obj client.Object, finalizer string) (updated bool) {
	updated = false
	if !ctrlutil.ContainsFinalizer(obj, finalizer) {
		ctrlutil.AddFinalizer(obj, finalizer)
		updated = true
	}
	return updated
}

func (v *VSHandler) addFinalizerAndUpdate(obj client.Object, finalizer string) error {
	if v.addFinalizer(obj, finalizer) {
		if err := v.client.Update(v.ctx, obj); err != nil {
			v.log.Error(err, "Failed to add finalizer", "finalizer", finalizer)
			return fmt.Errorf("%w", err)
		}
	}
	return nil
}

func (v *VSHandler) getRsyncServiceType() *corev1.ServiceType {
	if v.volSyncProfile != nil && v.volSyncProfile.ServiceType != nil {
		return v.volSyncProfile.ServiceType
	}
	// If the service type to use is not in the volsyncprofile (contained in the ramenconfig), then use the default
	return &DefaultRsyncServiceType
}

/*
func (v *VSHandler) getVolumeSnapshotClassFromStorageClass(storageClass string) error {
	v.log.Info("Fetching VolumeReplicationClasses")
	volumeSnapshotClassList := snapshots.Volu
	if err := v.reconciler.List(v.ctx, v.replClassList, listOptions...); err != nil {
		v.log.Error(err, "Failed to list Replication Classes",
			"labeled", labels.Set(labelSelector.MatchLabels))

		return fmt.Errorf("failed to list Replication Classes, %w", err)
	}

	return nil
}
*/

// This function is here to allow tests to override the volsyncProfile
func (v *VSHandler) SetVolSyncProfile(volSyncProfile *ramendrv1alpha1.VolSyncProfile) {
	v.volSyncProfile = volSyncProfile
}

func (v *VSHandler) getScheduleCronSpec() (*string, error) {
	if v.schedulingInterval != "" {
		return ConvertSchedulingIntervalToCronSpec(v.schedulingInterval)
	}
	/*
		// Fall-back to getting scheduling interval from VolSyncProfile
		if v.volSyncProfile != nil && v.volSyncProfile.SchedulingInterval != "" {
			return ConvertSchedulingIntervalToCronSpec(v.volSyncProfile.SchedulingInterval)
		}
	*/

	// Use default value if not specified
	return &DefaultScheduleCronSpec, nil
}

// Convert from schedulingInterval which is in the format of <num><m,h,d>
// to the format VolSync expects, which is cronspec: https://en.wikipedia.org/wiki/Cron#Overview
func ConvertSchedulingIntervalToCronSpec(schedulingInterval string) (*string, error) {
	// format needs to have at least 1 number and end with m or h or d
	if len(schedulingInterval) < 2 {
		return nil, fmt.Errorf("scheduling interval %s is invalid", schedulingInterval)
	}

	mhd := schedulingInterval[len(schedulingInterval)-1:]
	mhd = strings.ToLower(mhd) // Make sure we get lowercase m, h or d

	num := schedulingInterval[:len(schedulingInterval)-1]

	var cronSpec string

	switch mhd {
	case "m":
		cronSpec = fmt.Sprintf("*/%s * * * *", num)
	case "h":
		cronSpec = fmt.Sprintf("* */%s * * *", num)
	case "d":
		cronSpec = fmt.Sprintf("* * */%s * *", num)
	}

	if cronSpec == "" {
		return nil, fmt.Errorf("scheduling interval %s is invalid. Unable to parse m/h/d", schedulingInterval)
	}

	return &cronSpec, nil
}

func addVSRGOwnerLabel(owner, obj metav1.Object) {
	// Set vsrg label to owner name - enables lookups by owner label
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[VSRGReplicationSourceLabel] = owner.GetName()
	obj.SetLabels(labels)
}

func getReplicationDestinationName(rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec) string {
	//TODO: may need to include clustername to avoid service name collisions cross-cluster
	return rdSpec.ProtectedPVC.Name // Use PVC name as name of ReplicationDestination
}
func getReplicationSourceName(rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec) string {
	return rsSpec.PVCName // Use PVC name as name of ReplicationSource
}

// Service name that VolSync will create locally in the same namespace as the ReplicationDestination
func getLocalServiceNameForRD(rdName string) string {
	return "volsync-rsync-dst-" + rdName // This is the name VolSync will pick
}
