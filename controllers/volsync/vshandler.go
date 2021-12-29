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
	VolumeSnapshotKind                 string = "VolumeSnapshot"
	VolumeSnapshotGroup                string = "snapshot.storage.k8s.io"
	VolumeSnapshotVersion              string = "v1"
	VolumeSnapshotProtectFinalizerName string = "volsyncreplicationgroups.ramendr.openshift.io/volumesnapshot-protection"
)

type VSHandler struct {
	ctx                context.Context
	client             client.Client
	log                logr.Logger
	owner              metav1.Object
	schedulingInterval string
}

func NewVSHandler(ctx context.Context, client client.Client, log logr.Logger, owner metav1.Object,
	schedulingInterval string) *VSHandler {
	return &VSHandler{
		ctx:                ctx,
		client:             client,
		log:                log,
		owner:              owner,
		schedulingInterval: schedulingInterval,
	}
}

func (r *VSHandler) ReconcileRD(
	rdSpec ramendrv1alpha1.ReplicationDestinationSpec) (*ramendrv1alpha1.ReplicationDestinationInfo, error) {

	l := r.log.WithValues("rdSpec", rdSpec)

	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rdSpec.PVCName, // Use PVC name as name of ReplicationDestination
			Namespace: r.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(r.ctx, r.client, rd, func() error {
		if err := ctrl.SetControllerReference(r.owner, rd, r.client.Scheme()); err != nil {
			l.Error(err, "unable to set controller reference")
			return err
		}

		// Pre-alloacated shared secret
		var sshKeys *string
		if rdSpec.SSHKeys != "" {
			// If SSHKeys is not specified, RD will create its own secret
			sshKeys = &rdSpec.SSHKeys
		}

		rd.Spec.Rsync = &volsyncv1alpha1.ReplicationDestinationRsyncSpec{
			ServiceType: &rsyncServiceType,
			SSHKeys:     sshKeys,

			ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
				CopyMethod:       volsyncv1alpha1.CopyMethodSnapshot,
				Capacity:         rdSpec.Capacity,
				StorageClassName: rdSpec.StorageClassName,
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	l.V(1).Info("ReplicationDestination createOrUpdate Complete", "op", op)

	//
	// Now check status - only return an RDInfo if we have an address filled out in the ReplicationDestination Status
	//
	if rd.Status == nil || rd.Status.Rsync == nil || rd.Status.Rsync.Address == nil {
		l.V(1).Info("ReplicationDestination waiting for Address ...")
		return nil, nil
	}

	l.V(1).Info("ReplicationDestination Reconcile Complete")
	return &ramendrv1alpha1.ReplicationDestinationInfo{
		PVCName: rdSpec.PVCName,
		Address: *rd.Status.Rsync.Address,
	}, nil
}

func (r *VSHandler) ReconcileRS(rsSpec ramendrv1alpha1.ReplicationSourceSpec) error {
	l := r.log.WithValues("rsSpec", rsSpec)

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsSpec.PVCName, // Use PVC name as name of ReplicationSource
			Namespace: r.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(r.ctx, r.client, rs, func() error {
		if err := ctrl.SetControllerReference(r.owner, rs, r.client.Scheme()); err != nil {
			l.Error(err, "unable to set controller reference")
			return err
		}

		rs.Spec.SourcePVC = rsSpec.PVCName

		cronSpecSchedule, err := ConvertSchedulingIntervalToCronSpec(r.schedulingInterval)
		if err != nil {
			l.Error(err, "unable to parse schedulingInterval")
			return err
		}
		rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{
			Schedule: cronSpecSchedule,
		}

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
		return err
	}

	l.V(1).Info("ReplicationSource Reconcile Complete")
	return nil
}

func (r *VSHandler) EnsurePVCfromRD(rdSpec ramendrv1alpha1.ReplicationDestinationSpec) error {
	l := r.log.WithValues("rdSpec", rdSpec)

	// Get RD instance
	rdInst := &volsyncv1alpha1.ReplicationDestination{}
	err := r.client.Get(r.ctx, types.NamespacedName{Name: rdSpec.PVCName, Namespace: r.owner.GetNamespace()}, rdInst)
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
		noSnapErr := fmt.Errorf("unable to find LatestImage from ReplicationDestination %s", rdSpec.PVCName)
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

	if err := r.validateSnapshotAndAddFinalizer(*vsImageRef); err != nil {
		return err
	}

	if err := r.ensurePVCFromSnapshot(rdSpec, *vsImageRef); err != nil {
		return err
	}

	return nil
}

func (r *VSHandler) ensurePVCFromSnapshot(rdSpec ramendrv1alpha1.ReplicationDestinationSpec,
	snapshotRef corev1.TypedLocalObjectReference) error {
	l := r.log.WithValues("pvcName", rdSpec.PVCName, "snapshotRef", snapshotRef)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rdSpec.PVCName,
			Namespace: r.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(r.ctx, r.client, pvc, func() error {
		if err := ctrl.SetControllerReference(r.owner, pvc, r.client.Scheme()); err != nil {
			r.log.Error(err, "unable to set controller reference")
			return err
		}

		if pvc.Status.Phase == corev1.ClaimBound {
			// Assume no changes are required
			l.V(1).Info("PVC already bound")
			return nil
		}

		//TODO: needs finalizer?  r.addFinalizer(pvc, pvcFinalizerName)

		if pvc.CreationTimestamp.IsZero() { // set immutable fields
			pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce} //TODO: should this come from original PVC i.e. in RDspec?
			pvc.Spec.StorageClassName = rdSpec.StorageClassName

			// Don't change datasource - only set when initially creating
			pvc.Spec.DataSource = &snapshotRef
		}

		pvc.Spec.Resources.Requests = corev1.ResourceList{
			corev1.ResourceStorage: *rdSpec.Capacity,
		}

		return nil
	})

	if err != nil {
		l.Error(err, "Unable to createOrUpdate PVC from snapshot")
		return err
	}

	l.V(1).Info("PVC createOrUpdate Complete", "op", op)
	return nil
}

func (r *VSHandler) validateSnapshotAndAddFinalizer(volumeSnapshotRef corev1.TypedLocalObjectReference) error {
	// Using unstructured to avoid needing to require VolumeSnapshot in client scheme
	volSnap := &unstructured.Unstructured{}
	volSnap.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   *volumeSnapshotRef.APIGroup,
		Kind:    volumeSnapshotRef.Kind,
		Version: VolumeSnapshotVersion,
	})
	err := r.client.Get(r.ctx, types.NamespacedName{
		Name:      volumeSnapshotRef.Name,
		Namespace: r.owner.GetNamespace(),
	}, volSnap)

	if err != nil {
		r.log.Error(err, "Unable to get VolumeSnapshot", "volumeSnapshotRef", volumeSnapshotRef)
		return err
	}

	if err := r.addFinalizerAndUpdate(volSnap, VolumeSnapshotProtectFinalizerName); err != nil {
		r.log.Error(err, "Unable to add finalizer to VolumeSnapshot", "volumeSnapshotRef", volumeSnapshotRef)
		return err
	}

	r.log.V(1).Info("VolumeSnapshot validated and protected with finalizer", "volumeSnapshotRef", volumeSnapshotRef)
	return nil
}

func (r *VSHandler) addFinalizer(obj client.Object, finalizer string) (updated bool) {
	updated = false
	if !ctrlutil.ContainsFinalizer(obj, finalizer) {
		ctrlutil.AddFinalizer(obj, finalizer)
		updated = true
	}
	return updated
}

func (r *VSHandler) addFinalizerAndUpdate(obj client.Object, finalizer string) error {
	if r.addFinalizer(obj, finalizer) {
		if err := r.client.Update(r.ctx, obj); err != nil {
			r.log.Error(err, "Failed to add finalizer", "finalizer", finalizer)
			return fmt.Errorf("%w", err)
		}
	}
	return nil
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
