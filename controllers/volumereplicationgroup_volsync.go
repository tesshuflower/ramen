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
	"fmt"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
)

func (v *VRGInstance) restorePVsForVolSync() error {
	v.log.Info("VolSync: Restoring VolSync PVs")
	// TODO: refactor this per this comment: https://github.com/RamenDR/ramen/pull/197#discussion_r687246692
	pvsRestored := findCondition(v.instance.Status.Conditions, VRGConditionTypeVolSyncPVsRestored)
	if pvsRestored != nil && pvsRestored.Status == metav1.ConditionTrue &&
		pvsRestored.ObservedGeneration == v.instance.Generation {
		v.log.Info("VolSyncPVsRestored condition found. PVs already restored.")

		return nil
	}

	if len(v.instance.Spec.VolSync.RDSpec) == 0 {
		v.log.Info("No RDSpec entries. There are no PVCs to restore")
		// No ReplicationDestinations (i.e. no PVCs) to restore
		return nil
	}

	success := true
	for _, rdSpec := range v.instance.Spec.VolSync.RDSpec {

		err := v.volSyncHandler.EnsurePVCfromRD(rdSpec)
		if err != nil {
			v.log.Error(err, "Unable to ensure PVC", "rdSpec", rdSpec)
			success = false

			continue // Keep trying to ensure PVCs for other rdSpec
		}

		if protectedPVC := v.findProtectedPVC(rdSpec.ProtectedPVC.Name); protectedPVC != nil {
			setVRGConditionTypeVolSyncPVRestoreComplete(&protectedPVC.Conditions, v.instance.Generation, "PVC restored")

			continue
		}

		protectedPVC := rdSpec.ProtectedPVC
		setVRGConditionTypeVolSyncPVRestoreComplete(&protectedPVC.Conditions, v.instance.Generation, "PVC restored")
		v.instance.Status.ProtectedPVCs = append(v.instance.Status.ProtectedPVCs, protectedPVC)
	}

	if !success {
		return fmt.Errorf("failed to restore all PVCs using RDSpec (%v)", v.instance.Spec.VolSync.RDSpec)
	}

	msg := "VolSync: PVCs restored"
	setVRGConditionTypeVolSyncRepSourceSetupInitializing(&v.instance.Status.Conditions, v.instance.Generation, msg)
	v.log.Info(msg)

	return nil
}

func (v *VRGInstance) reconcileVolSyncAsPrimary() (requeue bool) {
	v.log.Info("Reconciling VolSync as Primary", "volSync", v.instance.Spec.VolSync)

	requeue = false

	// Will loop over all pvcs - return immediately on any error, but will return with requeue=true
	// after the loop if all are successful but replication sources are not ready yet

	// First time: Add all VolSync PVCs to the protected PVC list and set their ready condition to initializing
	for _, pvc := range v.volSyncPVCs {
		newProtectedPVC := &ramendrv1alpha1.ProtectedPVC{
			Name:               pvc.Name,
			ProtectedByVolSync: true,
			StorageClassName:   pvc.Spec.StorageClassName,
			AccessModes:        pvc.Spec.AccessModes,
			Resources:          pvc.Spec.Resources,
		}

		protectedPVC := v.findProtectedPVC(pvc.Name)
		if protectedPVC == nil {
			protectedPVC = newProtectedPVC
			v.instance.Status.ProtectedPVCs = append(v.instance.Status.ProtectedPVCs, *protectedPVC)
		} else if !reflect.DeepEqual(protectedPVC, newProtectedPVC) {
			newProtectedPVC.DeepCopyInto(protectedPVC)
		}

		// Not much need for VolSyncReplicationSourceSpec anymore - but keeping it around in case we want
		// to add anything to it later to control anything in the ReplicationSource
		rsSpec := ramendrv1alpha1.VolSyncReplicationSourceSpec{
			ProtectedPVC: *protectedPVC,
		}

		_, rs, err := v.volSyncHandler.ReconcileRS(rsSpec, false /* Schedule sync normally */)
		if err != nil {
			v.log.Error(err, "Failed to reconcile VolSync Replication Source")

			setVRGConditionTypeVolSyncRepSourceSetupError(&protectedPVC.Conditions, v.instance.Generation,
				"VolSync setup failed")

			requeue = true
			return
		}

		if rs == nil {
			// Replication destination is not ready yet, indicate we should requeue after for loop is complete
			requeue = true
		} else {
			setVRGConditionTypeVolSyncRepSourceSetupComplete(&protectedPVC.Conditions, v.instance.Generation, "VolSync PVC Ready")
		}
	}

	if requeue {
		v.log.Info("ReconcileRS - ReplicationSources are not all ready. We'll retry...")
		return
	}

	// Cleanup - this VRG is primary, cleanup if necessary
	// remove any ReplicationDestinations (that would have been created when this VRG was secondary) if they
	// are not in the RDSpec list
	if err := v.volSyncHandler.CleanupRDNotInSpecList(v.instance.Spec.VolSync.RDSpec); err != nil {
		v.log.Error(err, "Failed to cleanup the RDSpecs when this VRG instance was secondary")

		requeue = true
		return
	}

	v.log.Info("Successfully reconciled VolSync as Primary")

	msg := "VolSync Source is ready for data replication"
	setVRGConditionTypeVolSyncRepSourceSetupComplete(&v.instance.Status.Conditions, v.instance.Generation, msg)

	return
}

func (v *VRGInstance) reconcileVolSyncAsSecondary() (requeue bool) {
	v.log.Info("Reconcile VolSync as Secondary", "RDSpec", v.instance.Spec.VolSync.RDSpec)

	requeue = false

	// If we are secondary, and RDSpec is not set, then we don't want to have any PVC
	// flagged as a VolSync PVC.
	if v.instance.Spec.VolSync.RDSpec == nil {
		for idx := range v.instance.Status.ProtectedPVCs {
			v.instance.Status.ProtectedPVCs[idx].ProtectedByVolSync = false
		}
	}

	// Reconcile RDSpec (deletion or replication)
	for _, rdSpec := range v.instance.Spec.VolSync.RDSpec {
		v.log.Info("Reconcile RD as Secondary", "RDSpec", rdSpec)
		rd, err := v.volSyncHandler.ReconcileRD(rdSpec)
		if err != nil {
			v.log.Error(err, "Failed to reconcile VolSync Replication Destination")

			requeue = true
			return
		}

		if rd == nil {
			// Replication destination is not ready yet, indicate we should requeue after the for loop is complete
			requeue = true
		}

		// Cleanup - this VRG is secondary, cleanup if necessary
		// remove ReplicationSources that would have been created when this VRG was primary
		if err := v.volSyncHandler.DeleteRS(rdSpec.ProtectedPVC.Name); err != nil {
			v.log.Error(err, "Failed to delete RS from when this VRG instance was primary")

			requeue = true
			return
		}
	}

	if requeue {
		v.log.Info("ReconcileRD - ReplicationDestinations are not all ready. We'll retry...")
		return
	}

	//TODO: cleanup any RD that is not in rdSpec? may not be necessary?

	// This may be a relocate scenario - in which case we want to run a final sync
	// of the PVCs we've been syncing (via ReplicationDestinations) when we were primary
	// Trigger final sync on any ReplicationDestination in the RSSpec list
	// for _, rsSpec := range v.instance.Spec.VolSync.RSSpec {
	// 	finalSyncComplete, err := v.volSyncHandler.ReconcileRS(rsSpec, true /* Run final sync */)
	// 	if err != nil {
	// 		v.log.Error(err, "Failed to reconcile VolSync Replication Destination")

	// 		return false
	// 	}
	// 	if finalSyncComplete {
	// 		//TODO: will need to indicate status back to DRPC controller
	// 	}
	// }

	v.log.Info("Successfully reconciled VolSync as Secondary")

	msg := "VolSync Destination is ready for data replication"
	setVRGConditionTypeVolSyncRepDestSetupComplete(&v.instance.Status.Conditions, v.instance.Generation, msg)

	return
}
