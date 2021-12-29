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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type VolSyncReconciler struct {
	ctx                context.Context
	client             client.Client
	log                logr.Logger
	owner              metav1.Object
	schedulingInterval string
}

func NewVolSyncReconciler(ctx context.Context, client client.Client, log logr.Logger, owner metav1.Object,
	schedulingInterval string) *VolSyncReconciler {
	return &VolSyncReconciler{
		ctx:                ctx,
		client:             client,
		log:                log,
		owner:              owner,
		schedulingInterval: schedulingInterval,
	}
}

func (r *VolSyncReconciler) ReconcileRD(
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

func (r *VolSyncReconciler) ReconcileRS(rsSpec ramendrv1alpha1.ReplicationSourceSpec) error {
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
