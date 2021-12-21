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
	ctx    context.Context
	client client.Client
	log    logr.Logger
	owner  metav1.Object
}

func NewVolSyncReconciler(ctx context.Context, client client.Client, log logr.Logger, owner metav1.Object) *VolSyncReconciler {
	return &VolSyncReconciler{
		ctx:    ctx,
		client: client,
		log:    log,
		owner:  owner,
	}
}

func (r *VolSyncReconciler) ReconcileRD(rdSpec ramendrv1alpha1.ReplicationDestinationSpec) (*ramendrv1alpha1.ReplicationDestinationInfo, error) {
	l := r.log.WithValues("rdInfoSpec", rdSpec)

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

		//NOT SETTING TRIGGER - since we're using rsync here
		//rd.Spec.Trigger = &volsyncv1alpha1.ReplicationDestinationTriggerSpec{}

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
	if rd.Status.Rsync.Address != nil {
		l.V(1).Info("ReplicationDestination Reconcile Complete")
		return &ramendrv1alpha1.ReplicationDestinationInfo{
			PVCName: rdSpec.PVCName,
			SSHKeys: *rd.Status.Rsync.SSHKeys,
			Address: *rd.Status.Rsync.Address,
		}, nil
	}

	l.V(1).Info("ReplicationDestination waiting for Address ...")
	return nil, nil
}
