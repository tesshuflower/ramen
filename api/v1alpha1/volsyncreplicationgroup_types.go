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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type ReplicationDestinationSpec struct {
	PVCName string `json:"pvcName"`
	SSHKeys string `json:"sshKeys,omitempty"`
	// capacity is the size of the destination volume to create.
	Capacity *resource.Quantity `json:"capacity"`
	// storageClassName can be used to specify the StorageClass of the
	// destination volume. If not set, the default StorageClass will be used.
	//+optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

type ReplicationDestinationInfo struct {
	PVCName string `json:"pvcName"`
	Address string `json:"address"`
}

type ReplicationSourceSpec struct {
	PVCName string `json:"pvcName"`
	Address string `json:"address"`
	SSHKeys string `json:"sshKeys,omitempty"`
}

// VolSyncReplicationGroupSpec defines the desired state of VolSyncReplicationGroup
type VolSyncReplicationGroupSpec struct {
	// Label selector to identify all the PVCs that are in this group
	// that needs to be replicated to the peer cluster.
	PVCSelector metav1.LabelSelector `json:"pvcSelector"`

	// Label selector to identify the VolumeReplicationClass resources
	// that are scanned to select an appropriate VolumeReplicationClass
	// for the VolumeReplication resource.
	//+optional
	ReplicationClassSelector metav1.LabelSelector `json:"replicationClassSelector,omitempty"`

	// scheduling Interval for replicating Persistent Volume
	// data to a peer cluster. Interval is typically in the
	// form <num><m,h,d>. Here <num> is a number, 'm' means
	// minutes, 'h' means hours and 'd' stands for days.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^\d+[mhd]$`
	SchedulingInterval string `json:"schedulingInterval"`

	// Desired state of all volumes [primary or secondary] in this replication group;
	// this value is propagated to children VolumeReplication CRs
	ReplicationState ReplicationState `json:"replicationState"`

	RDSpec []ReplicationDestinationSpec `json:"rdSpec,omitempty"`
	RSSpec []ReplicationSourceSpec      `json:"rsSpec,omitempty"`
}

type VolSyncPVCInfo struct {
	// Name of the PVC resource
	Name string `json:"name"`

	// Represents the actual resources of the underlying volume.
	Capacity *resource.Quantity `json:"capacity,omitempty"`

	// Name of the StorageClass required by the claim.
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// VolSyncReplicationGroupStatus defines the observed state of VolSyncReplicationGroup
type VolSyncReplicationGroupStatus struct {
	State State `json:"state,omitempty"`

	// All the protected pvcs
	VolSyncPVCs []VolSyncPVCInfo `json:"volSyncPVCs,omitempty"`

	// Info about created RDs (should only be filled out by VSGR with ReplicateionState: secondary)
	RDInfo []ReplicationDestinationInfo `json:"rdInfo,omitempty"`

	// Conditions are the list of VRG's summary conditions and their status.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the last generation change the operator has dealt with
	// +optional
	ObservedGeneration int64       `json:"observedGeneration,omitempty"`
	LastUpdateTime     metav1.Time `json:"lastUpdateTime,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// VolSyncReplicationGroup is the Schema for the volsyncreplicationgroups API
type VolSyncReplicationGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VolSyncReplicationGroupSpec   `json:"spec,omitempty"`
	Status VolSyncReplicationGroupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// VolSyncReplicationGroupList contains a list of VolSyncReplicationGroup
type VolSyncReplicationGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VolSyncReplicationGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VolSyncReplicationGroup{}, &VolSyncReplicationGroupList{})
}
