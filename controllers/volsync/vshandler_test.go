package volsync_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/ramendr/ramen/controllers/volsync"
)

const (
	maxWait  = 20 * time.Second
	interval = 250 * time.Millisecond
)

var _ = Describe("VolSync Handler - utils", func() {
	Context("When converting scheduling interval to cronspec for VolSync", func() {
		It("Should successfully convert an interval specified in minutes", func() {
			cronSpecSchedule, err := volsync.ConvertSchedulingIntervalToCronSpec("10m")
			Expect(err).NotTo((HaveOccurred()))
			Expect(cronSpecSchedule).ToNot(BeNil())
			Expect(*cronSpecSchedule).To(Equal("*/10 * * * *"))
		})
		It("Should successfully convert an interval specified in minutes (case-insensitive)", func() {
			cronSpecSchedule, err := volsync.ConvertSchedulingIntervalToCronSpec("2M")
			Expect(err).NotTo((HaveOccurred()))
			Expect(cronSpecSchedule).ToNot(BeNil())
			Expect(*cronSpecSchedule).To(Equal("*/2 * * * *"))
		})
		It("Should successfully convert an interval specified in hours", func() {
			cronSpecSchedule, err := volsync.ConvertSchedulingIntervalToCronSpec("31h")
			Expect(err).NotTo((HaveOccurred()))
			Expect(cronSpecSchedule).ToNot(BeNil())
			Expect(*cronSpecSchedule).To(Equal("* */31 * * *"))
		})
		It("Should successfully convert an interval specified in days", func() {
			cronSpecSchedule, err := volsync.ConvertSchedulingIntervalToCronSpec("229d")
			Expect(err).NotTo((HaveOccurred()))
			Expect(cronSpecSchedule).ToNot(BeNil())
			Expect(*cronSpecSchedule).To(Equal("* * */229 * *"))
		})
		It("Should fail if interval is invalid (no num)", func() {
			_, err := volsync.ConvertSchedulingIntervalToCronSpec("d")
			Expect(err).To((HaveOccurred()))
		})
		It("Should fail if interval is invalid (no m/h/d)", func() {
			_, err := volsync.ConvertSchedulingIntervalToCronSpec("123")
			Expect(err).To((HaveOccurred()))
		})
	})
})

var _ = Describe("VolSync Handler", func() {
	var testNamespace *corev1.Namespace
	logger := zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter))

	var owner metav1.Object
	var vsHandler *volsync.VSHandler

	schedulingInterval := "5m"
	expectedCronSpecSchedule := "*/5 * * * *"

	BeforeEach(func() {
		// Create namespace for test
		testNamespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "vh-",
			},
		}
		Expect(k8sClient.Create(ctx, testNamespace)).To(Succeed())
		Expect(testNamespace.GetName()).NotTo(BeEmpty())

		// Create dummy resource to be the "owner" of the RDs and RSs
		// Using a configmap for now - in reality this owner resource will
		// be a DRPC
		ownerCm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "dummycm-owner-",
				Namespace:    testNamespace.GetName(),
			},
		}
		Expect(k8sClient.Create(ctx, ownerCm)).To(Succeed())
		Expect(ownerCm.GetName()).NotTo(BeEmpty())
		owner = ownerCm

		vsHandler = volsync.NewVSHandler(ctx, k8sClient, logger, owner, schedulingInterval)
	})

	AfterEach(func() {
		// All resources are namespaced, so this should clean it all up
		Expect(k8sClient.Delete(ctx, testNamespace)).To(Succeed())
	})

	Describe("Reconcile ReplicationDestination", func() {
		Context("When reconciling RDSpec", func() {
			capacity := resource.MustParse("2Gi")

			rdSpec := ramendrv1alpha1.ReplicationDestinationSpec{
				PVCName:  "mytestpvc",
				SSHKeys:  "testkey123",
				Capacity: &capacity,
			}

			var returnedRDInfo *ramendrv1alpha1.ReplicationDestinationInfo
			createdRD := &volsyncv1alpha1.ReplicationDestination{}

			JustBeforeEach(func() {
				// Run ReconcileRD
				var err error
				returnedRDInfo, err = vsHandler.ReconcileRD(rdSpec)
				Expect(err).ToNot(HaveOccurred())

				// RD should be created with name=PVCName
				Eventually(func() error {
					return k8sClient.Get(ctx,
						types.NamespacedName{Name: rdSpec.PVCName, Namespace: testNamespace.GetName()}, createdRD)
				}, maxWait, interval).Should(Succeed())

				// Expect the RD should be owned by owner
				Expect(ownerMatches(createdRD, owner.GetName(), "ConfigMap"))

				// Check common fields
				Expect(*createdRD.Spec.Rsync.ServiceType).To(Equal(corev1.ServiceTypeLoadBalancer))
				Expect(createdRD.Spec.Rsync.CopyMethod).To(Equal(volsyncv1alpha1.CopyMethodSnapshot))
				Expect(*createdRD.Spec.Rsync.SSHKeys).To(Equal(rdSpec.SSHKeys))
				Expect(createdRD.Spec.Rsync.Capacity).To(Equal(rdSpec.Capacity))
				Expect(createdRD.Spec.Rsync.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}))
				Expect(createdRD.Spec.Trigger).To(BeNil()) // No schedule should be set
			})

			Context("When storageClassName is not specified", func() {
				It("Should create an RD with no storage class name specified", func() {
					Expect(createdRD.Spec.Rsync.StorageClassName).To(BeNil())
					// Expect RDInfo to be nil (only should return an RDInfo if the RD.Status.Address is set)
					Expect(returnedRDInfo).To(BeNil())
				})
			})
			Context("When storageClassName is specified", func() {
				scName := "mystorageclass1"
				BeforeEach(func() {
					// Set a storageclass in the RDSpec
					rdSpec.StorageClassName = &scName
				})
				It("Should create an RD with proper storage class name", func() {
					Expect(*createdRD.Spec.Rsync.StorageClassName).To(Equal(scName))
					// Expect RDInfo to be nil (only should return an RDInfo if the RD.Status.Address is set)
					Expect(returnedRDInfo).To(BeNil())
				})

				Context("When replication destination already exists with status.address specified", func() {
					myTestAddress := "https://fakeaddress.abc.org:8888"
					BeforeEach(func() {
						// Pre-create a replication destination - and fill out Status.Address
						rdPrecreate := &volsyncv1alpha1.ReplicationDestination{
							ObjectMeta: metav1.ObjectMeta{
								Name:      rdSpec.PVCName,
								Namespace: testNamespace.GetName(),
							},
							// Empty spec - will expect the reconcile to fill this out properly for us (i.e. update)
							Spec: volsyncv1alpha1.ReplicationDestinationSpec{},
						}
						Expect(k8sClient.Create(ctx, rdPrecreate)).To(Succeed())

						//
						// Make sure the RD is created and update Status to set an address
						// (Simulating what the volsync controller would do)
						//
						Eventually(func() error {
							return k8sClient.Get(ctx, client.ObjectKeyFromObject(rdPrecreate), rdPrecreate)
						}, maxWait, interval).Should(Succeed())
						// Fake the address in the status
						rdPrecreate.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
							Rsync: &volsyncv1alpha1.ReplicationDestinationRsyncStatus{
								Address: &myTestAddress,
								SSHKeys: &rdSpec.SSHKeys,
							},
						}
						Expect(k8sClient.Status().Update(ctx, rdPrecreate)).To(Succeed())
						Eventually(func() *string {
							err := k8sClient.Get(ctx, client.ObjectKeyFromObject(rdPrecreate), rdPrecreate)
							if err != nil || rdPrecreate.Status == nil || rdPrecreate.Status.Rsync == nil {
								return nil
							}
							return rdPrecreate.Status.Rsync.Address
						}, maxWait, interval).Should(Not(BeNil()))
					})

					It("Should properly update Replication destination and return rdInfo", func() {
						// Common JustBeforeEach will run reconcileRD and check spec is proper

						Expect(*createdRD.Spec.Rsync.StorageClassName).To(Equal(scName)) // Check storage class
						// Expect RDInfo to NOT be nil - address was filled out so it should have been returned
						Expect(returnedRDInfo).ToNot(BeNil())
					})
				})
			})
		})
	})

	Describe("Reconcile ReplicationSource", func() {
		Context("When reconciling RSSpec", func() {
			rsSpec := ramendrv1alpha1.ReplicationSourceSpec{
				PVCName: "mytestpvc",
				Address: "https://testing.abc.org",
				SSHKeys: "testkey123",
			}

			createdRS := &volsyncv1alpha1.ReplicationSource{}

			JustBeforeEach(func() {
				// Run ReconcileRS
				err := vsHandler.ReconcileRS(rsSpec)
				Expect(err).ToNot(HaveOccurred())

				// RS should be created with name=PVCName
				Eventually(func() error {
					return k8sClient.Get(ctx,
						types.NamespacedName{Name: rsSpec.PVCName, Namespace: testNamespace.GetName()}, createdRS)
				}, maxWait, interval).Should(Succeed())

				// Expect the RS should be owned by owner
				Expect(ownerMatches(createdRS, owner.GetName(), "ConfigMap"))

				// Check common fields
				Expect(createdRS.Spec.SourcePVC).To(Equal(rsSpec.PVCName))
				Expect(createdRS.Spec.Rsync.CopyMethod).To(Equal(volsyncv1alpha1.CopyMethodSnapshot))
				Expect(*createdRS.Spec.Rsync.SSHKeys).To(Equal(rsSpec.SSHKeys))
				Expect(*createdRS.Spec.Rsync.Address).To(Equal(rsSpec.Address))

				Expect(createdRS.Spec.Trigger).ToNot(BeNil())
				Expect(createdRS.Spec.Trigger).To(Equal(&volsyncv1alpha1.ReplicationSourceTriggerSpec{
					Schedule: &expectedCronSpecSchedule,
				}))
			})

			It("Should create an ReplicationSource if one does not exist", func() {
				// All checks here performed in the JustBeforeEach(common checks)
			})

			Context("When replication source already exists", func() {
				BeforeEach(func() {
					// Pre-create a replication destination - and fill out Status.Address
					rsPrecreate := &volsyncv1alpha1.ReplicationSource{
						ObjectMeta: metav1.ObjectMeta{
							Name:      rsSpec.PVCName,
							Namespace: testNamespace.GetName(),
						},
						// Will expect the reconcile to fill this out properly for us (i.e. update)
						Spec: volsyncv1alpha1.ReplicationSourceSpec{
							Rsync: &volsyncv1alpha1.ReplicationSourceRsyncSpec{},
						},
					}
					Expect(k8sClient.Create(ctx, rsPrecreate)).To(Succeed())

					//
					// Make sure the RS is created
					//
					Eventually(func() error {
						return k8sClient.Get(ctx, client.ObjectKeyFromObject(rsPrecreate), rsPrecreate)
					}, maxWait, interval).Should(Succeed())
				})

				It("Should properly update ReplicationSource and return rsInfo", func() {
					// All checks here performed in the JustBeforeEach(common checks)
				})
			})
		})
	})

	Describe("Ensure PVC from ReplicationDestination", func() {
		pvcName := "testpvc1"
		pvcCapacity := resource.MustParse("1Gi")
		pvcStorageClassName := "teststorageclass"

		rdSpec := ramendrv1alpha1.ReplicationDestinationSpec{
			PVCName:          pvcName,
			SSHKeys:          "testsecret",
			Capacity:         &pvcCapacity,
			StorageClassName: &pvcStorageClassName,
		}

		var ensurePVCErr error
		JustBeforeEach(func() {
			ensurePVCErr = vsHandler.EnsurePVCfromRD(rdSpec)
		})

		Context("When ReplicationDestination Does not exist", func() {
			It("Should not throw an error", func() { // Ignoring if RD is not there right now
				Expect(ensurePVCErr).NotTo(HaveOccurred())
			})
		})

		Context("When ReplicationDestination exists with no latestImage", func() {
			BeforeEach(func() {
				// Pre-create the replication destination
				rd := &volsyncv1alpha1.ReplicationDestination{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pvcName,
						Namespace: testNamespace.GetName(),
					},
					Spec: volsyncv1alpha1.ReplicationDestinationSpec{
						Rsync: &volsyncv1alpha1.ReplicationDestinationRsyncSpec{},
					},
				}
				Expect(k8sClient.Create(ctx, rd)).To(Succeed())
			})
			It("Should fail to ensure PVC", func() {
				Expect(ensurePVCErr).To(HaveOccurred())
				Expect(ensurePVCErr.Error()).To(ContainSubstring("unable to find LatestImage"))
			})
		})

		Context("When ReplicationDestination exists with snapshot latestImage", func() {
			latestImageSnapshotName := "testingsnap001"

			BeforeEach(func() {
				// Pre-create the replication destination
				rd := &volsyncv1alpha1.ReplicationDestination{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pvcName,
						Namespace: testNamespace.GetName(),
					},
					Spec: volsyncv1alpha1.ReplicationDestinationSpec{
						Rsync: &volsyncv1alpha1.ReplicationDestinationRsyncSpec{},
					},
				}
				Expect(k8sClient.Create(ctx, rd)).To(Succeed())

				apiGrp := volsync.VolumeSnapshotGroup
				// Now force update the status to report a volume snapshot as latestImage
				rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
					LatestImage: &corev1.TypedLocalObjectReference{
						Kind:     volsync.VolumeSnapshotKind,
						APIGroup: &apiGrp,
						Name:     latestImageSnapshotName,
					},
				}
				Expect(k8sClient.Status().Update(ctx, rd)).To(Succeed())
			})

			Context("When the latest image volume snapshot does not exist", func() {
				It("Should fail to ensure PVC", func() {
					Expect(ensurePVCErr).To(HaveOccurred())
					Expect(ensurePVCErr.Error()).To(ContainSubstring("volumesnapshots"))
					Expect(ensurePVCErr.Error()).To(ContainSubstring("not found"))
					Expect(ensurePVCErr.Error()).To(ContainSubstring(latestImageSnapshotName))
				})
			})

			Context("When the latest image volume snapshot exists", func() {
				var latestImageSnap *unstructured.Unstructured
				BeforeEach(func() {
					// Create a fake volume snapshot
					var err error
					latestImageSnap, err = createSnapshot(latestImageSnapshotName, testNamespace.GetName())
					Expect(err).NotTo(HaveOccurred())
				})

				JustBeforeEach(func() {
					// Common checks for everything in this context - pvc should be created with correct spec
					Expect(ensurePVCErr).NotTo(HaveOccurred())

					pvc := &corev1.PersistentVolumeClaim{}
					Eventually(func() error {
						return k8sClient.Get(ctx, types.NamespacedName{
							Name:      pvcName,
							Namespace: testNamespace.GetName(),
						}, pvc)
					}, maxWait, interval).Should(Succeed())

					Expect(pvc.GetName()).To(Equal(pvcName))
					Expect(pvc.Spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}))
					Expect(*pvc.Spec.StorageClassName).To(Equal(pvcStorageClassName))
					apiGrp := volsync.VolumeSnapshotGroup
					Expect(pvc.Spec.DataSource).To(Equal(&corev1.TypedLocalObjectReference{
						Name:     latestImageSnapshotName,
						APIGroup: &apiGrp,
						Kind:     volsync.VolumeSnapshotKind,
					}))
					Expect(pvc.Spec.Resources.Requests).To(Equal(corev1.ResourceList{
						corev1.ResourceStorage: pvcCapacity,
					}))
				})

				It("PVC should be created, latestImage VolumeSnapshot should have a finalizer added", func() {
					Eventually(func() bool {
						err := k8sClient.Get(ctx, types.NamespacedName{
							Name:      latestImageSnapshotName,
							Namespace: testNamespace.GetName(),
						}, latestImageSnap)
						if err != nil {
							return false
						}
						return len(latestImageSnap.GetFinalizers()) == 1 &&
							latestImageSnap.GetFinalizers()[0] == volsync.VolumeSnapshotProtectFinalizerName
					}, maxWait, interval).Should(BeTrue())
				})

				Context("When pvc has already been created", func() {
					It("ensure PVC should not fail", func() {
						// Previous ensurePVC will already have created the PVC (see parent context)
						// Now run ensurePVC again - additional runs should just ensure the PVC is ok
						Expect(vsHandler.EnsurePVCfromRD(rdSpec)).To(Succeed())
					})
				})
			})
		})
	})
})

func ownerMatches(obj metav1.Object, ownerName, ownerKind string) bool {
	for _, ownerRef := range obj.GetOwnerReferences() {
		if ownerRef.Name == ownerName && ownerRef.Kind == ownerKind {
			return true
		}
	}

	return false
}

func createSnapshot(snapshotName, namespace string) (*unstructured.Unstructured, error) {
	volSnap := &unstructured.Unstructured{}
	volSnap.Object = map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      snapshotName,
			"namespace": namespace,
		},
		"spec": map[string]interface{}{
			"source": map[string]interface{}{
				"persistentVolumeClaimName": "fakepvcnamehere",
			},
		},
	}
	volSnap.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   volsync.VolumeSnapshotGroup,
		Kind:    volsync.VolumeSnapshotKind,
		Version: volsync.VolumeSnapshotVersion,
	})

	return volSnap, k8sClient.Create(ctx, volSnap)
}
