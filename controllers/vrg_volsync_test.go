package controllers_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	"github.com/ramendr/ramen/controllers/volsync"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
)

const (
	testMaxWait             = 20 * time.Second
	testInterval            = 250 * time.Millisecond
	testStorageClassName    = "fakestorageclass"
	testVolumeSnapshotClass = "fakevolumesnapshotclass"
)

var _ = Describe("VolumeReplicationGroupController", func() {
	var testNamespace *corev1.Namespace
	testLogger := zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter))
	var testCtx context.Context
	var cancel context.CancelFunc

	BeforeEach(func() {
		testCtx, cancel = context.WithCancel(context.TODO())

		// Create namespace for test
		testNamespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "vh-",
			},
		}
		Expect(k8sClient.Create(testCtx, testNamespace)).To(Succeed())
		Expect(testNamespace.GetName()).NotTo(BeEmpty())
	})

	AfterEach(func() {
		// All resources are namespaced, so this should clean it all up
		Expect(k8sClient.Delete(testCtx, testNamespace)).To(Succeed())

		cancel()
	})

	Describe("Primary initial setup", func() {
		testMatchLabels := map[string]string{
			"ramentest": "backmeup",
		}

		var testVsrg *ramendrv1alpha1.VolumeReplicationGroup

		Context("When VRG created on primary", func() {
			JustBeforeEach(func() {
				testVsrg = &ramendrv1alpha1.VolumeReplicationGroup{
					ObjectMeta: metav1.ObjectMeta{
						GenerateName: "test-vrg-east-",
						Namespace:    testNamespace.GetName(),
					},
					Spec: ramendrv1alpha1.VolumeReplicationGroupSpec{
						ReplicationState: ramendrv1alpha1.Primary,
						Async: ramendrv1alpha1.VRGAsyncSpec{
							Mode:               ramendrv1alpha1.AsyncModeEnabled,
							SchedulingInterval: "1h",
						},
						Sync: ramendrv1alpha1.VRGSyncSpec{
							Mode: ramendrv1alpha1.SyncModeDisabled,
						},
						PVCSelector: metav1.LabelSelector{
							MatchLabels: testMatchLabels,
						},
						S3Profiles: []string{s3Profiles[0].S3ProfileName},
						VolSync:    ramendrv1alpha1.VolSyncSpec{},
					},
				}

				Expect(k8sClient.Create(testCtx, testVsrg)).To(Succeed())

				Eventually(func() []string {
					err := k8sClient.Get(testCtx, client.ObjectKeyFromObject(testVsrg), testVsrg)
					if err != nil {
						return []string{}
					}

					return testVsrg.GetFinalizers()
				}, testMaxWait, testInterval).Should(
					ContainElement("volumereplicationgroups.ramendr.openshift.io/vrg-protection"))

				createSecret(testVsrg.GetName(), testNamespace.Name)
				createSC()
				createVSC()
			})

			Context("When no matching PVCs are bound", func() {
				It("Should not update status with protected PVCs", func() {
					Expect(len(testVsrg.Status.ProtectedPVCs)).To(Equal(0))
				})
			})

			Context("When matching PVCs are bound", func() {
				var boundPvcs []corev1.PersistentVolumeClaim
				JustBeforeEach(func() {
					boundPvcs = []corev1.PersistentVolumeClaim{} // Reset for each test

					// Create some PVCs that are bound
					for i := 0; i < 3; i++ {
						newPvc := createPVCBoundToRunningPod(testCtx, testNamespace.GetName(), testMatchLabels)
						boundPvcs = append(boundPvcs, *newPvc)
					}

					Eventually(func() int {
						err := k8sClient.Get(testCtx, client.ObjectKeyFromObject(testVsrg), testVsrg)
						if err != nil {
							return 0
						}

						return len(testVsrg.Status.ProtectedPVCs)
					}, testMaxWait, testInterval).Should(Equal(len(boundPvcs)))
				})

				It("Should find the bound PVCs and report in Status", func() {
					// Check the volsync pvcs
					foundBoundPVC0 := false
					foundBoundPVC1 := false
					foundBoundPVC2 := false
					for _, vsPvc := range testVsrg.Status.ProtectedPVCs {
						switch vsPvc.Name {
						case boundPvcs[0].GetName():
							foundBoundPVC0 = true
						case boundPvcs[1].GetName():
							foundBoundPVC1 = true
						case boundPvcs[2].GetName():
							foundBoundPVC2 = true
						}
					}
					Expect(foundBoundPVC0).To(BeTrue())
					Expect(foundBoundPVC1).To(BeTrue())
					Expect(foundBoundPVC2).To(BeTrue())
				})

				It("Should create ReplicationSources for each", func() {
					allRSs := &volsyncv1alpha1.ReplicationSourceList{}
					Eventually(func() int {
						Expect(k8sClient.List(testCtx, allRSs,
							client.InNamespace(testNamespace.GetName()))).To(Succeed())

						return len(allRSs.Items)
					}, testMaxWait, testInterval).Should(Equal(len(testVsrg.Status.ProtectedPVCs)))

					rs0 := &volsyncv1alpha1.ReplicationSource{}
					Expect(k8sClient.Get(testCtx, types.NamespacedName{
						Name: boundPvcs[0].GetName(), Namespace: testNamespace.GetName(),
					}, rs0)).To(Succeed())
					Expect(rs0.Spec.SourcePVC).To(Equal(boundPvcs[0].GetName()))
					Expect(rs0.Spec.Trigger).NotTo(BeNil())
					Expect(*rs0.Spec.Trigger.Schedule).To(Equal("0 */1 * * *")) // scheduling interval was set to 1h

					rs1 := &volsyncv1alpha1.ReplicationSource{}
					Expect(k8sClient.Get(testCtx, types.NamespacedName{
						Name: boundPvcs[1].GetName(), Namespace: testNamespace.GetName(),
					}, rs1)).To(Succeed())
					Expect(rs1.Spec.SourcePVC).To(Equal(boundPvcs[1].GetName()))
					Expect(rs1.Spec.Trigger).NotTo(BeNil())
					Expect(*rs1.Spec.Trigger.Schedule).To(Equal("0 */1 * * *")) // scheduling interval was set to 1h

					rs2 := &volsyncv1alpha1.ReplicationSource{}
					Expect(k8sClient.Get(testCtx, types.NamespacedName{
						Name: boundPvcs[2].GetName(), Namespace: testNamespace.GetName(),
					}, rs2)).To(Succeed())
					Expect(rs2.Spec.SourcePVC).To(Equal(boundPvcs[2].GetName()))
					Expect(rs2.Spec.Trigger).NotTo(BeNil())
					Expect(*rs2.Spec.Trigger.Schedule).To(Equal("0 */1 * * *")) // scheduling interval was set to 1h
				})

				Context("When a PVC is removed or label no longer matches the label selector", func() {
					JustBeforeEach(func() {
						// Delete pvc[1]
						pvcToDel := &boundPvcs[1]
						Eventually(func() bool {
							err := k8sClient.Delete(testCtx, pvcToDel)
							if err != nil {
								// Success if pvc is gone
								return errors.IsNotFound(err)
							}

							err = k8sClient.Get(testCtx, client.ObjectKeyFromObject(pvcToDel), pvcToDel)
							if err != nil {
								// Success if pvc is gone
								return errors.IsNotFound(err)
							}

							testLogger.Info("### PVC to del", "pvcToDel", &pvcToDel)

							// PVCs have a finalizer that's never cleared as there's no controller for this
							// manually remove the finalizer so the delete can proceed
							pvcToDel.Finalizers = []string{}
							k8sClient.Update(testCtx, pvcToDel)

							return false
						}, testMaxWait, interval).Should(BeTrue())

						// Update label so it won't be found by the label selector for pvc[2]
						pvcRemLabel := &boundPvcs[2]
						updatedLabels := map[string]string{}
						for labelKey, labelVal := range pvcRemLabel.GetLabels() {
							if _, ok := testMatchLabels[labelKey]; !ok {
								updatedLabels[labelKey] = labelVal
							}
						}

						Eventually(func() bool {
							err := k8sClient.Get(testCtx, client.ObjectKeyFromObject(pvcRemLabel), pvcRemLabel)
							if err != nil {
								return false
							}

							// Update labels
							pvcRemLabel.Labels = updatedLabels
							err = k8sClient.Update(testCtx, pvcRemLabel)
							return err == nil
						}, testMaxWait, interval).Should(BeTrue())
					})

					It("Should remove RSs for the pvcs that were removed or no longer match the label selector", func() {
						allRSs := &volsyncv1alpha1.ReplicationSourceList{}
						Eventually(func() int {
							// In vrg reconcile, pvcList should now no longer load pvc1 (because of deletion)
							// and pvc2 (because labels no longer match)
							// Now the ReplicationDestinations for these should be removed as well
							Expect(k8sClient.List(testCtx, allRSs,
								client.InNamespace(testNamespace.GetName()))).To(Succeed())

							return len(allRSs.Items)
						}, testMaxWait, testInterval).Should(Equal(1))

						remainingRS := allRSs.Items[0]

						Expect(remainingRS.Spec.SourcePVC).To(Equal(boundPvcs[0].GetName()))

						//TODO: Need to check status and ensure they are removed from protectedPVCs
					})
				})
			})
		})
	})

	Describe("Secondary initial setup", func() {
		testMatchLabels := map[string]string{
			"ramentest": "backmeup",
		}

		var testVrg *ramendrv1alpha1.VolumeReplicationGroup

		testAccessModes := []corev1.PersistentVolumeAccessMode{
			corev1.ReadWriteOnce,
		}

		Context("When VRG created on secondary", func() {
			JustBeforeEach(func() {
				testVrg = &ramendrv1alpha1.VolumeReplicationGroup{
					ObjectMeta: metav1.ObjectMeta{
						GenerateName: "test-vrg-east-",
						Namespace:    testNamespace.GetName(),
					},
					Spec: ramendrv1alpha1.VolumeReplicationGroupSpec{
						ReplicationState: ramendrv1alpha1.Secondary,
						Async: ramendrv1alpha1.VRGAsyncSpec{
							Mode:               ramendrv1alpha1.AsyncModeEnabled,
							SchedulingInterval: "1h",
						},
						Sync: ramendrv1alpha1.VRGSyncSpec{
							Mode: ramendrv1alpha1.SyncModeDisabled,
						},
						PVCSelector: metav1.LabelSelector{
							MatchLabels: testMatchLabels,
						},
						S3Profiles: []string{"fakeS3Profile"},
					},
				}

				Expect(k8sClient.Create(testCtx, testVrg)).To(Succeed())

				Eventually(func() []string {
					err := k8sClient.Get(testCtx, client.ObjectKeyFromObject(testVrg), testVrg)
					if err != nil {
						return []string{}
					}

					return testVrg.GetFinalizers()
				}, testMaxWait, testInterval).Should(
					ContainElement("volumereplicationgroups.ramendr.openshift.io/vrg-protection"))

				createSecret(testVrg.GetName(), testNamespace.Name)
				createSC()
				createVSC()
			})

			Context("When RDSpec entries are added to vrg spec", func() {
				storageClassName := testStorageClassName

				testCapacity0 := corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				}
				testCapacity1 := corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("20Gi"),
				}

				rd0 := &volsyncv1alpha1.ReplicationDestination{}
				rd1 := &volsyncv1alpha1.ReplicationDestination{}

				JustBeforeEach(func() {
					// Update the vrg spec with some RDSpec entries
					Expect(k8sClient.Get(testCtx, client.ObjectKeyFromObject(testVrg), testVrg)).To(Succeed())
					testVrg.Spec.VolSync.RDSpec = []ramendrv1alpha1.VolSyncReplicationDestinationSpec{
						{
							ProtectedPVC: ramendrv1alpha1.ProtectedPVC{
								Name:               "testingpvc-a",
								ProtectedByVolSync: true,
								StorageClassName:   &storageClassName,
								AccessModes:        testAccessModes,

								Resources: corev1.ResourceRequirements{Requests: testCapacity0},
							},
						},
						{
							ProtectedPVC: ramendrv1alpha1.ProtectedPVC{
								Name:               "testingpvc-b",
								ProtectedByVolSync: true,
								StorageClassName:   &storageClassName,
								AccessModes:        testAccessModes,

								Resources: corev1.ResourceRequirements{Requests: testCapacity1},
							},
						},
					}

					Eventually(func() error {
						return k8sClient.Update(testCtx, testVrg)
					}, testMaxWait, testInterval).Should(Succeed())

					allRDs := &volsyncv1alpha1.ReplicationDestinationList{}
					Eventually(func() int {
						Expect(k8sClient.List(testCtx, allRDs,
							client.InNamespace(testNamespace.GetName()))).To(Succeed())

						return len(allRDs.Items)
					}, testMaxWait, testInterval).Should(Equal(len(testVrg.Spec.VolSync.RDSpec)))

					testLogger.Info("Found RDs", "allRDs", allRDs)

					Expect(k8sClient.Get(testCtx, types.NamespacedName{
						Name:      testVrg.Spec.VolSync.RDSpec[0].ProtectedPVC.Name,
						Namespace: testNamespace.GetName(),
					}, rd0)).To(Succeed())
					Expect(k8sClient.Get(testCtx, types.NamespacedName{
						Name:      testVrg.Spec.VolSync.RDSpec[1].ProtectedPVC.Name,
						Namespace: testNamespace.GetName(),
					}, rd1)).To(Succeed())
				})

				It("Should create ReplicationDestinations for each", func() {
					Expect(rd0.Spec.Trigger).To(BeNil()) // Rsync, so destination will not have schedule
					Expect(rd0.Spec.Rsync).NotTo(BeNil())
					Expect(*rd0.Spec.Rsync.SSHKeys).To(Equal(volsync.GetVolSyncSSHSecretNameFromVRGName(testVrg.GetName())))
					Expect(*rd0.Spec.Rsync.StorageClassName).To(Equal(testStorageClassName))
					Expect(rd0.Spec.Rsync.AccessModes).To(Equal(testAccessModes))
					Expect(rd0.Spec.Rsync.CopyMethod).To(Equal(volsyncv1alpha1.CopyMethodSnapshot))
					Expect(*rd0.Spec.Rsync.ServiceType).To(Equal(corev1.ServiceTypeClusterIP))

					Expect(rd1.Spec.Trigger).To(BeNil()) // Rsync, so destination will not have schedule
					Expect(rd1.Spec.Rsync).NotTo(BeNil())
					Expect(*rd1.Spec.Rsync.SSHKeys).To(Equal(volsync.GetVolSyncSSHSecretNameFromVRGName(testVrg.GetName())))
					Expect(*rd1.Spec.Rsync.StorageClassName).To(Equal(testStorageClassName))
					Expect(rd1.Spec.Rsync.AccessModes).To(Equal(testAccessModes))
					Expect(rd1.Spec.Rsync.CopyMethod).To(Equal(volsyncv1alpha1.CopyMethodSnapshot))
					Expect(*rd1.Spec.Rsync.ServiceType).To(Equal(corev1.ServiceTypeClusterIP))
				})

				Context("When ReplicationDestinations have address set in status and latest Image", func() {
					rd0Address := "99.98.97.96"
					rd1Address := "99.88.77.66"
					JustBeforeEach(func() {
						snapApiGroup := snapv1.GroupName

						// fake address set in status on the ReplicationDestinations
						// and also fake out latestImage
						rd0.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
							Rsync: &volsyncv1alpha1.ReplicationDestinationRsyncStatus{
								Address: &rd0Address,
							},
							LatestImage: &corev1.TypedLocalObjectReference{
								Name:     "my-test-snap0",
								APIGroup: &snapApiGroup,
								Kind:     "VolumeSnapshot",
							},
						}
						Expect(k8sClient.Status().Update(testCtx, rd0)).To(Succeed())

						rd1.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
							Rsync: &volsyncv1alpha1.ReplicationDestinationRsyncStatus{
								Address: &rd1Address,
							},
							LatestImage: &corev1.TypedLocalObjectReference{
								Name:     "my-test-snap1",
								APIGroup: &snapApiGroup,
								Kind:     "VolumeSnapshot",
							},
						}
						Expect(k8sClient.Status().Update(testCtx, rd1)).To(Succeed())
					})

					It("Should update the status of the vrg to indicate data protected", func() {
						Eventually(func() bool {
							err := k8sClient.Get(testCtx, client.ObjectKeyFromObject(testVrg), testVrg)
							if err != nil {
								return false
							}

							testLogger.Info("test VRG looks like this", "testVrg", testVrg)

							for _, cond := range testVrg.Status.Conditions {
								if cond.Type == "DataProtected" {
									return cond.Status == metav1.ConditionTrue
								}
							}

							return false
						}, testMaxWait, interval).Should(BeTrue())
					})
				})

				Context("When a RDSpec is removed from the vrg spec", func() {
					JustBeforeEach(func() {
						Eventually(func() error {
							err := k8sClient.Get(testCtx, client.ObjectKeyFromObject(testVrg), testVrg)
							if err != nil {
								return err
							}

							updatedRDList := []ramendrv1alpha1.VolSyncReplicationDestinationSpec{
								testVrg.Spec.VolSync.RDSpec[0],
							}
							testVrg.Spec.VolSync.RDSpec = updatedRDList

							return k8sClient.Update(testCtx, testVrg)
						}, testMaxWait, testInterval).Should(Succeed())
					})

					It("Should remove the RD that no longer has an RDSpec", func() {
						allRDs := &volsyncv1alpha1.ReplicationDestinationList{}
						Eventually(func() int {
							Expect(k8sClient.List(testCtx, allRDs,
								client.InNamespace(testNamespace.GetName()))).To(Succeed())

							return len(allRDs.Items)
						}, testMaxWait, testInterval).Should(Equal(1))
					})
				})
			})
		})
	})
})

//nolint:funlen
func createPVCBoundToRunningPod(ctx context.Context, namespace string,
	labels map[string]string) *corev1.PersistentVolumeClaim {
	capacity := corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse("1Gi"),
	}
	accessModes := []corev1.PersistentVolumeAccessMode{
		corev1.ReadWriteOnce,
	}

	storageClassName := testStorageClassName

	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "testpvc-",
			Labels:       labels,
			Namespace:    namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			Resources:        corev1.ResourceRequirements{Requests: capacity},
			StorageClassName: &storageClassName,
		},
	}

	Expect(k8sClient.Create(context.TODO(), pvc)).To(Succeed())

	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.AccessModes = accessModes
	pvc.Status.Capacity = capacity
	Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())

	// Create the pod which is mounting the pvc
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "test-mounting-pod-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "c1",
					Image: "testimage123",
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "testvolume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc.GetName(),
						},
					},
				},
			},
		},
	}

	Expect(k8sClient.Create(ctx, pod)).To(Succeed())

	// Set the pod phase
	pod.Status.Phase = corev1.PodRunning

	pod.Status.Conditions = []corev1.PodCondition{
		{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		},
	}

	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

	return pvc
}

func createSecret(vrgName, namespace string) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volsync.GetVolSyncSSHSecretNameFromVRGName(vrgName),
			Namespace: namespace,
		},
	}
	Expect(k8sClient.Create(context.TODO(), secret)).To(Succeed())
	Expect(secret.GetName()).NotTo(BeEmpty())
}

func createSC() {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: testStorageClassName,
		},
		Provisioner: "manual.storage.com",
	}

	err := k8sClient.Create(context.TODO(), sc)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			err = k8sClient.Get(context.TODO(), types.NamespacedName{Name: sc.Name}, sc)
		}
	}

	Expect(err).NotTo(HaveOccurred(),
		"failed to create/get StorageClass %s", sc.Name)
}

func createVSC() {
	vsc := &snapv1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: testVolumeSnapshotClass,
		},
		Driver:         "manual.storage.com",
		DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
	}

	err := k8sClient.Create(context.TODO(), vsc)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			err = k8sClient.Get(context.TODO(), types.NamespacedName{Name: vsc.Name}, vsc)
		}
	}

	Expect(err).NotTo(HaveOccurred(),
		"failed to create/get StorageClass %s", vsc.Name)
}
