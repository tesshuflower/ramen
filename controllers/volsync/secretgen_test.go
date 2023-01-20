// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package volsync_test

import (
	"crypto/x509"
	"encoding/pem"

	"golang.org/x/crypto/ssh"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/controllers/volsync"
)

var _ = Describe("Secretgen", func() {
	var testNamespace *corev1.Namespace
	var owner metav1.Object

	BeforeEach(func() {
		// Create namespace for test
		testNamespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "sg-test-",
			},
		}
		Expect(k8sClient.Create(ctx, testNamespace)).To(Succeed())
		Expect(testNamespace.GetName()).NotTo(BeEmpty())

		// Create dummy resource to be the "owner" of the generated secret
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
	})

	AfterEach(func() {
		// All resources are namespaced, so this should clean it all up
		Expect(k8sClient.Delete(ctx, testNamespace)).To(Succeed())
	})

	Describe("Reconcile volsync rsync secret", func() {
		testSSHSecretName := "test-secret-rsync"
		testTLSSecretName := "test-secret-rsync-tls-psk"

		JustBeforeEach(func() {
			testSSHSecret, err := volsync.ReconcileVolSyncReplicationSecret(ctx, k8sClient, owner,
				testSSHSecretName, testNamespace.GetName(), false /* old rsync ssh mover */, logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(testSSHSecret.GetName()).To(Equal(testSSHSecretName))
			Expect(testSSHSecret.GetNamespace()).To(Equal(testNamespace.GetName()))

			testTLSSecret, err := volsync.ReconcileVolSyncReplicationSecret(ctx, k8sClient, owner,
				testTLSSecretName, testNamespace.GetName(), true /* new rsync-tls mover */, logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(testTLSSecret.GetName()).To(Equal(testTLSSecretName))
			Expect(testTLSSecret.GetNamespace()).To(Equal(testNamespace.GetName()))
		})

		Context("When the secret does not previously exist", func() {
			It("Should create a volsync rsync secret", func() {
				//
				// Validate Rsync SSH secret has been created properly
				//
				// Re-load secret to make sure it's been created properly
				newSSHSecret := &corev1.Secret{}
				Eventually(func() error {
					return k8sClient.Get(ctx,
						types.NamespacedName{Name: testSSHSecretName, Namespace: testNamespace.GetName()}, newSSHSecret)
				}, maxWait, interval).Should(Succeed())

				// Expect the secret should be owned by owner
				Expect(ownerMatches(newSSHSecret, owner.GetName(), "ConfigMap", true))

				// Check secret data
				Expect(len(newSSHSecret.Data)).To(Equal(4))

				sourceBytes, ok := newSSHSecret.Data["source"]
				Expect(ok).To(BeTrue())
				sourcePubBytes, ok := newSSHSecret.Data["source.pub"]
				Expect(ok).To(BeTrue())
				validateKeyPair(sourceBytes, sourcePubBytes)

				destBytes, ok := newSSHSecret.Data["destination"]
				Expect(ok).To(BeTrue())
				destPubBytes, ok := newSSHSecret.Data["destination.pub"]
				Expect(ok).To(BeTrue())
				validateKeyPair(destBytes, destPubBytes)

				//
				// Validate Rsync-TLS secret has been created properly
				//
				// Re-load the rsync-tls secret to make sure it's been created properly
				newTLSSecret := &corev1.Secret{}
				Eventually(func() error {
					return k8sClient.Get(ctx,
						types.NamespacedName{Name: testTLSSecretName, Namespace: testNamespace.GetName()}, newTLSSecret)
				}, maxWait, interval).Should(Succeed())

				// Expect the secret should be owned by owner
				Expect(ownerMatches(newTLSSecret, owner.GetName(), "ConfigMap", true))

				// Check secret data
				Expect(len(newTLSSecret.Data)).To(Equal(1))

				tlsPskData, ok := newTLSSecret.Data["psk.txt"]
				Expect(ok).To(BeTrue())

				tlsPsk := string(tlsPskData)
				Expect(tlsPsk).To(ContainSubstring("volsyncramen:"))
			})
		})

		Context("When the secret already exists", func() {
			var existingSSHSecret *corev1.Secret
			var existingTLSSecret *corev1.Secret
			BeforeEach(func() {
				existingSSHSecret = &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSSHSecretName,
						Namespace: testNamespace.GetName(),
					},
					StringData: map[string]string{
						"a":          "b",
						"anotherkey": "anothervalue",
					},
				}
				Expect(k8sClient.Create(ctx, existingSSHSecret)).To(Succeed())

				// Make sure secret has been created, and in client cache
				Eventually(func() error {
					return k8sClient.Get(ctx, client.ObjectKeyFromObject(existingSSHSecret), existingSSHSecret)
				}, maxWait, interval).Should(Succeed())

				existingTLSSecret = &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testTLSSecretName,
						Namespace: testNamespace.GetName(),
					},
					StringData: map[string]string{
						"psk.txt": "testing1:6063bb702a13a6c237e56b2f6bd9e3014699e0cec6b3e5d1844ee5f2d2d545e4",
					},
				}
				Expect(k8sClient.Create(ctx, existingTLSSecret)).To(Succeed())

				// Make sure secret has been created, and in client cache
				Eventually(func() error {
					return k8sClient.Get(ctx, client.ObjectKeyFromObject(existingTLSSecret), existingTLSSecret)
				}, maxWait, interval).Should(Succeed())
			})

			It("Should leave the existing secret unchanged", func() {
				// Re-load secret to make sure it's been created properly
				sshSecret := &corev1.Secret{}
				Eventually(func() error {
					return k8sClient.Get(ctx,
						types.NamespacedName{Name: testSSHSecretName, Namespace: testNamespace.GetName()}, sshSecret)
				}, maxWait, interval).Should(Succeed())

				Expect(sshSecret.Data).To(Equal(existingSSHSecret.Data))

				tlsSecret := &corev1.Secret{}
				Eventually(func() error {
					return k8sClient.Get(ctx,
						types.NamespacedName{Name: testTLSSecretName, Namespace: testNamespace.GetName()}, tlsSecret)
				}, maxWait, interval).Should(Succeed())

				Expect(tlsSecret.Data).To(Equal(existingTLSSecret.Data))
			})
		})
	})
})

func validateKeyPair(privateKeyData, publicKeyData []byte) {
	pemBlock, _ := pem.Decode(privateKeyData)
	Expect(pemBlock).NotTo(BeNil())
	Expect(pemBlock.Type).To(Equal("RSA PRIVATE KEY"))

	rsaPrivKey, err := x509.ParsePKCS1PrivateKey(pemBlock.Bytes)
	Expect(err).NotTo(HaveOccurred())
	Expect(rsaPrivKey).NotTo(BeNil())
	Expect(rsaPrivKey.Validate()).To(Succeed())

	// One more check using ssh to see if private matches public
	sshSigner, err := ssh.ParsePrivateKey(privateKeyData)
	Expect(err).NotTo(HaveOccurred())

	sshPubKey, comment, _, _, err := ssh.ParseAuthorizedKey(publicKeyData)
	Expect(err).NotTo(HaveOccurred())
	Expect(comment).To(Equal(""))

	Expect(sshSigner.PublicKey()).To(Equal(sshPubKey))
}
