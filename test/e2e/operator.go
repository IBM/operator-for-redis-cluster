package e2e

import (
	"context"

	api "k8s.io/api/core/v1"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"

	// for test lisibility
	. "github.com/onsi/ginkgo"
	// for test lisibility
	. "github.com/onsi/gomega"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/test/e2e/framework"
)

func deleteRedisCluster(kubeClient kclient.Client, rediscluster *rapi.RedisCluster) {
	Expect(kubeClient.Delete(context.Background(), rediscluster)).To(Succeed())
}

const (
	defaultPrimaries = int32(3)
	defaultReplicas  = int32(1)
)

var kubeClient kclient.Client
var rediscluster *rapi.RedisCluster

const clusterName = "cluster1"
const clusterNs = api.NamespaceDefault

var _ = BeforeSuite(func() {
	kubeClient = framework.BuildAndSetClients()
})

var _ = AfterSuite(func() {
	deleteRedisCluster(kubeClient, rediscluster)
})

var _ = Describe("RedisCluster CRUD operations", func() {
	It("should create a RedisCluster", func() {
		rediscluster = framework.NewRedisCluster(clusterName, clusterNs, framework.FrameworkContext.ImageTag, defaultPrimaries, defaultReplicas)
		Eventually(framework.CreateRedisNodeServiceAccountFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())

		Eventually(framework.CreateRedisClusterFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())

		Eventually(framework.IsPodDisruptionBudgetCreatedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())

		Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

		Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
	})
	Context("a RedisCluster is created", func() {
		It("should update the RedisCluster", func() {
			newTag := "new"
			rediscluster = framework.NewRedisCluster(clusterName, clusterNs, newTag, defaultPrimaries, defaultReplicas)

			Eventually(framework.UpdateRedisClusterFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())

			Eventually(framework.IsPodSpecUpdatedFunc(kubeClient, rediscluster, newTag), "5m", "5s").ShouldNot(HaveOccurred())

			Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

			Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
		})
		It("should scale up the RedisCluster", func() {
			nbPrimary := int32(4)
			Eventually(framework.UpdateConfigRedisClusterFunc(kubeClient, rediscluster, &nbPrimary, nil), "5s", "1s").ShouldNot(HaveOccurred())

			Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

			Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
		})
		Context("a RedisCluster is running", func() {
			When("the number of primaries is reduced", func() {
				It("should scale down the RedisCluster", func() {
					nbPrimary := int32(3)
					Eventually(framework.UpdateConfigRedisClusterFunc(kubeClient, rediscluster, &nbPrimary, nil), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
			When("the number of replicas is increased", func() {
				It("should create additional replicas for each primary in the RedisCluster", func() {
					replicas := int32(2)
					Eventually(framework.UpdateConfigRedisClusterFunc(kubeClient, rediscluster, nil, &replicas), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
			When("the number of replicas is decreased", func() {
				It("should delete replicas for each primary in the RedisCluster", func() {
					replicas := int32(1)
					Eventually(framework.UpdateConfigRedisClusterFunc(kubeClient, rediscluster, nil, &replicas), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
			When("the number of primaries is decreased and the number of replicas is increased", func() {
				It("should scale down the primaries and create additional replicas in the RedisCluster", func() {
					nbPrimary := int32(2)
					replicas := int32(2)
					Eventually(framework.UpdateConfigRedisClusterFunc(kubeClient, rediscluster, &nbPrimary, &replicas), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
			When("the number of primaries is increased and the number of replicas is decreased", func() {
				It("should scale up the primaries and delete replicas in the RedisCluster", func() {
					nbPrimary := int32(3)
					replicas := int32(1)
					Eventually(framework.UpdateConfigRedisClusterFunc(kubeClient, rediscluster, &nbPrimary, &replicas), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
			When("the number of primaries is increased and the number of replicas is increased", func() {
				It("should scale up the primaries and create additional replicas in the RedisCluster", func() {
					nbPrimary := int32(4)
					replicas := int32(2)
					Eventually(framework.UpdateConfigRedisClusterFunc(kubeClient, rediscluster, &nbPrimary, &replicas), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
			When("the number of primaries is decreased and the number of replicas is decreased", func() {
				It("should scale down the primaries and delete replicas in the RedisCluster", func() {
					nbPrimary := int32(3)
					replicas := int32(1)
					Eventually(framework.UpdateConfigRedisClusterFunc(kubeClient, rediscluster, &nbPrimary, &replicas), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(kubeClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
		})
	})

})
