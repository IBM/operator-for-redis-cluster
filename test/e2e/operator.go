package e2e

import (
	api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	// for test lisibility
	. "github.com/onsi/ginkgo"
	// for test lisibility
	. "github.com/onsi/gomega"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/client/clientset/versioned"
	"github.com/TheWeatherCompany/icm-redis-operator/test/e2e/framework"
)

func deleteRedisCluster(client versioned.Interface, rediscluster *rapi.RedisCluster) {
	client.Redisoperator().RedisClusters(rediscluster.Namespace).Delete(rediscluster.Name, &metav1.DeleteOptions{})
}

var redisClient versioned.Interface
var kubeClient clientset.Interface
var rediscluster *rapi.RedisCluster

const clusterName = "cluster1"
const clusterNs = api.NamespaceDefault

var _ = BeforeSuite(func() {
	redisClient, kubeClient = framework.BuildAndSetClients()
})

var _ = Describe("RedisCluster CRUD operations", func() {
	It("should create a RedisCluster", func() {
		rediscluster = framework.NewRedisCluster(clusterName, clusterNs, framework.FrameworkContext.ImageTag, 3, 1)
		Eventually(framework.CreateRedisNodeServiceAccountFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())

		Eventually(framework.CreateRedisClusterFunc(redisClient, rediscluster, clusterNs), "5s", "1s").ShouldNot(HaveOccurred())

		Eventually(framework.IsPodDisruptionBudgetCreatedFunc(kubeClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())

		Eventually(framework.IsRedisClusterStartedFunc(redisClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

		Eventually(framework.ZonesBalancedFunc(kubeClient, redisClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
	})
	Context("a RedisCluster is created", func() {
		It("should scale up the RedisCluster", func() {
			nbPrimary := int32(4)
			Eventually(framework.UpdateConfigRedisClusterFunc(redisClient, rediscluster, &nbPrimary, nil), "5s", "1s").ShouldNot(HaveOccurred())

			Eventually(framework.IsRedisClusterStartedFunc(redisClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

			Eventually(framework.ZonesBalancedFunc(kubeClient, redisClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
		})
		Context("a RedisCluster is running", func() {
			When("the number of primaries is reduced", func() {
				It("should scale down the RedisCluster", func() {
					nbPrimary := int32(3)
					Eventually(framework.UpdateConfigRedisClusterFunc(redisClient, rediscluster, &nbPrimary, nil), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(redisClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, redisClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
			When("the number of replicas is increased", func() {
				It("should create additional replicas for each primary in the RedisCluster", func() {
					replicas := int32(2)
					Eventually(framework.UpdateConfigRedisClusterFunc(redisClient, rediscluster, nil, &replicas), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(redisClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, redisClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
			When("the number of replicas is decreased", func() {
				It("should delete replicas for each primary in the RedisCluster", func() {
					replicas := int32(1)
					Eventually(framework.UpdateConfigRedisClusterFunc(redisClient, rediscluster, nil, &replicas), "5s", "1s").ShouldNot(HaveOccurred())

					Eventually(framework.IsRedisClusterStartedFunc(redisClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

					Eventually(framework.ZonesBalancedFunc(kubeClient, redisClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
				})
			})
		})
		It("should update the RedisCluster", func() {
			newTag := "new"
			rediscluster = framework.NewRedisCluster(clusterName, clusterNs, newTag, 3, 1)

			Eventually(framework.UpdateRedisClusterFunc(redisClient, rediscluster, clusterNs), "5s", "1s").ShouldNot(HaveOccurred())

			Eventually(framework.IsPodSpecUpdatedFunc(kubeClient, rediscluster, newTag), "5m", "5s").ShouldNot(HaveOccurred())

			Eventually(framework.IsRedisClusterStartedFunc(redisClient, rediscluster), "5m", "5s").ShouldNot(HaveOccurred())

			Eventually(framework.ZonesBalancedFunc(kubeClient, redisClient, rediscluster), "5s", "1s").ShouldNot(HaveOccurred())
		})
	})

})
