package framework

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/glog"

	"github.com/onsi/gomega"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/client/clientset/versioned"

	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientset "k8s.io/client-go/kubernetes"
)

// NewRedisCluster builds and returns a new RedisCluster instance
func NewRedisCluster(name, namespace, tag string, nbPrimary, replication int32) *v1alpha1.RedisCluster {
	return &v1alpha1.RedisCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.ResourceKind,
			APIVersion: rapi.GroupName + "/" + v1alpha1.ResourceVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1alpha1.RedisClusterSpec{
			AdditionalLabels:  map[string]string{"foo": "bar"},
			NumberOfPrimaries: &nbPrimary,
			ReplicationFactor: &replication,
			PodTemplate: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "redis-cluster",
					},
				},
				Spec: v1.PodSpec{
					ServiceAccountName: "redis-node",
					Volumes: []v1.Volume{
						{Name: "data"},
						{Name: "conf"},
					},
					Containers: []v1.Container{
						{
							Name:            "redis",
							Image:           fmt.Sprintf("icm-redis-node:%s", tag),
							ImagePullPolicy: v1.PullIfNotPresent,
							Args: []string{
								"--v=6",
								"--logtostderr=true",
								"--alsologtostderr=true",
								fmt.Sprintf("--rs=%s", name),
								"--t=10s",
								"--d=10s",
								"--ns=$(POD_NAMESPACE)",
								"--ip=$(POD_IP)",
								"--cluster-node-timeout=2000",
							},
							Ports: []v1.ContainerPort{
								{Name: "redis", ContainerPort: 6379},
								{Name: "cluster", ContainerPort: 16379},
							},
							VolumeMounts: []v1.VolumeMount{
								{Name: "data", MountPath: "/redis-data"},
								{Name: "conf", MountPath: "/redis-conf"},
							},
							Env: []v1.EnvVar{
								{Name: "POD_NAMESPACE", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
								{Name: "POD_IP", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
							},
							LivenessProbe: &v1.Probe{
								Handler: v1.Handler{
									HTTPGet: &v1.HTTPGetAction{
										Path: "/live",
										Port: intstr.FromInt(8080),
									},
								},
								InitialDelaySeconds: 10,
								TimeoutSeconds:      5,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								FailureThreshold:    30,
							},
							ReadinessProbe: &v1.Probe{
								Handler: v1.Handler{
									HTTPGet: &v1.HTTPGetAction{
										Path: "/ready",
										Port: intstr.FromInt(8080),
									},
								},
								InitialDelaySeconds: 10,
								TimeoutSeconds:      5,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								FailureThreshold:    3,
							},
						},
					},
				},
			},
		},
	}
}

// BuildAndSetClients builds and initializes RedisCluster and kube client
func BuildAndSetClients() (versioned.Interface, clientset.Interface) {
	f, err := NewFramework()
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	gomega.Ω(f).ShouldNot(gomega.BeNil())

	kubeClient, err := f.kubeClient()
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	gomega.Ω(kubeClient).ShouldNot(gomega.BeNil())
	Logf("Check whether RedisCluster resource is registered...")

	redisClient, err := f.redisOperatorClient()
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	gomega.Ω(redisClient).ShouldNot(gomega.BeNil())
	return redisClient, kubeClient
}

// CreateRedisClusterFunc returns the func to create a RedisCluster
func CreateRedisClusterFunc(client versioned.Interface, rediscluster *v1alpha1.RedisCluster, namespace string) func() error {
	return func() error {
		if _, err := client.RedisoperatorV1().RedisClusters(namespace).Create(rediscluster); err != nil {
			glog.Warningf("cannot create RedisCluster %s/%s: %v", namespace, rediscluster.Name, err)
			return err
		}
		Logf("RedisCluster created")
		return nil
	}
}

// UpdateRedisClusterFunc returns the func to update a RedisCluster
func UpdateRedisClusterFunc(client versioned.Interface, rediscluster *v1alpha1.RedisCluster, namespace string) func() error {
	return func() error {
		cluster, err := client.RedisoperatorV1().RedisClusters(rediscluster.Namespace).Get(rediscluster.Name, metav1.GetOptions{})
		if err != nil {
			glog.Warningf("cannot get RedisCluster %s/%s: %v", rediscluster.Namespace, rediscluster.Name, err)
			return err
		}
		cluster.Spec = rediscluster.Spec
		if _, err := client.RedisoperatorV1().RedisClusters(namespace).Update(cluster); err != nil {
			glog.Warningf("cannot update RedisCluster %s/%s: %v", namespace, rediscluster.Name, err)
			return err
		}
		Logf("RedisCluster updated")
		return nil
	}
}

// IsRedisClusterStartedFunc returns the func that checks whether or not the RedisCluster is started and configured properly
func IsRedisClusterStartedFunc(client versioned.Interface, rediscluster *v1alpha1.RedisCluster) func() error {
	return func() error {
		cluster, err := client.RedisoperatorV1().RedisClusters(rediscluster.Namespace).Get(rediscluster.Name, metav1.GetOptions{})
		if err != nil {
			glog.Warningf("cannot get RedisCluster %s/%s: %v", rediscluster.Namespace, rediscluster.Name, err)
			return err
		}

		if cluster.Status.Cluster.NumberOfPrimaries != *cluster.Spec.NumberOfPrimaries {
			return LogAndReturnErrorf("RedisCluster %s has incorrect number of primary, expected: %d - current:%d", cluster.Name, *cluster.Spec.NumberOfPrimaries, cluster.Status.Cluster.NumberOfPrimaries)
		}

		if cluster.Spec.ReplicationFactor == nil {
			return LogAndReturnErrorf("RedisCluster %s is spec not updated", cluster.Name)
		}

		if cluster.Status.Cluster.MinReplicationFactor != *cluster.Spec.ReplicationFactor {
			return LogAndReturnErrorf("RedisCluster %s has incorrect min replication factor, expected: %v - current: %v ", cluster.Name, *cluster.Spec.ReplicationFactor, rediscluster.Status.Cluster.MinReplicationFactor)
		}

		if cluster.Status.Cluster.MaxReplicationFactor != *cluster.Spec.ReplicationFactor {
			return LogAndReturnErrorf("RedisCluster %s has incorrect max replication factor, expected: %d - current: %v ", cluster.Name, *cluster.Spec.ReplicationFactor, rediscluster.Status.Cluster.MaxReplicationFactor)
		}

		if cluster.Status.Cluster.Status != v1alpha1.ClusterStatusOK {
			return LogAndReturnErrorf("RedisCluster %s status is not OK, current value: %s", cluster.Name, cluster.Status.Cluster.Status)
		}

		return nil
	}
}

// UpdateConfigRedisClusterFunc returns the func to update the RedisCluster configuration
func UpdateConfigRedisClusterFunc(client versioned.Interface, rediscluster *v1alpha1.RedisCluster, nbPrimary, replicas *int32) func() error {
	return func() error {
		cluster, err := client.RedisoperatorV1().RedisClusters(rediscluster.Namespace).Get(rediscluster.Name, metav1.GetOptions{})
		if err != nil {
			glog.Warningf("cannot get RedisCluster %s/%s: %v", rediscluster.Namespace, rediscluster.Name, err)
			return err
		}
		if nbPrimary != nil {
			rediscluster.Spec.NumberOfPrimaries = nbPrimary
			cluster.Spec.NumberOfPrimaries = nbPrimary
		}
		if replicas != nil {
			rediscluster.Spec.ReplicationFactor = replicas
			cluster.Spec.ReplicationFactor = replicas
		}
		if _, err := client.RedisoperatorV1().RedisClusters(rediscluster.Namespace).Update(cluster); err != nil {
			Logf("cannot update RedisCluster %s/%s: %v", rediscluster.Namespace, rediscluster.Name, err)
			return err
		}

		Logf("RedisCluster created")
		return nil
	}
}

// ZonesBalancedFunc checks if the RedisCluster node's zones are balanced
func ZonesBalancedFunc(kubeClient clientset.Interface, redisClient versioned.Interface, rediscluster *v1alpha1.RedisCluster) func() error {
	return func() error {
		zoneToPrimaries := make(map[string][]v1alpha1.RedisClusterNode)
		zoneToReplicas := make(map[string][]v1alpha1.RedisClusterNode)
		idToPrimary := make(map[string]v1alpha1.RedisClusterNode)
		ctx := context.Background()

		cluster, err := redisClient.RedisoperatorV1().RedisClusters(rediscluster.Namespace).Get(rediscluster.Name, metav1.GetOptions{})
		if err != nil {
			glog.Warningf("cannot get RedisCluster %s/%s: %v", rediscluster.Namespace, rediscluster.Name, err)
			return err
		}
		nodes := cluster.Status.Cluster.Nodes
		labelSelector := labels.Set(cluster.Spec.NodeSelector).String()

		nodeList, err := kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return LogAndReturnErrorf("error getting k8s nodes with label selector %s", labelSelector)
		}
		kubeNodes := nodeList.Items
		zones := getZonesFromKubeNodes(kubeNodes)
		for _, node := range nodes {
			pod, err := kubeClient.CoreV1().Pods(cluster.Namespace).Get(ctx, node.PodName, metav1.GetOptions{})
			if err != nil {
				return LogAndReturnErrorf("error getting pod for redis node %s", node.ID)
			}
			addNodeToMaps(node, pod.Spec.NodeName, kubeNodes, idToPrimary, zoneToPrimaries, zoneToReplicas)
		}
		// check for primary and replica in the same zone
		if int(*cluster.Spec.ReplicationFactor) < len(zones) {
			for zone, replicas := range zoneToReplicas {
				for _, node := range replicas {
					if sameZone(node, zone, idToPrimary, kubeNodes) {
						return LogAndReturnErrorf("primary node cannot be in the same zone as a replica node if RF < number of zones")
					}
				}
			}
		}
		// check for large zone skew
		if err = zonesSkewed(zoneToPrimaries, zoneToReplicas); err != nil {
			return err
		}
		Logf("RedisCluster node zones are balanced")
		return nil
	}
}

// IsPodSpecUpdatedFunc checks if all RedisCluster pods have the new PodSpec
func IsPodSpecUpdatedFunc(client clientset.Interface, rediscluster *v1alpha1.RedisCluster, imageTag string) func() error {
	return func() error {
		labelSet := labels.Set{}
		labelSet[v1alpha1.ClusterNameLabelKey] = rediscluster.Name
		podList, err := client.CoreV1().Pods(rediscluster.Namespace).List(context.Background(), metav1.ListOptions{LabelSelector: labelSet.AsSelector().String()})
		if err != nil {
			return LogAndReturnErrorf("cannot get RedisCluster %s/%s: %v", rediscluster.Namespace, rediscluster.Name, err)
		}

		for _, pod := range podList.Items {
			found := false
			for _, container := range pod.Spec.Containers {
				if container.Name == "redis" {
					found = true
					splitString := strings.Split(container.Image, ":")
					if len(splitString) != 2 {
						LogAndReturnErrorf("unable to get the tag from the container.Image:%s", container.Image)
					}
					if splitString[1] != imageTag {
						LogAndReturnErrorf("current container.Image have a wrong tag:%s, want:%s", splitString[1], imageTag)
					}
				}
			}
			if !found {
				LogAndReturnErrorf("unable to found the container with name: redis")
			}
		}

		Logf("RedisCluster podSpec updated properly")
		return nil
	}
}

// CreateRedisNodeServiceAccountFunc returns the func to create the service account associated with the redis node
func CreateRedisNodeServiceAccountFunc(client clientset.Interface, rediscluster *v1alpha1.RedisCluster) func() error {
	return func() error {
		_, err := client.CoreV1().ServiceAccounts(rediscluster.Namespace).Get(context.Background(), "redis-node", metav1.GetOptions{})
		if err != nil && errors.IsNotFound(err) {
			newSa := v1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name: "redis-node",
				},
			}
			_, err = client.CoreV1().ServiceAccounts(rediscluster.Namespace).Create(context.Background(), &newSa, metav1.CreateOptions{})
			if err != nil {
				return err
			}
		}

		_, err = client.RbacV1().ClusterRoles().Get(context.Background(), "redis-node", metav1.GetOptions{})
		if err != nil && errors.IsNotFound(err) {
			cr := rbacv1.ClusterRole{
				ObjectMeta: metav1.ObjectMeta{
					Name: "redis-node",
				},
				Rules: []rbacv1.PolicyRule{
					{
						APIGroups: []string{""},
						Resources: []string{"namespaces", "services", "endpoints", "pods"},
						Verbs:     []string{"list", "get"},
					},
				},
			}
			_, err = client.RbacV1().ClusterRoles().Create(context.Background(), &cr, metav1.CreateOptions{})
			if err != nil {
				return err
			}
		}
		_, err = client.RbacV1().RoleBindings(rediscluster.Namespace).Get(context.Background(), "redis-node", metav1.GetOptions{})
		if err != nil && errors.IsNotFound(err) {
			rb := rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name: "redis-node",
				},
				RoleRef: rbacv1.RoleRef{
					APIGroup: "rbac.authorization.k8s.io",
					Kind:     "ClusterRole",
					Name:     "redis-node",
				},
				Subjects: []rbacv1.Subject{
					{
						Kind:      "ServiceAccount",
						Name:      "redis-node",
						Namespace: rediscluster.Namespace,
					},
				},
			}
			_, err = client.RbacV1().RoleBindings(rediscluster.Namespace).Create(context.Background(), &rb, metav1.CreateOptions{})
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// IsPodDisruptionBudgetCreatedFunc returns the func that checks if the PodDisruptionBudget
// associated with the the RedisCluster has been created properly.
func IsPodDisruptionBudgetCreatedFunc(client clientset.Interface, rediscluster *v1alpha1.RedisCluster) func() error {
	return func() error {
		_, err := client.PolicyV1beta1().PodDisruptionBudgets(rediscluster.Namespace).Get(context.Background(), rediscluster.Name, metav1.GetOptions{})
		if err != nil {
			Logf("Cannot get PodDisruptionBudget associated to the rediscluster:%s/%s, err:%v", rediscluster.Namespace, rediscluster.Name, err)
			return err
		}
		return nil
	}
}
