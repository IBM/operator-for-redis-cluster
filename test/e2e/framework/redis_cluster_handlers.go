package framework

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/fields"

	"github.com/gogo/protobuf/proto"

	"github.com/IBM/operator-for-redis-cluster/pkg/utils"

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/golang/glog"

	"github.com/onsi/gomega"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	redisImageName = "ibmcom/node-for-redis"
)

// NewRedisCluster builds and returns a new RedisCluster instance
func NewRedisCluster(name, namespace, tag string, nbPrimary, replication int32) *rapi.RedisCluster {
	cluster := &rapi.RedisCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       rapi.ResourceKind,
			APIVersion: rapi.GroupName + "/" + rapi.ResourceVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: rapi.RedisClusterSpec{
			AdditionalLabels:  map[string]string{"foo": "bar"},
			NumberOfPrimaries: &nbPrimary,
			ReplicationFactor: &replication,
			RollingUpdate: &rapi.RollingUpdate{
				Migration: rapi.Migration{
					KeyBatchSize:  proto.Int32(10000),
					SlotBatchSize: proto.Int32(1000),
				},
			},
			Scaling: &rapi.Migration{
				KeyBatchSize:  proto.Int32(10000),
				SlotBatchSize: proto.Int32(1000),
			},
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
							Image:           fmt.Sprintf("%s:%s", redisImageName, tag),
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
								ProbeHandler: v1.ProbeHandler{
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
								ProbeHandler: v1.ProbeHandler{
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
	return rapi.DefaultRedisCluster(cluster)
}

// BuildAndSetClients builds and initializes RedisCluster and kube client
func BuildAndSetClients() kclient.Client {
	f, err := NewFramework()
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	gomega.Ω(f).ShouldNot(gomega.BeNil())

	kubeClient, err := f.kubeClient()
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	gomega.Ω(kubeClient).ShouldNot(gomega.BeNil())
	Logf("Check whether RedisCluster resource is registered...")

	return kubeClient
}

// CreateRedisClusterFunc returns the func to create a RedisCluster
func CreateRedisClusterFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster) func() error {
	return func() error {
		if err := kubeClient.Create(context.Background(), redisCluster); err != nil {
			glog.Warningf("cannot create RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		Logf("RedisCluster created")
		return nil
	}
}

// UpdateRedisClusterFunc returns the func to update a RedisCluster
func UpdateRedisClusterFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster) func() error {
	return func() error {
		cluster := &rapi.RedisCluster{}
		clusterName := types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}
		err := kubeClient.Get(context.Background(), clusterName, cluster)
		if err != nil {
			glog.Warningf("cannot get RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		cluster.Spec = redisCluster.Spec
		if err := kubeClient.Update(context.Background(), cluster); err != nil {
			glog.Warningf("cannot update RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		Logf("RedisCluster updated")
		return nil
	}
}

// IsRedisClusterStartedFunc returns the func that checks whether the RedisCluster is started and configured properly
func IsRedisClusterStartedFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster) func() error {
	return func() error {
		cluster := &rapi.RedisCluster{}
		clusterName := types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}
		err := kubeClient.Get(context.Background(), clusterName, cluster)
		if err != nil {
			glog.Warningf("cannot get RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		totalNbPods := *cluster.Spec.NumberOfPrimaries * (1 + *cluster.Spec.ReplicationFactor)

		if cluster.Status.Cluster.NumberOfPrimaries != *cluster.Spec.NumberOfPrimaries {
			return LogAndReturnErrorf("RedisCluster %s has incorrect number of primary, expected: %d - current: %d", cluster.Name, *cluster.Spec.NumberOfPrimaries, cluster.Status.Cluster.NumberOfPrimaries)
		}

		if cluster.Spec.ReplicationFactor == nil {
			return LogAndReturnErrorf("RedisCluster %s is spec not updated", cluster.Name)
		}

		if cluster.Status.Cluster.MinReplicationFactor != *cluster.Spec.ReplicationFactor {
			return LogAndReturnErrorf("RedisCluster %s has incorrect min replication factor, expected: %v - current: %v", cluster.Name, *cluster.Spec.ReplicationFactor, redisCluster.Status.Cluster.MinReplicationFactor)
		}

		if cluster.Status.Cluster.MaxReplicationFactor != *cluster.Spec.ReplicationFactor {
			return LogAndReturnErrorf("RedisCluster %s has incorrect max replication factor, expected: %d - current: %v", cluster.Name, *cluster.Spec.ReplicationFactor, redisCluster.Status.Cluster.MaxReplicationFactor)
		}

		if cluster.Status.Cluster.NumberOfPods != totalNbPods {
			return LogAndReturnErrorf("RedisCluster %s has incorrect number of pods, expected: %v - current: %v", cluster.Name, totalNbPods, cluster.Status.Cluster.NumberOfPods)
		}

		if cluster.Status.Cluster.Status != rapi.ClusterStatusOK {
			return LogAndReturnErrorf("RedisCluster %s status is not OK, current value: %s", cluster.Name, cluster.Status.Cluster.Status)
		}

		return nil
	}
}

// UpdateConfigRedisClusterFunc returns the func to update the RedisCluster configuration
func UpdateConfigRedisClusterFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster, nbPrimary, replicas *int32) func() error {
	return func() error {
		cluster := &rapi.RedisCluster{}
		clusterName := types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}
		err := kubeClient.Get(context.Background(), clusterName, cluster)
		if err != nil {
			glog.Warningf("cannot get RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		if nbPrimary != nil {
			redisCluster.Spec.NumberOfPrimaries = nbPrimary
			cluster.Spec.NumberOfPrimaries = nbPrimary
		}
		if replicas != nil {
			redisCluster.Spec.ReplicationFactor = replicas
			cluster.Spec.ReplicationFactor = replicas
		}
		if err = kubeClient.Update(context.Background(), cluster); err != nil {
			Logf("cannot update RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}

		Logf("RedisCluster created")
		return nil
	}
}

// CreateRedisClusterConfigMapFunc returns a func to create the RedisCluster server configuration config map
func CreateRedisClusterConfigMapFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster) func() error {
	return func() error {
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      redisCluster.Name,
				Namespace: redisCluster.Namespace,
			},
			Data: map[string]string{
				"redis.yaml": `
maxmemory-policy: volatile-lfu
maxmemory: 6gb
cluster-enabled: yes`,
				"redis.conf": `
maxmemory-policy volatile-lfu
maxmemory 6gb
cluster-enabled yes`,
			},
		}
		if err := kubeClient.Create(context.Background(), configMap); err != nil {
			glog.Warningf("cannot create ConfigMap %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		Logf("ConfigMap created")
		return nil
	}
}

// UpdateRedisClusterConfigMapFunc returns a func to update the RedisCluster server configuration config map data
func UpdateRedisClusterConfigMapFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster, config map[string]string) func() error {
	return func() error {
		cmName := types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}
		cm := &v1.ConfigMap{}
		if err := kubeClient.Get(context.Background(), cmName, cm); err != nil {
			glog.Warningf("cannot get ConfigMap %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		cm.Data = config
		if err := kubeClient.Update(context.Background(), cm); err != nil {
			glog.Warningf("cannot update ConfigMap %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		Logf("ConfigMap updated")
		return nil
	}
}

// GetConfigUpdateEventFunc returns a func to get the UpdateConfig event associated with the RedisCluster
func GetConfigUpdateEventFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster) func() error {
	return func() error {
		events := &v1.EventList{}
		if err := kubeClient.List(context.Background(), events,
			kclient.InNamespace(redisCluster.Namespace),
			kclient.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("involvedObject.name", redisCluster.Name)}); err != nil {
			glog.Warningf("cannot get events for RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		for _, event := range events.Items {
			if event.Reason == "ConfigUpdate" {
				return nil
			}
		}
		return fmt.Errorf("no ConfigUpdate event for RedisCluster %s/%s", redisCluster.Namespace, redisCluster.Name)
	}
}

// ZonesBalancedFunc checks if a RedisCluster's node zones are balanced
func ZonesBalancedFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster) func() error {
	return func() error {
		primaryToReplicas := make(map[string][]rapi.RedisClusterNode)
		primaryToZone := make(map[string]string)
		ctx := context.Background()
		cluster := &rapi.RedisCluster{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}, cluster); err != nil {
			glog.Warningf("cannot get RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		kubeNodes, err := utils.GetKubeNodes(ctx, kubeClient, cluster.Spec.PodTemplate.Spec.NodeSelector)
		if err != nil {
			return err
		}
		// initialize primaries
		for _, node := range cluster.Status.Cluster.Nodes {
			if node.Role == rapi.RedisClusterNodeRolePrimary {
				primaryToReplicas[node.ID] = []rapi.RedisClusterNode{}
				primaryToZone[node.ID] = node.Zone
			}
		}
		// attach replicas to primaries
		for _, node := range cluster.Status.Cluster.Nodes {
			if node.Role == rapi.RedisClusterNodeRoleReplica && node.PrimaryRef != "" {
				primaryToReplicas[node.PrimaryRef] = append(primaryToReplicas[node.PrimaryRef], node)
			}
		}
		if int(*cluster.Spec.ReplicationFactor) < len(utils.GetZones(kubeNodes)) {
			// check for primaries and replicas in same zone
			for primary, replicas := range primaryToReplicas {
				for _, replica := range replicas {
					if replica.Zone == primaryToZone[primary] {
						glog.Warningf("primary node should not be in the same zone as a replica node if RF < number of zones - primary: %s, replica: %s, zone: %s", primary, replica.ID, replica.Zone)
					}
				}
			}
		}
		// check for large zone skew
		zoneToPrimaries, zoneToReplicas := utils.ZoneToRole(cluster.Status.Cluster.Nodes)
		primarySkew, replicaSkew, ok := utils.GetZoneSkewByRole(zoneToPrimaries, zoneToReplicas)
		if !ok {
			if primarySkew > 2 {
				return LogAndReturnErrorf("primary node zones are not balanced, skew is too large: %v", primarySkew)
			}
			if replicaSkew > 2 {
				return LogAndReturnErrorf("replica node zones are not balanced, skew is too large: %v", replicaSkew)
			}
		}
		Logf("RedisCluster node zones are balanced")
		return nil
	}
}

// IsPodSpecUpdatedFunc checks if all RedisCluster pods have the new PodSpec
func IsPodSpecUpdatedFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster, imageTag string) func() error {
	return func() error {
		labelSet := make(map[string]string)
		labelSet[rapi.ClusterNameLabelKey] = redisCluster.Name
		podList := &v1.PodList{}
		err := kubeClient.List(context.Background(), podList, kclient.InNamespace(redisCluster.Namespace), kclient.MatchingLabels(labelSet))
		if err != nil {
			return LogAndReturnErrorf("cannot get RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
		}

		withOldTag := make([]string, 0)
		for _, pod := range podList.Items {
			found := false
			for _, container := range pod.Spec.Containers {
				if container.Name == "redis" {
					found = true
					splitString := strings.Split(container.Image, ":")
					if len(splitString) != 2 {
						return LogAndReturnErrorf("unable to get the tag from the container.Image:%s", container.Image)
					}
					if splitString[1] != imageTag {
						withOldTag = append(withOldTag, pod.Name)
						Logf("pod %q container.Image has the wrong tag:%s, want:%s", pod.Name, splitString[1], imageTag)
					}
				}
			}
			if !found {
				return LogAndReturnErrorf("unable to find the container with name: redis")
			}
		}

		if len(withOldTag) > 0 {
			return LogAndReturnErrorf("%d of %d pods have old tags: %v:", len(withOldTag), len(podList.Items), withOldTag)
		}

		Logf("RedisCluster podSpec updated properly")
		return nil
	}
}

// CreateRedisNodeServiceAccountFunc returns the func to create the service account associated with the redis node
func CreateRedisNodeServiceAccountFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster) func() error {
	return func() error {
		saName := types.NamespacedName{Namespace: redisCluster.Namespace, Name: "redis-node"}
		err := kubeClient.Get(context.Background(), saName, &v1.ServiceAccount{})
		if err != nil && errors.IsNotFound(err) {
			newSA := &v1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-node",
					Namespace: redisCluster.Namespace,
				},
			}
			err = kubeClient.Create(context.Background(), newSA)
			if err != nil {
				return err
			}
		}

		clusterRoleName := types.NamespacedName{Name: "redis-node"}
		err = kubeClient.Get(context.Background(), clusterRoleName, &rbacv1.ClusterRole{})
		if err != nil && errors.IsNotFound(err) {
			cr := &rbacv1.ClusterRole{
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
			err = kubeClient.Create(context.Background(), cr)
			if err != nil {
				return err
			}
		}

		rbName := types.NamespacedName{Name: "redis-node", Namespace: redisCluster.Namespace}
		err = kubeClient.Get(context.Background(), rbName, &rbacv1.RoleBinding{})
		if err != nil && errors.IsNotFound(err) {
			rb := &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-node",
					Namespace: redisCluster.Namespace,
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
						Namespace: redisCluster.Namespace,
					},
				},
			}
			err = kubeClient.Create(context.Background(), rb)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// IsPodDisruptionBudgetCreatedFunc returns the func that checks if the PodDisruptionBudget
// associated with the RedisCluster has been created properly.
func IsPodDisruptionBudgetCreatedFunc(kubeClient kclient.Client, redisCluster *rapi.RedisCluster) func() error {
	return func() error {
		pdbName := types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}
		err := kubeClient.Get(context.Background(), pdbName, &policyv1.PodDisruptionBudget{})
		if err != nil {
			Logf("Cannot get PodDisruptionBudget associated to the redisCluster:%s/%s, err:%v", redisCluster.Namespace, redisCluster.Name, err)
			return err
		}
		return nil
	}
}
