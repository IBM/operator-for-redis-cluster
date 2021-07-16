package controller

import (
	"reflect"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"

	"github.com/golang/glog"

	kapi "k8s.io/api/core/v1"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	podctrl "github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/pod"
)

func compareStatus(old, new *rapi.RedisClusterState) bool {
	if compareStringValue("ClusterStatus", string(old.Status), string(new.Status)) {
		return true
	}
	if compareInts("NumberOfPods", old.NumberOfPods, new.NumberOfPods) {
		return true
	}
	if compareInts("NumberOfPodsReady", old.NumberOfPodsReady, new.NumberOfPodsReady) {
		return true
	}
	if compareInts("NumberOfRedisNodesRunning", old.NumberOfRedisNodesRunning, new.NumberOfRedisNodesRunning) {
		return true
	}
	if compareInts("NumberOfPrimaries", old.NumberOfPrimaries, new.NumberOfPrimaries) {
		return true
	}
	if compareInts("MinReplicationFactor", old.MinReplicationFactor, new.MinReplicationFactor) {
		return true
	}
	if compareInts("MaxReplicationFactor", old.MaxReplicationFactor, new.MaxReplicationFactor) {
		return true
	}
	if compareStringValue("ClusterStatus", string(old.Status), string(new.Status)) {
		return true
	}
	if compareStringValue("NodesPlacement", string(old.NodesPlacement), string(new.NodesPlacement)) {
		return true
	}
	if compareInts("len(Nodes)", int32(len(old.Nodes)), int32(len(new.Nodes))) {
		return true
	}

	if len(old.Nodes) != len(new.Nodes) {
		return true
	}
	for _, nodeA := range old.Nodes {
		found := false
		for _, nodeB := range new.Nodes {
			if nodeA.ID == nodeB.ID {
				found = true
				if compareNodes(&nodeA, &nodeB) {
					return true
				}
			}
		}
		if !found {
			return true
		}
	}

	return false
}

// filterLostNodes divides pods into lost and other
func filterLostNodes(pods []kapi.Pod) (ok []kapi.Pod, ko []kapi.Pod) {
	for i, pod := range pods {
		if pod.Status.Reason == "NodeLost" {
			ko = append(ko, pods[i])
		} else {
			ok = append(ok, pods[i])
		}
	}
	return ok, ko
}

func compareNodes(nodeA, nodeB *rapi.RedisClusterNode) bool {
	if compareStringValue("Node.IP", nodeA.IP, nodeB.IP) {
		return true
	}
	if compareStringValue("Node.PrimaryRef", nodeA.PrimaryRef, nodeB.PrimaryRef) {
		return true
	}
	if compareStringValue("Node.PodName", nodeA.PodName, nodeB.PodName) {
		return true
	}
	if compareStringValue("Node.Port", nodeA.Port, nodeB.Port) {
		return true
	}
	if compareStringValue("Node.Role", string(nodeA.Role), string(nodeB.Role)) {
		return true
	}

	sizeSlotsA := 0
	sizeSlotsB := 0
	if nodeA.Slots != nil {
		sizeSlotsA = len(nodeA.Slots)
	}
	if nodeB.Slots != nil {
		sizeSlotsB = len(nodeB.Slots)
	}
	if sizeSlotsA != sizeSlotsB {
		glog.Infof("compare Node.Slote size: %d - %d", sizeSlotsA, sizeSlotsB)
		return true
	}

	if (sizeSlotsA != 0) && !reflect.DeepEqual(nodeA.Slots, nodeB.Slots) {
		glog.Infof("compare Node.Slote deepEqual: %v - %v", nodeA.Slots, nodeB.Slots)
		return true
	}

	return false
}

func compareIntValue(name string, old, new *int32) bool {
	if old == nil && new == nil {
		return true
	} else if old == nil || new == nil {
		return false
	} else if *old != *new {
		glog.Infof("compare status.%s: %d - %d", name, *old, *new)
		return true
	}

	return false
}

func compareInts(name string, old, new int32) bool {
	if old != new {
		glog.Infof("compare status.%s: %d - %d", name, old, new)
		return true
	}

	return false
}

func compareStringValue(name string, old, new string) bool {
	if old != new {
		glog.V(6).Infof("compare %s: %s - %s", name, old, new)
		return true
	}

	return false
}

func needClusterOperation(cluster *rapi.RedisCluster) bool {
	if needRollingUpdate(cluster) {
		glog.V(6).Info("needClusterOperation---needRollingUpdate")
		return true
	}

	if needMorePods(cluster) {
		glog.V(6).Info("needClusterOperation---needMorePods")
		return true
	}

	if needLessPods(cluster) {
		glog.Info("needClusterOperation---needLessPods")
		return true
	}

	if compareIntValue("NumberOfPrimaries", &cluster.Status.Cluster.NumberOfPrimaries, cluster.Spec.NumberOfPrimaries) {
		glog.V(6).Info("needClusterOperation---NumberOfPrimaries")
		return true
	}

	if compareIntValue("MinReplicationFactor", &cluster.Status.Cluster.MinReplicationFactor, cluster.Spec.ReplicationFactor) {
		glog.V(6).Info("needClusterOperation---MinReplicationFactor")
		return true
	}

	if compareIntValue("MaxReplicationFactor", &cluster.Status.Cluster.MaxReplicationFactor, cluster.Spec.ReplicationFactor) {
		glog.V(6).Info("needClusterOperation---MaxReplicationFactor")
		return true
	}

	return false
}

func needRollingUpdate(cluster *rapi.RedisCluster) bool {
	return !comparePodsWithPodTemplate(cluster)
}

func comparePodsWithPodTemplate(cluster *rapi.RedisCluster) bool {
	clusterPodSpecHash, _ := podctrl.GenerateMD5Spec(&cluster.Spec.PodTemplate.Spec)
	for _, node := range cluster.Status.Cluster.Nodes {
		if node.Pod == nil {
			continue
		}
		if !comparePodSpecMD5Hash(clusterPodSpecHash, node.Pod) {
			return false
		}
	}

	return true
}

func comparePodSpecMD5Hash(hash string, pod *kapi.Pod) bool {
	if val, ok := pod.Annotations[rapi.PodSpecMD5LabelKey]; ok {
		if val != hash {
			return false
		}
	} else {
		return false
	}

	return true
}

func needMorePods(cluster *rapi.RedisCluster) bool {
	nbPodNeed := *cluster.Spec.NumberOfPrimaries * (1 + *cluster.Spec.ReplicationFactor)

	if cluster.Status.Cluster.NumberOfPods != cluster.Status.Cluster.NumberOfPodsReady {
		return false
	}
	output := false
	if cluster.Status.Cluster.NumberOfPods < nbPodNeed {
		glog.V(4).Infof("Not enough pods running to apply the cluster [%s-%s] spec, current %d, needed %d ", cluster.Namespace, cluster.Name, cluster.Status.Cluster.NumberOfPodsReady, nbPodNeed)
		output = true
	}

	return output
}

func needLessPods(cluster *rapi.RedisCluster) bool {
	nbPodNeed := *cluster.Spec.NumberOfPrimaries * (1 + *cluster.Spec.ReplicationFactor)

	if cluster.Status.Cluster.NumberOfPods != cluster.Status.Cluster.NumberOfPodsReady {
		return false
	}
	output := false
	if cluster.Status.Cluster.NumberOfPods > nbPodNeed {
		glog.V(4).Infof("Too many pods running, need to scale down the cluster [%s-%s], current %d, needed %d ", cluster.Namespace, cluster.Name, cluster.Status.Cluster.NumberOfPods, nbPodNeed)
		output = true
	}
	return output
}

func generatePrimaryToReplicas(cluster *rapi.RedisCluster, rCluster *redis.Cluster) map[string]redis.Nodes {
	primaryToReplicas := make(map[string]redis.Nodes)
	for _, node := range cluster.Status.Cluster.Nodes {
		switch node.Role {
		case rapi.RedisClusterNodeRolePrimary:
			if _, ok := primaryToReplicas[node.ID]; !ok {
				primaryToReplicas[node.ID] = redis.Nodes{}
			}
		case rapi.RedisClusterNodeRoleReplica:
			replica, err := rCluster.GetNodeByID(node.ID)
			if err != nil {
				glog.Errorf("unable to find the node with redis ID:%s", node.ID)
			}
			if replica != nil && node.PrimaryRef != "" {
				primaryToReplicas[node.PrimaryRef] = append(primaryToReplicas[node.PrimaryRef], replica)
			}
		}
	}
	return primaryToReplicas
}

// checkReplicationFactor checks the primary replication factor.
// It returns a map with the primary IDs as key and list of replicas as value
// The second returned value is a boolean. True if replicationFactor is correct for each primary,
// otherwise it returns false
func checkReplicationFactor(cluster *rapi.RedisCluster, rCluster *redis.Cluster) (map[string]redis.Nodes, bool) {
	primaryToReplicas := generatePrimaryToReplicas(cluster, rCluster)
	if (cluster.Status.Cluster.MaxReplicationFactor != cluster.Status.Cluster.MinReplicationFactor) || (*cluster.Spec.ReplicationFactor != cluster.Status.Cluster.MaxReplicationFactor) {
		return primaryToReplicas, false
	}
	return primaryToReplicas, true
}

// checkNumberOfPrimaries returns the difference between the number of primaries currently existing and the number of desired primaries
// also returns true if the number of primary status is equal to the spec
func checkNumberOfPrimaries(cluster *rapi.RedisCluster) (int32, bool) {
	nbPrimarySpec := *cluster.Spec.NumberOfPrimaries
	nbPrimaryStatus := cluster.Status.Cluster.NumberOfPrimaries
	same := (nbPrimaryStatus) == nbPrimarySpec
	return nbPrimaryStatus - nbPrimarySpec, same
}

// shouldDeleteNodes use to detect if some nodes can be removed without impacting the cluster
// returns true if there are nodes to be deleted, false otherwise
func shouldDeleteNodes(cluster *rapi.RedisCluster, newCluster *redis.Cluster) ([]*rapi.RedisClusterNode, bool) {
	uselessNodes := []*rapi.RedisClusterNode{}
	if !needLessPods(cluster) {
		return uselessNodes, false
	}

	_, primaryOK := checkNumberOfPrimaries(cluster)
	_, replicaOK := checkReplicationFactor(cluster, newCluster)
	if !primaryOK || !replicaOK {
		return uselessNodes, false
	}

	for i, node := range cluster.Status.Cluster.Nodes {
		if node.Role == rapi.RedisClusterNodeRolePrimary && len(node.Slots) == 0 {
			uselessNodes = append(uselessNodes, &cluster.Status.Cluster.Nodes[i])
		}
		if node.Role == rapi.RedisClusterNodeRoleNone {
			uselessNodes = append(uselessNodes, &cluster.Status.Cluster.Nodes[i])
		}
	}

	return uselessNodes, true
}

func checkReplicasOfReplica(cluster *rapi.RedisCluster) (map[string][]*rapi.RedisClusterNode, bool) {
	replicasOfReplica := make(map[string][]*rapi.RedisClusterNode)

	for i, nodeA := range cluster.Status.Cluster.Nodes {
		if nodeA.Role != rapi.RedisClusterNodeRoleReplica {
			continue
		}
		if nodeA.PrimaryRef != "" {
			isReplica := false
			for _, nodeB := range cluster.Status.Cluster.Nodes {
				if nodeB.ID == nodeA.PrimaryRef && nodeB.PrimaryRef != "" {
					isReplica = true
					break
				}
			}
			if isReplica {
				replicasOfReplica[nodeA.PrimaryRef] = append(replicasOfReplica[nodeA.PrimaryRef], &cluster.Status.Cluster.Nodes[i])
			}
		}

	}
	return replicasOfReplica, len(replicasOfReplica) == 0
}
