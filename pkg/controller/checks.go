package controller

import (
	"reflect"

	"github.com/golang/glog"

	kapi "k8s.io/api/core/v1"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1"
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
	if compareInts("NumberOfMasters", old.NumberOfMasters, new.NumberOfMasters) {
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
func filterLostNodes(pods []*kapi.Pod) (ok []*kapi.Pod, ko []*kapi.Pod) {
	for _, pod := range pods {
		if pod.Status.Reason == "NodeLost" {
			ko = append(ko, pod)
		} else {
			ok = append(ok, pod)
		}
	}
	return ok, ko
}

func compareNodes(nodeA, nodeB *rapi.RedisClusterNode) bool {
	if compareStringValue("Node.IP", nodeA.IP, nodeB.IP) {
		return true
	}
	if compareStringValue("Node.MasterRef", nodeA.MasterRef, nodeB.MasterRef) {
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

	if compareIntValue("NumberOfMasters", &cluster.Status.Cluster.NumberOfMasters, cluster.Spec.NumberOfMaster) {
		glog.V(6).Info("needClusterOperation---NumberOfMasters")
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
	nbPodNeed := *cluster.Spec.NumberOfMaster * (1 + *cluster.Spec.ReplicationFactor)

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
	nbPodNeed := *cluster.Spec.NumberOfMaster * (1 + *cluster.Spec.ReplicationFactor)

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

// checkReplicationFactor checks the master replication factor.
// It returns a map with the master IDs as key and list of Slave ID as value
// The second returned value is a boolean. True if replicationFactor is correct for each master,
// otherwise it returns false
func checkReplicationFactor(cluster *rapi.RedisCluster) (map[string][]string, bool) {
	masterToSlaves := make(map[string][]string)
	for _, node := range cluster.Status.Cluster.Nodes {
		switch node.Role {
		case rapi.RedisClusterNodeRoleMaster:
			if _, ok := masterToSlaves[node.ID]; !ok {
				masterToSlaves[node.ID] = []string{}
			}
		case rapi.RedisClusterNodeRoleSlave:
			if node.MasterRef != "" {
				masterToSlaves[node.MasterRef] = append(masterToSlaves[node.MasterRef], node.ID)
			}
		}
	}
	if (cluster.Status.Cluster.MaxReplicationFactor != cluster.Status.Cluster.MinReplicationFactor) || (*cluster.Spec.ReplicationFactor != cluster.Status.Cluster.MaxReplicationFactor) {
		return masterToSlaves, false
	}

	return masterToSlaves, true
}

// checkNumberOfMasters returns the difference between the number of masters currently existing and the number of desired masters
// also returns true if the number of master status is equal to the spec
func checkNumberOfMasters(cluster *rapi.RedisCluster) (int32, bool) {
	nbMasterSpec := *cluster.Spec.NumberOfMaster
	nbMasterStatus := cluster.Status.Cluster.NumberOfMasters
	same := (nbMasterStatus) == nbMasterSpec
	return nbMasterStatus - nbMasterSpec, same
}

// shouldDeleteNodes use to detect if some nodes can be removed without impacting the cluster
// returns true if there are nodes to be deleted, false otherwise
func shouldDeleteNodes(cluster *rapi.RedisCluster) ([]*rapi.RedisClusterNode, bool) {
	uselessNodes := []*rapi.RedisClusterNode{}
	if !needLessPods(cluster) {
		return uselessNodes, false
	}

	_, masterOK := checkNumberOfMasters(cluster)
	_, slaveOK := checkReplicationFactor(cluster)
	if !masterOK || !slaveOK {
		return uselessNodes, false
	}

	for _, node := range cluster.Status.Cluster.Nodes {
		if node.Role == rapi.RedisClusterNodeRoleMaster && len(node.Slots) == 0 {
			uselessNodes = append(uselessNodes, &node)
		}
		if node.Role == rapi.RedisClusterNodeRoleNone {
			uselessNodes = append(uselessNodes, &node)
		}
	}

	return uselessNodes, true
}

func checkSlavesOfSlave(cluster *rapi.RedisCluster) (map[string][]*rapi.RedisClusterNode, bool) {
	slaveOfSlaveList := make(map[string][]*rapi.RedisClusterNode)

	for i, nodeA := range cluster.Status.Cluster.Nodes {
		if nodeA.Role != rapi.RedisClusterNodeRoleSlave {
			continue
		}
		if nodeA.MasterRef != "" {
			isSlave := false
			for _, nodeB := range cluster.Status.Cluster.Nodes {
				if nodeB.ID == nodeA.MasterRef && nodeB.MasterRef != "" {
					isSlave = true
					break
				}
			}
			if isSlave {
				slaveOfSlaveList[nodeA.MasterRef] = append(slaveOfSlaveList[nodeA.MasterRef], &cluster.Status.Cluster.Nodes[i])
			}
		}

	}
	return slaveOfSlaveList, len(slaveOfSlaveList) == 0
}
