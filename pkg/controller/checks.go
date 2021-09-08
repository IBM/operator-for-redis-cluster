package controller

import (
	"context"
	"math"
	"reflect"
	"strconv"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	kapi "k8s.io/api/core/v1"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	podctrl "github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/pod"
)

type resources struct {
	cpu    resource.Quantity
	memory resource.Quantity
}

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

// checkReplicationFactor checks the primary replication factor
// It returns a map with the primary IDs as the key and a list of replicas as the value
// The second returned value is a boolean. True if replicationFactor is correct for each primary, false otherwise
func checkReplicationFactor(cluster *rapi.RedisCluster, rCluster *redis.Cluster) (map[string]redis.Nodes, bool) {
	primaryToReplicas := generatePrimaryToReplicas(cluster, rCluster)
	if (cluster.Status.Cluster.MaxReplicationFactor != cluster.Status.Cluster.MinReplicationFactor) || (*cluster.Spec.ReplicationFactor != cluster.Status.Cluster.MaxReplicationFactor) {
		return primaryToReplicas, false
	}
	return primaryToReplicas, true
}

// checkNumberOfPrimaries returns the difference between the number of primaries currently existing and the number of desired primaries
// Returns true if the number of primary status is equal to the spec
func checkNumberOfPrimaries(cluster *rapi.RedisCluster) (int32, bool) {
	nbPrimarySpec := *cluster.Spec.NumberOfPrimaries
	nbPrimaryStatus := cluster.Status.Cluster.NumberOfPrimaries
	same := (nbPrimaryStatus) == nbPrimarySpec
	return nbPrimaryStatus - nbPrimarySpec, same
}

// checkNodeResources checks if there are resources to schedule a new pod
// Calculates the allocated memory/cpu across all k8s nodes
// Returns true if there are sufficient resources, false otherwise
func checkNodeResources(ctx context.Context, mgr manager.Manager, cluster *rapi.RedisCluster, nodes []kapi.Node) (bool, error) {
	var podLimits resources
	// sum cpu and memory across all containers in the pod spec
	for _, container := range cluster.Spec.PodTemplate.Spec.Containers {
		podLimits.cpu.Add(*container.Resources.Limits.Cpu())
		podLimits.memory.Add(*container.Resources.Limits.Memory())
	}
	// create a direct client to the k8s API
	directClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme(), Mapper: mgr.GetRESTMapper()})
	if err != nil {
		return false, err
	}
	// find the node with the least allocated resources across all k8s nodes in the cluster
	nodeAllocated, nodeCapacity := getNodeMinResources(ctx, directClient, nodes)
	glog.V(6).Infof("allocated node memory: %v, memory capacity: %v, allocated node cpu: %v, cpu capacity: %v", nodeAllocated.memory.String(), nodeCapacity.memory.String(), nodeAllocated.cpu.String(), nodeCapacity.cpu.String())
	glog.V(6).Infof("required pod memory: %v, required pod cpu: %v", podLimits.memory.String(), podLimits.cpu.String())
	// add the pod resources to the allocated node resources
	nodeAllocated.cpu.Add(podLimits.cpu)
	nodeAllocated.memory.Add(podLimits.memory)
	// return false if the resources are greater than the node's capacity
	if nodeAllocated.cpu.Cmp(nodeCapacity.cpu) == 1 {
		return false, nil
	}
	if nodeAllocated.memory.Cmp(nodeCapacity.memory) == 1 {
		return false, nil
	}
	return true, nil
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

func getNodeMinResources(ctx context.Context, directClient client.Client, nodes []kapi.Node) (resources, resources) {
	var nodeMinAllocated, nodeMinCapacity resources
	nodeMinAllocated.cpu = resource.MustParse(strconv.Itoa(math.MaxInt32))
	nodeMinAllocated.memory = resource.MustParse(strconv.Itoa(math.MaxInt32) + "Ei")
	selector, _ := fields.ParseSelector("status.phase!=" + string(kapi.PodSucceeded) + ",status.phase!=" + string(kapi.PodFailed))
	for _, node := range nodes {
		maxPods, _ := node.Status.Allocatable.Pods().AsInt64()
		opts := []client.ListOption{
			client.MatchingFieldsSelector{Selector: fields.AndSelectors(selector, fields.OneTermEqualSelector("spec.nodeName", node.Name))},
			client.Limit(maxPods),
		}
		pods, err := listPods(ctx, directClient, opts)
		if err != nil {
			glog.Errorf("unable to list pods for node %s: %v", node.Name, err)
		}
		var nodeLimitsCpu, nodeLimitsMemory resource.Quantity
		for _, pod := range pods {
			for _, container := range pod.Spec.Containers {
				nodeLimitsCpu.Add(*container.Resources.Limits.Cpu())
				nodeLimitsMemory.Add(*container.Resources.Limits.Memory())
			}
		}
		// update resources if this node's allocated memory limit is less than the minimum
		if nodeLimitsMemory.Cmp(nodeMinAllocated.memory) == -1 {
			nodeMinAllocated.cpu = nodeLimitsCpu
			nodeMinAllocated.memory = nodeLimitsMemory
			nodeMinCapacity.cpu = *node.Status.Capacity.Cpu()
			nodeMinCapacity.memory = *node.Status.Capacity.Memory()
		}
	}
	return nodeMinAllocated, nodeMinCapacity
}

// shouldDeleteNodes detects if some nodes can be removed without impacting the cluster
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
