package controller

import (
	"context"
	"math"
	"reflect"
	"strconv"

	"gopkg.in/yaml.v3"

	"github.com/IBM/operator-for-redis-cluster/pkg/utils"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	kapi "k8s.io/api/core/v1"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	podctrl "github.com/IBM/operator-for-redis-cluster/pkg/controller/pod"
)

type resources struct {
	cpu    resource.Quantity
	memory resource.Quantity
}

func compareStatus(old, new *rapi.RedisClusterStatus) bool {
	if compareStringValue("ClusterStatus", string(old.Cluster.Status), string(new.Cluster.Status)) {
		return true
	}
	if compareInts("NumberOfPods", old.Cluster.NumberOfPods, new.Cluster.NumberOfPods) {
		return true
	}
	if compareInts("NumberOfPodsReady", old.Cluster.NumberOfPodsReady, new.Cluster.NumberOfPodsReady) {
		return true
	}
	if compareInts("NumberOfRedisNodesRunning", old.Cluster.NumberOfRedisNodesRunning, new.Cluster.NumberOfRedisNodesRunning) {
		return true
	}
	if compareInts("NumberOfPrimaries", old.Cluster.NumberOfPrimaries, new.Cluster.NumberOfPrimaries) {
		return true
	}
	if compareInts("MinReplicationFactor", old.Cluster.MinReplicationFactor, new.Cluster.MinReplicationFactor) {
		return true
	}
	if compareInts("MaxReplicationFactor", old.Cluster.MaxReplicationFactor, new.Cluster.MaxReplicationFactor) {
		return true
	}
	if compareStringValue("ClusterStatus", string(old.Cluster.Status), string(new.Cluster.Status)) {
		return true
	}
	if compareStringValue("NodesPlacement", string(old.Cluster.NodesPlacement), string(new.Cluster.NodesPlacement)) {
		return true
	}
	if compareInts("len(Nodes)", int32(len(old.Cluster.Nodes)), int32(len(new.Cluster.Nodes))) {
		return true
	}

	if len(old.Cluster.Nodes) != len(new.Cluster.Nodes) {
		return true
	}
	for _, nodeA := range old.Cluster.Nodes {
		found := false
		for _, nodeB := range new.Cluster.Nodes {
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

	if len(old.Conditions) != len(new.Conditions) {
		return true
	}
	for _, condA := range old.Conditions {
		found := false
		for _, condB := range new.Conditions {
			if condA.Type == condB.Type {
				found = true
				if compareConditions(&condA, &condB) {
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
		glog.Infof("compare Node.Slot size: %d - %d", sizeSlotsA, sizeSlotsB)
		return true
	}

	if (sizeSlotsA != 0) && !reflect.DeepEqual(nodeA.Slots, nodeB.Slots) {
		glog.Infof("compare Node.Slot deepEqual: %v - %v", nodeA.Slots, nodeB.Slots)
		return true
	}

	return false
}

func compareConditions(condA, condB *rapi.RedisClusterCondition) bool {
	if compareStringValue("Condition.Type", string(condA.Type), string(condB.Type)) {
		return true
	}
	if compareStringValue("Condition.Status", string(condA.Status), string(condB.Status)) {
		return true
	}
	if compareStringValue("Condition.Reason", condA.Reason, condB.Reason) {
		return true
	}
	if compareStringValue("Condition.Message", condA.Message, condB.Message) {
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

	if needConditionUpdate(cluster) {
		glog.V(6).Info("needClusterOperation---needConditionUpdate")
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

func needConditionUpdate(cluster *rapi.RedisCluster) bool {
	for _, cond := range cluster.Status.Conditions {
		if cond.Status == kapi.ConditionTrue {
			if cond.Type == rapi.RedisClusterRollingUpdate || cond.Type == rapi.RedisClusterRebalancing || cond.Type == rapi.RedisClusterScaling {
				return true
			}
		}
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

func compareConfig(oldConfig, newConfig map[string]string) map[string]string {
	configChanges := make(map[string]string)
	for field, newVal := range newConfig {
		if oldVal, ok := oldConfig[field]; ok {
			if field == "maxmemory" {
				oldVal, _ = utils.StringToByteString(oldVal)
				newVal, _ = utils.StringToByteString(newVal)
			}
			if oldVal != newVal {
				configChanges[field] = newVal
			}
		}
	}
	return configChanges
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

// checkNumberOfPrimaries returns the difference between the number of existing primaries and the number of desired primaries
// Returns true if the number of primary status is equal to the spec
func checkNumberOfPrimaries(cluster *rapi.RedisCluster) (int32, bool) {
	same := *cluster.Spec.NumberOfPrimaries == cluster.Status.Cluster.NumberOfPrimaries
	return cluster.Status.Cluster.NumberOfPrimaries - *cluster.Spec.NumberOfPrimaries, same
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

// checkZoneBalance checks if zones are balanced across the cluster
// Calculates the zone skew for primaries and replicas
// Returns true if zone balance <= 2, false otherwise
func checkZoneBalance(cluster *rapi.RedisCluster) bool {
	zoneToPrimaries, zoneToReplicas := utils.ZoneToRole(cluster.Status.Cluster.Nodes)
	_, _, ok := utils.GetZoneSkewByRole(zoneToPrimaries, zoneToReplicas)
	return ok
}

// checkServerConfig checks if the running redis server config matches
// the server config stored in the redis cluster config map
// Returns a map of the changed configuration, if any
func checkServerConfig(ctx context.Context, admin redis.AdminInterface, redisClusterConfigMap *kapi.ConfigMap) (map[string]string, error) {
	var values string
	if val, ok := redisClusterConfigMap.Data["redis.yaml"]; ok {
		values = val
	}
	clusterConfig := make(map[string]string)
	if err := yaml.Unmarshal([]byte(values), &clusterConfig); err != nil {
		return nil, err
	}
	serverConfig, err := admin.GetConfig(ctx, "*")
	if err != nil {
		return nil, err
	}
	return compareConfig(serverConfig, clusterConfig), nil
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
		pods, err := utils.ListPods(ctx, directClient, opts)
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
