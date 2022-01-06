package controller

import (
	"context"
	"fmt"
	"time"

	podctrl "github.com/IBM/operator-for-redis-cluster/pkg/controller/pod"
	"github.com/IBM/operator-for-redis-cluster/pkg/utils"

	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/apimachinery/pkg/util/errors"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller/clustering"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller/sanitycheck"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

func (c *Controller) clusterAction(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, infos *redis.ClusterInfos) (ctrl.Result, error) {
	var err error
	result := ctrl.Result{}
	// run sanity check if needed
	needSanity, err := sanitycheck.RunSanityChecks(ctx, admin, &c.config.redis, c.podControl, cluster, infos, true)
	if err != nil {
		glog.Errorf("[clusterAction] cluster %s/%s, an error occurs during sanity check: %v ", cluster.Namespace, cluster.Name, err)
		return result, err
	}
	if needSanity {
		glog.V(3).Infof("[clusterAction] run sanity check cluster: %s/%s", cluster.Namespace, cluster.Name)
		result.Requeue, err = sanitycheck.RunSanityChecks(ctx, admin, &c.config.redis, c.podControl, cluster, infos, false)
		return result, err
	}

	// reconfigure the cluster if needed
	if result, err = c.applyConfiguration(ctx, admin, cluster); err != nil {
		glog.Errorf("[clusterAction] RedisCluster %s/%s, error: %v ", cluster.Namespace, cluster.Name, err)
		return result, err
	}

	glog.V(6).Infof("[clusterAction] cluster change for RedisCluster %s/%s: %v", cluster.Namespace, cluster.Name, result.Requeue)
	return result, nil
}

// applyConfiguration apply new configuration if needed:
// - add or delete pods
// - configure the redis-server process
func (c *Controller) applyConfiguration(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster) (ctrl.Result, error) {
	glog.V(6).Info("applyConfiguration START")
	defer glog.V(6).Info("applyConfiguration STOP")
	result := ctrl.Result{}
	newCluster, nodes, err := newRedisCluster(ctx, admin, cluster, c.client)
	if err != nil {
		glog.Errorf("unable to create the RedisCluster view: %v", err)
		return result, err
	}
	if c.needsMorePods(cluster) {
		glog.Info("applyConfiguration needMorePods")
		if _, err = c.createPod(ctx, cluster); err != nil {
			return result, err
		}
		result.RequeueAfter = requeueDelay
		return result, nil
	}
	if c.needsRollingUpdate(cluster) {
		glog.Info("applyConfiguration needRollingUpdate")
		pods, err := c.createMigrationPods(ctx, cluster)
		if err != nil {
			return result, err
		}
		if len(pods) > 0 {
			result.RequeueAfter = requeueDelay
			return result, nil
		}
		oldNodes, newNodes, err := getNodesWithNewHash(cluster, nodes)
		if err != nil {
			return result, err
		}
		return result, c.manageRollingUpdate(ctx, admin, cluster, newCluster, oldNodes, newNodes)
	}
	if c.needsLessPods(cluster) {
		glog.Info("applyConfiguration needLessPods")
		result.Requeue, err = c.managePodScaleDown(ctx, admin, cluster, newCluster, nodes)
		return result, err
	}

	clusterStatus := &cluster.Status.Cluster
	if (clusterStatus.NumberOfPods - clusterStatus.NumberOfRedisNodesRunning) != 0 {
		glog.V(3).Infof("Not all redis nodes are running, numberOfPods: %d, numberOfRedisNodesRunning: %d", clusterStatus.NumberOfPods, clusterStatus.NumberOfRedisNodesRunning)
		return result, err
	}

	result.Requeue, err = scalingOperations(ctx, admin, cluster, newCluster, nodes)
	if err != nil {
		return result, err
	}

	glog.V(4).Infof("new nodes status: \n %v", nodes)

	cluster.Status.Cluster.Status = rapi.ClusterStatusOK
	// wait a bit for the cluster to propagate configuration to reduce warning logs because of temporary inconsistency
	time.Sleep(1 * time.Second)
	return result, nil
}

func (c *Controller) createMigrationPods(ctx context.Context, cluster *rapi.RedisCluster) ([]*v1.Pod, error) {
	nbPodsToCreate := utils.GetNbPodsToCreate(cluster)
	pods, err := c.createPods(ctx, cluster, nbPodsToCreate)
	if err != nil {
		return pods, err
	}
	return pods, nil
}

// manageRollingUpdate used to manage properly a cluster rolling update if the pod template spec has changed
func (c *Controller) manageRollingUpdate(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, oldNodes, newNodes redis.Nodes) error {
	oldPrimaries, oldReplicas, _ := clustering.ClassifyNodesByRole(oldNodes)
	newPrimaries, newReplicas, newPrimariesNoSlots := clustering.ClassifyNodesByRole(newNodes)
	selectedPrimaries, selectedNewPrimaries, err := clustering.SelectPrimariesToReplace(oldPrimaries, newPrimaries, newPrimariesNoSlots, *cluster.Spec.NumberOfPrimaries, 1)
	if err != nil {
		glog.Errorf("error while selecting primaries to replace: %v", err)
	}
	currentReplicas := append(oldReplicas, newReplicas...)
	candidateReplicas := newPrimariesNoSlots.FilterByFunc(func(n *redis.Node) bool {
		for _, newPrimary := range selectedNewPrimaries {
			if n.ID == newPrimary.ID {
				return false
			}
		}
		return true
	})

	primaryToReplicas, unusedReplicas := clustering.GeneratePrimaryToReplicas(selectedPrimaries, currentReplicas, *cluster.Spec.ReplicationFactor)
	candidateReplicas = addReplicasToPrimaries(primaryToReplicas, selectedNewPrimaries, candidateReplicas, *cluster.Spec.ReplicationFactor)
	if err = clustering.PlaceReplicas(rCluster, primaryToReplicas, clustering.RemoveOldReplicas(currentReplicas, candidateReplicas), unusedReplicas, *cluster.Spec.ReplicationFactor); err != nil {
		glog.Errorf("unable to place replicas: %v", err)
		return err
	}
	cluster.Status.Cluster.NodesPlacement = rapi.NodesPlacementInfoOptimal

	if err = clustering.AttachReplicasToPrimary(ctx, rCluster, admin, primaryToReplicas); err != nil {
		glog.Errorf("unable to attach replicas to primary: %v", err)
		return err
	}

	currentPrimaries := append(oldPrimaries, newPrimaries...)
	allPrimaries := append(currentPrimaries, selectedNewPrimaries...)
	removedPrimaries, removedReplicas := getOldNodesToRemove(currentPrimaries, selectedPrimaries, append(oldNodes, newNodes...))

	// now we can move slot from old primary to new primary
	if err = clustering.DispatchSlotsToNewPrimaries(ctx, admin, cluster, rCluster, selectedPrimaries, currentPrimaries, allPrimaries, false); err != nil {
		glog.Errorf("unable to dispatch slot on new primary: %v", err)
	}

	for _, node := range append(removedPrimaries, removedReplicas...) {
		if err = c.detachForgetDeleteNode(ctx, admin, cluster, node); err != nil {
			glog.Errorf("unable to detach, forget, and delete node %s: %v", node.ID, err)
		}
	}

	return nil
}

// managePodScaleDown used to manage the scale down of a cluster
func (c *Controller) managePodScaleDown(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, newCluster *redis.Cluster, nodes redis.Nodes) (bool, error) {
	glog.V(6).Info("managePodScaleDown START")
	defer glog.V(6).Info("managePodScaleDown STOP")
	if nodesToDelete, ok := shouldDeleteNodes(cluster, newCluster); ok {
		return false, c.deletePods(cluster, nodesToDelete)
	}

	if replicasOfReplica, ok := checkReplicasOfReplica(cluster); !ok {
		glog.V(6).Info("checkReplicasOfReplica NOT OK")
		c.removeReplicasOfReplica(ctx, admin, cluster, nodes, replicasOfReplica)
		return true, nil
	}

	if nbPrimaryToDelete, ok := checkNumberOfPrimaries(cluster); !ok {
		glog.V(6).Info("checkNumberOfPrimaries NOT OK")
		if err := c.scaleDownPrimaries(ctx, admin, cluster, newCluster, nodes, nbPrimaryToDelete); err != nil {
			return false, err
		}
	}

	if primaryToReplicas, ok := checkReplicationFactor(cluster, newCluster); !ok {
		glog.V(6).Info("checkReplicationFactor NOT OK")
		if err := c.reconcileReplicationFactor(ctx, admin, cluster, newCluster, nodes, primaryToReplicas); err != nil {
			return false, err
		}
	}

	return false, nil
}

func (c *Controller) needsRollingUpdate(cluster *rapi.RedisCluster) bool {
	needsUpdate := needRollingUpdate(cluster)
	if setRollingUpdateCondition(&cluster.Status, needsUpdate) {
		cluster.Status.Cluster.Status = rapi.ClusterStatusRollingUpdate
	}
	return needsUpdate
}

func (c *Controller) needsMorePods(cluster *rapi.RedisCluster) bool {
	needsMorePods := needMorePods(cluster)
	if setScalingCondition(&cluster.Status, needsMorePods) {
		cluster.Status.Cluster.Status = rapi.ClusterStatusScaling
	}
	return needsMorePods
}

func (c *Controller) needsLessPods(cluster *rapi.RedisCluster) bool {
	needsLessPods := needLessPods(cluster)
	if setRebalancingCondition(&cluster.Status, needsLessPods) {
		cluster.Status.Cluster.Status = rapi.ClusterStatusRebalancing
	}
	return needsLessPods
}

func (c *Controller) removeReplicasOfReplica(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, nodes redis.Nodes, replicasOfReplica map[string][]*rapi.RedisClusterNode) {
	// Currently, the algorithm assumes redis is able to attach a replica to another replica.
	// In practice, we detach the replicas and the controller reassigns the replica to a primary if needed.
	for _, replicas := range replicasOfReplica {
		for _, replica := range replicas {
			node, err := nodes.GetNodeByID(replica.ID)
			if err != nil {
				glog.Errorf("unable to find node with ID %s: %v", replica.ID, err)
			} else if err = admin.DetachReplica(ctx, node); err != nil {
				glog.Errorf("unable to detach replica with ID %s: %v", node.ID, err)
			} else if err = c.podControl.DeletePod(cluster, replica.PodName); err != nil {
				glog.Errorf("unable to delete pod %s corresponding to replica with the ID %s: %v", replica.PodName, node.ID, err)
			}
		}
	}
}

func (c *Controller) scaleDownPrimaries(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, nodes redis.Nodes, nbPrimaryToDelete int32) error {
	newNumberOfPrimaries := cluster.Status.Cluster.NumberOfPrimaries

	// Decrease the number of primaries by one in order to limit the impact on the client
	if nbPrimaryToDelete > 0 {
		newNumberOfPrimaries--
	}
	currentPrimaries, candidatePrimaries, allPrimaries := getPrimaries(nodes)

	// Create the new primaries
	newPrimaries, err := clustering.SelectPrimaries(rCluster, currentPrimaries, candidatePrimaries, newNumberOfPrimaries)
	if err != nil {
		glog.Errorf("error while dispatching slots to primaries: %v", err)
		cluster.Status.Cluster.Status = rapi.ClusterStatusKO
		rCluster.Status = rapi.ClusterStatusKO
		return err
	}

	// Dispatch slots to the new primaries
	if err = clustering.DispatchSlotsToNewPrimaries(ctx, admin, cluster, rCluster, newPrimaries, currentPrimaries, allPrimaries, true); err != nil {
		glog.Errorf("unable to dispatch slot to new primary: %v", err)
		return err
	}

	// Get old primaries and replicas to be removed
	removedPrimaries, removedReplicas := getOldNodesToRemove(currentPrimaries, newPrimaries, nodes)

	// Detach and forget nodes to be removed
	if _, err = detachAndForgetNodes(ctx, admin, removedPrimaries, removedReplicas); err != nil {
		glog.Errorf("unable to detach and forget old primaries: %v", err)
		return err
	}

	// Delete nodes
	nodesToDelete := append(removedPrimaries, removedReplicas...)
	var errs []error
	for _, node := range nodesToDelete {
		if node.Pod != nil {
			if err = c.podControl.DeletePod(cluster, node.Pod.Name); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.NewAggregate(errs)
}

func (c *Controller) reconcileReplicationFactor(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, nodes redis.Nodes, primaryToReplicas map[string]redis.Nodes) error {
	var errs []error
	var currentReplicas redis.Nodes
	for _, replicas := range primaryToReplicas {
		currentReplicas = append(currentReplicas, replicas...)
	}
	for primaryID, replicas := range primaryToReplicas {
		diff := int32(len(replicas)) - *cluster.Spec.ReplicationFactor
		if diff < 0 {
			// not enough replicas on this primary
			// get the primary node
			primary, err := nodes.GetNodeByID(primaryID)
			if err != nil {
				return err
			}
			if err := c.attachReplicasToPrimary(ctx, admin, rCluster.GetZones(), primary, nodes, -diff); err != nil {
				errs = append(errs, err)
			}
		}
		if diff > 0 {
			// too many replicas on this primary
			if err := c.removeReplicasFromPrimary(ctx, admin, cluster, rCluster, primaryID, replicas, currentReplicas, diff); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.NewAggregate(errs)
}

func (c *Controller) removeReplicasFromPrimary(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, primaryID string, replicas redis.Nodes, allReplicas redis.Nodes, diff int32) error {
	var errs []error
	// select a replica to be removed
	nodesToDelete, err := selectReplicasToDelete(rCluster, primaryID, replicas, allReplicas, diff)
	if err != nil {
		return err
	}
	for _, node := range nodesToDelete {
		err := admin.DetachReplica(ctx, node)
		if err != nil {
			errs = append(errs, err)
		}

		if node.Pod != nil {
			if err := c.podControl.DeletePod(cluster, node.Pod.Name); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.NewAggregate(errs)
}

func (c *Controller) attachReplicasToPrimary(ctx context.Context, admin redis.AdminInterface, zones []string, primary *redis.Node, nodes redis.Nodes, diff int32) error {
	var errs []error
	// find an available redis nodes to be the new replicas
	replicas, err := selectNewReplicasByZone(zones, primary, nodes, diff)
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		if err = admin.AttachReplicaToPrimary(ctx, replica, primary); err != nil {
			glog.Errorf("error during AttachReplicaToPrimary")
			errs = append(errs, err)
		}
	}
	return errors.NewAggregate(errs)
}

func (c *Controller) detachForgetDeleteNode(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, node *redis.Node) error {
	if redis.IsReplica(node) {
		if err := admin.DetachReplica(ctx, node); err != nil {
			glog.Errorf("unable to detach the replica with ID %s: %v", node.ID, err)
		}
	}
	if err := admin.ForgetNode(ctx, node.ID); err != nil {
		glog.Errorf("unable to forget the node with ID %s: %v", node.ID, err)
	}
	if err := c.podControl.DeletePod(cluster, node.Pod.Name); err != nil {
		glog.Errorf("unable to delete the pod %s/%s, err:%v", node.Pod.Name, node.Pod.Namespace, err)
		return err
	}
	return nil
}

func (c *Controller) createPod(ctx context.Context, cluster *rapi.RedisCluster) (*v1.Pod, error) {
	kubeNodes, err := utils.GetKubeNodes(ctx, c.client, cluster.Spec.PodTemplate.Spec.NodeSelector)
	if err != nil {
		glog.Errorf("error getting k8s nodes with label selector %q: %v", cluster.Spec.PodTemplate.Spec.NodeSelector, err)
		return nil, err
	}
	ok, err := checkNodeResources(ctx, c.mgr, cluster, kubeNodes)
	if err != nil {
		return nil, err
	}
	if !ok {
		glog.Warningf("Insufficient resources to schedule a new pod on nodes in this cluster. Please allocate more resources for existing nodes or create additional nodes.")
		c.recorder.Event(cluster, v1.EventTypeWarning, "InsufficientResources", "Insufficient resources to schedule pod")
	}
	pod, err := c.podControl.CreatePod(cluster)
	if err != nil {
		glog.Errorf("unable to create pod for RedisCluster %s/%s, err: %v", cluster.Namespace, cluster.Name, err)
		return nil, err
	}
	return pod, nil
}

func (c *Controller) createPods(ctx context.Context, cluster *rapi.RedisCluster, nbPodToCreate int32) ([]*v1.Pod, error) {
	var pods []*v1.Pod
	for i := int32(0); i < nbPodToCreate; i++ {
		pod, err := c.createPod(ctx, cluster)
		if err != nil {
			return pods, err
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

func (c *Controller) deletePods(cluster *rapi.RedisCluster, nodes []*rapi.RedisClusterNode) error {
	var errs []error
	for _, node := range nodes {
		if err := c.podControl.DeletePod(cluster, node.PodName); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.NewAggregate(errs)
}

func newRedisCluster(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, kubeClient client.Client) (*redis.Cluster, redis.Nodes, error) {
	infos, err := admin.GetClusterInfos(ctx)
	if redis.IsPartialError(err) {
		glog.Errorf("error getting consolidated view of the cluster: %v", err)
		return nil, nil, err
	}

	nodes := infos.GetNodes()
	rCluster := redis.NewCluster(cluster.Name, cluster.Namespace)

	if cluster.Spec.PodTemplate != nil {
		rCluster.NodeSelector = cluster.Spec.PodTemplate.Spec.NodeSelector
	}

	kubeNodes, err := utils.GetKubeNodes(ctx, kubeClient, rCluster.NodeSelector)
	if err != nil {
		glog.Errorf("error getting k8s nodes with label selector %q: %v", rCluster.NodeSelector, err)
		return nil, nil, err
	}

	rCluster.KubeNodes = kubeNodes
	for _, node := range nodes {
		rCluster.Nodes[node.ID] = node
	}

	for _, node := range cluster.Status.Cluster.Nodes {
		if rNode, ok := rCluster.Nodes[node.ID]; ok {
			rNode.Pod = node.Pod
			rNode.Zone = node.Zone
		}
	}

	return rCluster, nodes, nil
}

func scalingOperations(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, nodes redis.Nodes) (bool, error) {
	nbPrimaries := *cluster.Spec.NumberOfPrimaries
	currentPrimaries, candidatePrimaries, allPrimaries := getPrimaries(nodes)
	replicas := nodes.FilterByFunc(redis.IsReplica)

	// if we do not have enough primaries, promote replicas to primaries
	if len(currentPrimaries)+len(candidatePrimaries) < int(nbPrimaries) {
		promotedPrimaries := promoteReplicasToPrimaries(ctx, admin, rCluster, replicas, candidatePrimaries, int(nbPrimaries)-len(currentPrimaries)-len(candidatePrimaries))
		candidatePrimaries = append(candidatePrimaries, promotedPrimaries...)
	}

	// first, we select the new primaries
	newPrimaries, err := clustering.SelectPrimaries(rCluster, currentPrimaries, candidatePrimaries, nbPrimaries)
	if err != nil {
		glog.Errorf("cannot dispatch slots to primaries: %v", err)
		rCluster.Status = rapi.ClusterStatusKO
		return false, err
	}

	// second, get the current and new replica nodes
	currentReplicas, newReplicas := getReplicas(newPrimaries, nodes)

	// depending on whether we scale up or down, we will dispatch replicas before/after the dispatch of slots
	if int(nbPrimaries) < len(currentPrimaries) {
		// this happens after a scale down of the cluster
		// we should dispatch slots before dispatching replicas
		if err = scaleDown(ctx, admin, cluster, rCluster, currentPrimaries, newPrimaries, allPrimaries, currentReplicas, newReplicas); err != nil {
			return false, err
		}
	} else {
		// scaling up the number of primaries or the number of primaries hasn't changed
		// assign primary/replica roles
		if err = scaleUp(ctx, admin, cluster, rCluster, currentPrimaries, newPrimaries, allPrimaries, currentReplicas, newReplicas); err != nil {
			return false, err
		}
	}
	if len(currentPrimaries) != len(newPrimaries) {
		return true, nil
	}
	return false, nil
}

func addReplicasToPrimaries(primaryToReplicas map[string]redis.Nodes, primaries, replicas redis.Nodes, replicationFactor int32) redis.Nodes {
	// attach replicas to primaries with no slots
	for _, primary := range primaries {
		i := 0
		for _, replica := range replicas {
			primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], replica)
			i++
			if len(primaryToReplicas[primary.ID]) >= int(replicationFactor) {
				break
			}
		}
		replicas = replicas[i:]
	}
	return replicas
}

func promoteReplicasToPrimaries(ctx context.Context, admin redis.AdminInterface, rCluster *redis.Cluster, replicas, candidatePrimaries redis.Nodes, nbPrimariesToAdd int) redis.Nodes {
	newPrimaries := redis.Nodes{}
	zones := rCluster.GetZones()
	for _, replica := range replicas {
		if clustering.ZonesBalanced(zones, replica, candidatePrimaries) {
			glog.V(4).Infof("promoting replica %s to primary", replica.ID)
			if err := admin.DetachReplica(ctx, replica); err != nil {
				glog.Errorf("unable to detach replica with ID %s: %v", replica.ID, err)
			}
			newPrimaries = append(newPrimaries, replica)
			candidatePrimaries = append(candidatePrimaries, replica)
			if len(newPrimaries) >= nbPrimariesToAdd {
				break
			}
		}
	}
	return newPrimaries
}

func scaleUp(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, currentPrimaries, newPrimaries, allPrimaries, currentReplicas, newReplicas redis.Nodes) error {
	if err := placeAndAttachReplicas(ctx, admin, cluster, rCluster, currentReplicas, newPrimaries, newReplicas); err != nil {
		return err
	}
	if err := clustering.DispatchSlotsToNewPrimaries(ctx, admin, cluster, rCluster, newPrimaries, currentPrimaries, allPrimaries, true); err != nil {
		glog.Errorf("unable to dispatch slot on new primary: %v", err)
		return err
	}
	return nil
}

func scaleDown(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, currentPrimaries, newPrimaries, allPrimaries, currentReplicas, newReplicas redis.Nodes) error {
	if err := clustering.DispatchSlotsToNewPrimaries(ctx, admin, cluster, rCluster, newPrimaries, currentPrimaries, allPrimaries, true); err != nil {
		glog.Errorf("unable to dispatch slot on new primary: %v", err)
		return err
	}
	if err := placeAndAttachReplicas(ctx, admin, cluster, rCluster, currentReplicas, newPrimaries, newReplicas); err != nil {
		return err
	}
	return nil
}

func getNodesWithNewHash(cluster *rapi.RedisCluster, nodes redis.Nodes) (redis.Nodes, redis.Nodes, error) {
	clusterPodSpecHash, err := podctrl.GenerateMD5Spec(&cluster.Spec.PodTemplate.Spec)
	if err != nil {
		return redis.Nodes{}, redis.Nodes{}, err
	}
	// nodes with new pod spec version
	newNodes := nodes.FilterByFunc(func(n *redis.Node) bool {
		if n.Pod == nil {
			return false
		}
		return comparePodSpecMD5Hash(clusterPodSpecHash, n.Pod)
	})
	// nodes with old pod spec version
	oldNodes := nodes.FilterByFunc(func(n *redis.Node) bool {
		if n.Pod == nil {
			return false
		}
		return !comparePodSpecMD5Hash(clusterPodSpecHash, n.Pod)
	})

	return oldNodes, newNodes, nil
}

func getPrimaries(nodes redis.Nodes) (redis.Nodes, redis.Nodes, redis.Nodes) {
	var allPrimaries redis.Nodes
	// Get primaries with slots assigned
	currentPrimaries := nodes.FilterByFunc(redis.IsPrimaryWithSlot)
	allPrimaries = append(allPrimaries, currentPrimaries...)

	// Add available primaries without slots
	candidatePrimaries := nodes.FilterByFunc(redis.IsPrimaryWithNoSlot)
	allPrimaries = append(allPrimaries, candidatePrimaries...)
	glog.V(2).Info("current number of primaries with no slots: ", len(candidatePrimaries))

	return currentPrimaries, candidatePrimaries, allPrimaries
}

func getReplicas(primaries, nodes redis.Nodes) (redis.Nodes, redis.Nodes) {
	currentReplicas := nodes.FilterByFunc(redis.IsReplica)
	newReplicas := nodes.FilterByFunc(func(nodeA *redis.Node) bool {
		for _, nodeB := range primaries {
			if nodeA.ID == nodeB.ID {
				return false
			}
		}
		for _, nodeB := range currentReplicas {
			if nodeA.ID == nodeB.ID {
				return false
			}
		}
		return true
	})

	return currentReplicas, newReplicas
}

func getOldNodesToRemove(currPrimaries, newPrimaries, nodes redis.Nodes) (redis.Nodes, redis.Nodes) {
	removedPrimaries := redis.Nodes{}
	for _, node := range currPrimaries {
		if _, err := newPrimaries.GetNodeByID(node.ID); err != nil {
			removedPrimaries = append(removedPrimaries, node)
		}
	}

	removedReplicas := redis.Nodes{}
	for _, primary := range removedPrimaries {
		replicas := nodes.FilterByFunc(func(node *redis.Node) bool {
			return redis.IsReplica(node) && (node.PrimaryReferent == primary.ID)
		})
		removedReplicas = append(removedReplicas, replicas...)
	}
	return removedPrimaries, removedReplicas
}

func updateConfig(ctx context.Context, admin redis.AdminInterface, config map[string]string) {
	var errs []error
	for field, val := range config {
		glog.V(6).Infof("updating config option from %s to %s", field, val)
		for addr := range admin.Connections().GetAll() {
			if err := admin.SetConfig(ctx, addr, []string{field, val}); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		glog.Errorf("unable to update config: %v", errors.NewAggregate(errs))
	}
}

func detachAndForgetNodes(ctx context.Context, admin redis.AdminInterface, primaries, replicas redis.Nodes) (redis.Nodes, error) {
	detachReplicas(ctx, admin, replicas)
	removedNodes := append(primaries, replicas...)
	for _, node := range removedNodes {
		if err := admin.ForgetNode(ctx, node.ID); err != nil {
			glog.Errorf("unable to forget the node with ID %s: %v", node.ID, err)
		}
	}
	return removedNodes, nil
}

func detachReplicas(ctx context.Context, admin redis.AdminInterface, nodes redis.Nodes) {
	for _, node := range nodes {
		if err := admin.DetachReplica(ctx, node); err != nil {
			glog.Errorf("unable to detach the replica with ID %s: %v", node.ID, err)
		}
	}
}

func placeAndAttachReplicas(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, currentReplicas, newPrimaries, newReplicas redis.Nodes) error {
	primaryToReplicas, unusedReplicas := clustering.GeneratePrimaryToReplicas(newPrimaries, currentReplicas, *cluster.Spec.ReplicationFactor)
	detachReplicas(ctx, admin, unusedReplicas)
	err := clustering.PlaceReplicas(rCluster, primaryToReplicas, clustering.RemoveOldReplicas(currentReplicas, newReplicas), unusedReplicas, *cluster.Spec.ReplicationFactor)
	if err != nil {
		glog.Errorf("unable to place replicas: %v", err)
		return err
	}
	cluster.Status.Cluster.NodesPlacement = rapi.NodesPlacementInfoOptimal
	if err := clustering.AttachReplicasToPrimary(ctx, rCluster, admin, primaryToReplicas); err != nil {
		glog.Errorf("unable to dispatch replica on new primary: %v", err)
		return err
	}
	return nil
}

func selectReplicasToDelete(rCluster *redis.Cluster, primaryID string, primaryReplicas redis.Nodes, allReplicas redis.Nodes, nbReplicasToDelete int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	primaryNode, err := rCluster.GetNodeByID(primaryID)
	if err != nil {
		return selection, err
	}
	for len(selection) < int(nbReplicasToDelete) {
		if removeReplicasByZone(rCluster.GetZones(), &selection, primaryNode, primaryReplicas, allReplicas, nbReplicasToDelete) {
			return selection, nil
		}
	}
	return selection, nil
}

func selectNewReplicasByZone(zones []string, primary *redis.Node, nodes redis.Nodes, nbReplicasNeeded int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	primaryReplicas := redis.Nodes{}
	candidateReplicas := redis.Nodes{}

	if nbReplicasNeeded <= 0 {
		return selection, nil
	}

	for _, node := range nodes {
		if redis.IsReplica(node) && node.PrimaryReferent == primary.ID {
			primaryReplicas = append(primaryReplicas, node)
		}
		if redis.IsPrimaryWithNoSlot(node) {
			candidateReplicas = append(candidateReplicas, node)
		}
	}
	zoneToReplicas := clustering.ZoneToNodes(zones, candidateReplicas)
	zoneIndex := clustering.GetZoneIndex(zones, primary.Zone, primaryReplicas)
	numEmptyZones := 0
	// iterate while zones are non-empty and the number of replicas is less than RF
	for numEmptyZones < len(zones) && len(selection) < int(nbReplicasNeeded) {
		zone := zones[zoneIndex]
		zoneReplicas := zoneToReplicas[zone]
		if len(zoneReplicas) > 0 {
			// append replica to primary and remove from map
			selection = append(selection, zoneReplicas[0])
			zoneToReplicas[zone] = zoneReplicas[1:]
		} else {
			numEmptyZones++
		}
		zoneIndex = (zoneIndex + 1) % len(zones)
	}
	if len(selection) < int(nbReplicasNeeded) {
		return selection, fmt.Errorf("insufficient number of replicas for primary %s, expected: %v, actual: %v", primary.ID, int(nbReplicasNeeded), len(selection))
	}
	return selection, nil
}

func removeReplicaFromZone(replica *redis.Node, zoneToReplicas map[string]redis.Nodes) {
	for zone, replicas := range zoneToReplicas {
		for i, s := range replicas {
			if replica == s {
				zoneToReplicas[zone][i] = zoneToReplicas[zone][len(zoneToReplicas[zone])-1]
				zoneToReplicas[zone] = zoneToReplicas[zone][:len(zoneToReplicas[zone])-1]
				return
			}
		}
	}
}

func replicaInLargestZone(selection *redis.Nodes, replicas redis.Nodes, zoneToReplicas map[string]redis.Nodes, nbReplicasToDelete int32) bool {
	largestZone := largestZoneSize(zoneToReplicas)
	for _, replica := range replicas {
		// check if this replica is in the largest zone and there are no other replica zones of equal size
		if len(zoneToReplicas[replica.Zone]) == largestZone && isLargestZone(zoneToReplicas, largestZone) {
			// add it to the selection of replicas to delete and remove it from zoneToReplicas
			*selection = append(*selection, replica)
			removeReplicaFromZone(replica, zoneToReplicas)
			if len(*selection) == int(nbReplicasToDelete) {
				return true
			}
		}
	}
	return false
}

func removeReplicasByZone(zones []string, selection *redis.Nodes, primary *redis.Node, replicas redis.Nodes, allReplicas redis.Nodes, nbReplicasToDelete int32) bool {
	zoneToReplicas := clustering.ZoneToNodes(zones, replicas)
	// remove replicas in the largest zone
	if replicaInLargestZone(selection, replicas, zoneToReplicas, nbReplicasToDelete) {
		return true
	}
	// replicas are in zones of equal size, remove replicas in the same zone as their primary
	if replica := sameZoneAsPrimary(primary, replicas); replica != nil {
		*selection = append(*selection, replica)
		removeReplicaFromZone(replica, zoneToReplicas)
		if len(*selection) == int(nbReplicasToDelete) {
			return true
		}
	}
	// remove selected replicas
	i := 0
	for _, replica := range allReplicas {
		if !containsReplica(selection, replica) {
			allReplicas[i] = replica
			i++
		}
	}
	allReplicas = allReplicas[:i]
	// remove replicas in the largest zone cluster-wide
	zoneToAllReplicas := clustering.ZoneToNodes(zones, allReplicas)
	if replicaInLargestZone(selection, replicas, zoneToAllReplicas, nbReplicasToDelete) {
		return true
	}
	// if we still do not have enough replicas, pick the first available
	for _, replica := range replicas {
		if !containsReplica(selection, replica) {
			*selection = append(*selection, replica)
			removeReplicaFromZone(replica, zoneToReplicas)
			if len(*selection) == int(nbReplicasToDelete) {
				return true
			}
		}
	}
	return false
}

func isLargestZone(zoneToNodes map[string]redis.Nodes, largestZone int) bool {
	count := 0
	for _, nodes := range zoneToNodes {
		if len(nodes) == largestZone {
			count++
		}
	}
	return count == 1
}

func largestZoneSize(zoneToNodes map[string]redis.Nodes) int {
	largest := 0
	for _, nodes := range zoneToNodes {
		if len(nodes) > largest {
			largest = len(nodes)
		}
	}
	return largest
}

func sameZoneAsPrimary(primary *redis.Node, replicas redis.Nodes) *redis.Node {
	for _, replica := range replicas {
		if primary.Zone == replica.Zone {
			return replica
		}
	}
	return nil
}

func containsReplica(nodes *redis.Nodes, replica *redis.Node) bool {
	for _, s := range *nodes {
		if replica == s {
			return true
		}
	}
	return false
}
