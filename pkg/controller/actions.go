package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/clustering"
	podctrl "github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/pod"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/sanitycheck"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

func (c *Controller) clusterAction(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, infos *redis.ClusterInfos) (bool, error) {
	var err error
	// run sanity check if needed
	needSanity, err := sanitycheck.RunSanityChecks(ctx, admin, &c.config.redis, c.podControl, cluster, infos, true)
	if err != nil {
		glog.Errorf("[clusterAction] cluster %s/%s, an error occurs during sanitycheck: %v ", cluster.Namespace, cluster.Name, err)
		return false, err
	}
	if needSanity {
		glog.V(3).Infof("[clusterAction] run sanitycheck cluster: %s/%s", cluster.Namespace, cluster.Name)
		return sanitycheck.RunSanityChecks(ctx, admin, &c.config.redis, c.podControl, cluster, infos, false)
	}

	// Start more pods if needed
	if needMorePods(cluster) {
		if setScalingCondition(&cluster.Status, true) {
			if cluster, err = c.updateHandler(cluster); err != nil {
				return false, err
			}
		}
		pod, err2 := c.podControl.CreatePod(cluster)
		if err2 != nil {
			glog.Errorf("[clusterAction] unable to create a pod associated to the RedisCluster: %s/%s, err: %v", cluster.Namespace, cluster.Name, err2)
			return false, err2
		}

		glog.V(3).Infof("[clusterAction]create a Pod %s/%s", pod.Namespace, pod.Name)
		return true, nil
	}
	if setScalingCondition(&cluster.Status, false) {
		if cluster, err = c.updateHandler(cluster); err != nil {
			return false, err
		}
	}

	// Reconfigure the cluster if needed
	hasChanged, err := c.applyConfiguration(ctx, admin, cluster)
	if err != nil {
		glog.Errorf("[clusterAction] cluster %s/%s, an error occurs: %v ", cluster.Namespace, cluster.Name, err)
		return false, err
	}

	if hasChanged {
		glog.V(6).Infof("[clusterAction] cluster has changed cluster: %s/%s", cluster.Namespace, cluster.Name)
		return true, nil
	}

	glog.V(6).Infof("[clusterAction] cluster hasn't changed cluster: %s/%s", cluster.Namespace, cluster.Name)
	return false, nil
}

// manageRollingUpdate used to manage properly a cluster rolling update if the pod template spec has changed
func (c *Controller) manageRollingUpdate(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, nodes redis.Nodes) (bool, error) {
	nbRequiredPodForSpec := *cluster.Spec.NumberOfPrimaries * (1 + *cluster.Spec.ReplicationFactor)
	nbPodByNodeMigration := 1 + *cluster.Spec.ReplicationFactor
	nbPodToCreate := nbRequiredPodForSpec + nbPodByNodeMigration - cluster.Status.Cluster.NumberOfPods
	if nbPodToCreate > 0 {
		for i := int32(0); i < nbPodToCreate; i++ {
			_, err := c.podControl.CreatePod(cluster)
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}

	clusterPodSpecHash, err := podctrl.GenerateMD5Spec(&cluster.Spec.PodTemplate.Spec)
	if err != nil {
		return false, err
	}
	// pods with new version are ready it is time to migrate slots and key
	newNodes := nodes.FilterByFunc(func(n *redis.Node) bool {
		if n.Pod == nil {
			return false
		}
		return comparePodSpecMD5Hash(clusterPodSpecHash, n.Pod)
	})
	oldNodes := nodes.FilterByFunc(func(n *redis.Node) bool {
		if n.Pod == nil {
			return false
		}
		return !comparePodSpecMD5Hash(clusterPodSpecHash, n.Pod)
	})
	newPrimaryNodes, newReplicaNodes, newPrimaryNodesNoSlots := clustering.ClassifyNodesByRole(newNodes)
	oldPrimaryNodes, oldReplicaNodes, _ := clustering.ClassifyNodesByRole(oldNodes)

	selectedPrimaries, selectedNewPrimaries, err := clustering.SelectPrimariesToReplace(oldPrimaryNodes, newPrimaryNodes, newPrimaryNodesNoSlots, *cluster.Spec.NumberOfPrimaries, 1)
	if err != nil {
		return false, err
	}
	currentReplicas := append(oldReplicaNodes, newReplicaNodes...)
	futureReplicas := newPrimaryNodesNoSlots.FilterByFunc(func(n *redis.Node) bool {
		for _, newPrimary := range selectedNewPrimaries {
			if n.ID == newPrimary.ID {
				return false
			}
		}
		return true
	})

	primaryToReplicas, err := clustering.PlaceReplicas(rCluster, selectedPrimaries, currentReplicas, futureReplicas, *cluster.Spec.ReplicationFactor)
	if err != nil {
		glog.Errorf("unable to place replicas: %v", err)
		return false, err
	}
	cluster.Status.Cluster.NodesPlacement = rapi.NodesPlacementInfoOptimal

	if err = clustering.AttachReplicasToPrimary(ctx, rCluster, admin, primaryToReplicas); err != nil {
		glog.Error("unable to dispatch replica on new primary, err:", err)
		return false, err
	}

	currentPrimaries := append(oldPrimaryNodes, newPrimaryNodes...)
	allPrimaries := append(currentPrimaries, selectedNewPrimaries...)
	// now we can move slot from old primary to new primary
	if err = clustering.DispatchSlotsToNewPrimaries(ctx, rCluster, admin, selectedPrimaries, currentPrimaries, allPrimaries); err != nil {
		glog.Error("unable to dispatch slot on new primary, err:", err)
		return false, err
	}

	removedPrimaries, removeReplicas := getOldNodesToRemove(currentPrimaries, selectedPrimaries, nodes)

	nodesToDelete, err := detachAndForgetNodes(ctx, admin, removedPrimaries, removeReplicas)
	if err != nil {
		glog.Error("unable to detach and forget old primaries and associated replicas, err:", err)
		return false, err
	}

	for _, node := range nodesToDelete {
		if err := c.podControl.DeletePod(cluster, node.Pod.Name); err != nil {
			glog.Errorf("unable to delete the pod %s/%s, err:%v", node.Pod.Name, node.Pod.Namespace, err)
			return false, err
		}
	}

	return false, nil
}

// managePodScaleDown used to manage the scale down of a cluster
func (c *Controller) managePodScaleDown(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, newCluster *redis.Cluster, nodes redis.Nodes) (bool, error) {
	glog.V(6).Info("managePodScaleDown START")
	defer glog.V(6).Info("managePodScaleDown STOP")
	if nodesToDelete, ok := shouldDeleteNodes(cluster); ok {
		for _, node := range nodesToDelete {
			if err := c.podControl.DeletePod(cluster, node.PodName); err != nil {
				return false, err
			}
		}
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

	if primaryToReplicas, ok := checkReplicationFactor(cluster); !ok {
		glog.V(6).Info("checkReplicationFactor NOT OK")
		if err := c.reconcileReplicationFactor(ctx, admin, cluster, newCluster, nodes, primaryToReplicas); err != nil {
			return false, err
		}
	}

	return false, nil
}

func (c *Controller) removeReplicasOfReplica(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, nodes redis.Nodes, replicasOfReplica map[string][]*rapi.RedisClusterNode) {
	// Currently the algorithm assumes redis is able to attach a replica to another replica.
	// In practice, we detach the replicas and the controller reassigns the replica to a primary if needed.
	for _, replicas := range replicasOfReplica {
		for _, replica := range replicas {
			node, err := nodes.GetNodeByID(replica.ID)
			if err != nil {
				glog.Errorf("unable to find node with ID %s: %v", replica.ID, err)
			} else if err := admin.DetachReplica(ctx, node); err != nil {
				glog.Errorf("unable to detach replica with ID %s: %v", node.ID, err)
			} else if err := c.podControl.DeletePod(cluster, replica.PodName); err != nil {
				glog.Errorf("unable to delete pod %s corresponding to replica with the ID %s: %v", replica.PodName, node.ID, err)
			}
		}
	}
}

func (c *Controller) scaleDownPrimaries(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, newCluster *redis.Cluster, nodes redis.Nodes, nbPrimaryToDelete int32) error {
	newNumberOfPrimaries := cluster.Status.Cluster.NumberOfPrimaries

	// Decrease the number of primaries by one in order to limit the impact on the client
	if nbPrimaryToDelete > 0 {
		newNumberOfPrimaries--
	}

	// Create the new primaries
	newPrimaries, currPrimaries, allPrimary, err := clustering.SelectPrimaries(newCluster, nodes, newNumberOfPrimaries)
	if err != nil {
		glog.Errorf("error while dispatching slots to primaries: %v", err)
		cluster.Status.Cluster.Status = rapi.ClusterStatusKO
		newCluster.Status = rapi.ClusterStatusKO
		return err
	}

	// Dispatch slots to the new primaries
	if err = clustering.DispatchSlotsToNewPrimaries(ctx, newCluster, admin, newPrimaries, currPrimaries, allPrimary); err != nil {
		glog.Errorf("unable to dispatch slot to new primary: %v", err)
		return err
	}

	// Get old primaries and replicas to be removed
	removedPrimaries, removedReplicas := getOldNodesToRemove(currPrimaries, newPrimaries, nodes)

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
	if len(errs) > 0 {
		return errors.NewAggregate(errs)
	}
	return nil
}

func (c *Controller) reconcileReplicationFactor(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, nodes redis.Nodes, primaryToReplicas map[string][]string) error {
	var errs []error
	var replicas redis.Nodes
	for _, replicaIDs := range primaryToReplicas {
		for _, replicaID := range replicaIDs {
			replica, err := rCluster.GetNodeByID(replicaID)
			if err != nil {
				errs = append(errs, err)
			}
			replicas = append(replicas, replica)
		}
	}
	for primaryID, replicaIDs := range primaryToReplicas {
		diff := int32(len(replicaIDs)) - *cluster.Spec.ReplicationFactor
		if diff < 0 {
			// not enough replicas on this primary
			if err := c.attachReplicasToPrimary(ctx, rCluster, admin, primaryID, nodes, -diff); err != nil {
				errs = append(errs, err)
			}
		}
		if diff > 0 {
			var replicaNodes redis.Nodes
			for _, replicaID := range replicaIDs {
				replica, err := rCluster.GetNodeByID(replicaID)
				if err != nil {
					errs = append(errs, err)
				}
				replicaNodes = append(replicaNodes, replica)
			}
			// too many replicas on this primary
			if err := c.removeReplicasFromPrimary(ctx, admin, cluster, rCluster, primaryID, replicaNodes, replicas, diff); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.NewAggregate(errs)
}

func (c *Controller) removeReplicasFromPrimary(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, newCluster *redis.Cluster, primaryID string, replicas redis.Nodes, allReplicas redis.Nodes, diff int32) error {
	var errs []error
	// select a replica to be removed
	nodesToDelete, err := selectReplicasToDelete(newCluster, primaryID, replicas, allReplicas, diff)
	if err != nil {
		return err
	}
	for _, node := range nodesToDelete {
		admin.DetachReplica(ctx, node)
		if node.Pod != nil {
			if err := c.podControl.DeletePod(cluster, node.Pod.Name); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.NewAggregate(errs)
}

func (c *Controller) attachReplicasToPrimary(ctx context.Context, cluster *redis.Cluster, admin redis.AdminInterface, primaryID string, nodes redis.Nodes, diff int32) error {
	var errs []error
	// get the primary node
	primary, err := nodes.GetNodeByID(primaryID)
	if err != nil {
		return err
	}
	// find an available redis nodes to be the new replicas
	replicas, err := getNewReplicas(cluster, nodes, diff)
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		if err = admin.AttachReplicaToPrimary(ctx, replica, primary); err != nil {
			glog.Errorf("error during reconcileReplicationFactor")
			errs = append(errs, err)
		}
	}
	return errors.NewAggregate(errs)
}

func getOldNodesToRemove(currPrimaries, newPrimaries, nodes redis.Nodes) (removedPrimaries, removeReplicas redis.Nodes) {
	removedPrimaries = redis.Nodes{}
	for _, node := range currPrimaries {
		if _, err := newPrimaries.GetNodeByID(node.ID); err != nil {
			removedPrimaries = append(removedPrimaries, node)
		}
	}

	removeReplicas = redis.Nodes{}
	for _, primary := range removedPrimaries {
		replicas := nodes.FilterByFunc(func(node *redis.Node) bool {
			return redis.IsReplica(node) && (node.PrimaryReferent == primary.ID)
		})
		removeReplicas = append(removeReplicas, replicas...)
	}

	return removedPrimaries, removeReplicas
}

func detachAndForgetNodes(ctx context.Context, admin redis.AdminInterface, primaries, replicas redis.Nodes) (redis.Nodes, error) {
	for _, node := range replicas {
		if err := admin.DetachReplica(ctx, node); err != nil {
			glog.Errorf("unable to detach the replica with ID %s: %v", node.ID, err)
		}
	}

	removedNodes := append(primaries, replicas...)
	for _, node := range removedNodes {
		if err := admin.ForgetNode(ctx, node.ID); err != nil {
			glog.Errorf("unable to forget the node with ID %s: %v", node.ID, err)
		}
	}
	return removedNodes, nil
}

func getNewReplicas(cluster *redis.Cluster, nodes redis.Nodes, nbReplicadNeeded int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	currentReplicas := redis.Nodes{}
	candidateReplicas := redis.Nodes{}

	if nbReplicadNeeded <= 0 {
		return selection, nil
	}

	for _, node := range nodes {
		if redis.IsReplica(node) {
			currentReplicas = append(currentReplicas, node)
		}
		if redis.IsPrimaryWithNoSlot(node) {
			candidateReplicas = append(candidateReplicas, node)
		}
	}
	nodeAdded := false
	for len(selection) < int(nbReplicadNeeded) && !nodeAdded {
		i := 0
		for _, candidate := range candidateReplicas {
			if clustering.ZonesBalanced(cluster, candidate, currentReplicas) {
				nodeAdded = true
				selection = append(selection, candidate)
			} else {
				candidateReplicas[i] = candidate
				i++
			}
			if len(selection) >= int(nbReplicadNeeded) {
				return selection, nil
			}
		}
		candidateReplicas = candidateReplicas[:i]
	}
	if !nodeAdded {
		for _, candidate := range candidateReplicas {
			selection = append(selection, candidate)
			if len(selection) >= int(nbReplicadNeeded) {
				return selection, nil
			}
		}
	}
	return selection, fmt.Errorf("insufficient number of replicas - expected: %v, actual: %v", nbReplicadNeeded, len(selection))
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

func selectReplicasToDelete(cluster *redis.Cluster, primaryID string, primaryReplicas redis.Nodes, allReplicas redis.Nodes, nbReplicasToDelete int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	primaryNode, err := cluster.GetNodeByID(primaryID)
	if err != nil {
		return selection, err
	}
	for len(selection) < int(nbReplicasToDelete) {
		if removeReplicasByZone(cluster, &selection, primaryNode, primaryReplicas, allReplicas, nbReplicasToDelete) {
			return selection, nil
		}
	}
	return selection, nil
}

func sameZoneAsPrimary(selection *redis.Nodes, primary *redis.Node, replicas redis.Nodes, zoneToReplicas map[string]redis.Nodes) bool {
	for _, replica := range replicas {
		if primary.Zone == replica.Zone {
			addToSelection(selection, replica, zoneToReplicas)
			return true
		}
	}
	return false
}

func removeReplicasInLargestZone(selection *redis.Nodes, replicas redis.Nodes, zoneToReplicas map[string]redis.Nodes, nbReplicasToDelete int32) bool {
	nodeAdded := false
	largestZoneSize := largestZone(zoneToReplicas)
	for _, replica := range replicas {
		// check if this replica is in the largest zone and there are no other replica zones of equal size
		if len(zoneToReplicas[replica.Zone]) == largestZoneSize && isLargestZone(zoneToReplicas, largestZoneSize) {
			// add it to the selection of replicas to delete and remove it from zoneToReplicas
			nodeAdded = true
			addToSelection(selection, replica, zoneToReplicas)
			if len(*selection) == int(nbReplicasToDelete) {
				break
			}
		}
	}
	return nodeAdded
}

func removeReplicasByZone(cluster *redis.Cluster, selection *redis.Nodes, primary *redis.Node, primaryReplicas redis.Nodes, allReplicas redis.Nodes, nbReplicasToDelete int32) bool {
	zoneToReplicas := clustering.ZoneToNodes(cluster, primaryReplicas)
	nodeAdded := removeReplicasInLargestZone(selection, primaryReplicas, zoneToReplicas, nbReplicasToDelete)
	if len(*selection) == int(nbReplicasToDelete) {
		return true
	}
	if !nodeAdded {
		// all replicas are in zones of equal size
		if sameZoneAsPrimary(selection, primary, primaryReplicas, zoneToReplicas) {
			if len(*selection) == int(nbReplicasToDelete) {
				return true
			}
		}
		// remove replicas that have been selected to be deleted from the list of all replicas
		i := 0
		for _, replica := range allReplicas {
			if !selectionContainsReplica(selection, replica) {
				allReplicas[i] = replica
				i++
			}
		}
		allReplicas = allReplicas[:i]
		// remove the replica in the largest zone cluster-wide
		zoneToAllReplicas := clustering.ZoneToNodes(cluster, allReplicas)
		removeReplicasInLargestZone(selection, primaryReplicas, zoneToAllReplicas, nbReplicasToDelete)
		if len(*selection) == int(nbReplicasToDelete) {
			return true
		}
		// if we still do not have enough replicas, pick the first available
		for _, replica := range primaryReplicas {
			if !selectionContainsReplica(selection, replica) {
				addToSelection(selection, replica, zoneToReplicas)
				if len(*selection) == int(nbReplicasToDelete) {
					return true
				}
			}
		}
	}
	return false
}

func selectionContainsReplica(selection *redis.Nodes, replica *redis.Node) bool {
	for _, s := range *selection {
		if replica == s {
			return true
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

func largestZone(zoneToNodes map[string]redis.Nodes) int {
	largest := 0
	for _, nodes := range zoneToNodes {
		if len(nodes) > largest {
			largest = len(nodes)
		}
	}
	return largest
}

func addToSelection(selection *redis.Nodes, replica *redis.Node, zoneToReplicas map[string]redis.Nodes) {
	*selection = append(*selection, replica)
	removeReplicaFromZone(replica, zoneToReplicas)
}

// applyConfiguration apply new configuration if needed:
// - add or delete pods
// - configure the redis-server process
func (c *Controller) applyConfiguration(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster) (bool, error) {
	glog.V(6).Info("applyConfiguration START")
	defer glog.V(6).Info("applyConfiguration STOP")

	hasChanged := false

	// Configuration
	cReplicationFactor := *cluster.Spec.ReplicationFactor
	cNbPrimaries := *cluster.Spec.NumberOfPrimaries

	newCluster, nodes, err := newRedisCluster(ctx, admin, cluster, c.kubeClient)
	if err != nil {
		glog.Errorf("unable to create the RedisCluster view: %v", err)
		return false, err
	}

	if needRollingUpdate(cluster) {
		if setRollingUpdateCondition(&cluster.Status, true) {
			if cluster, err = c.updateHandler(cluster); err != nil {
				return false, err
			}
		}

		glog.Info("applyConfiguration needRollingUpdate")
		return c.manageRollingUpdate(ctx, admin, cluster, newCluster, nodes)
	}
	if setRollingUpdateCondition(&cluster.Status, false) {
		if cluster, err = c.updateHandler(cluster); err != nil {
			return false, err
		}
	}

	if needLessPods(cluster) {
		if setRebalancingCondition(&cluster.Status, true) {
			if cluster, err = c.updateHandler(cluster); err != nil {
				return false, err
			}
		}
		glog.Info("applyConfiguration needLessPods")
		return c.managePodScaleDown(ctx, admin, cluster, newCluster, nodes)
	}
	if setRebalancingCondition(&cluster.Status, false) {
		if cluster, err = c.updateHandler(cluster); err != nil {
			return false, err
		}
	}

	clusterStatus := &cluster.Status.Cluster
	if (clusterStatus.NumberOfPods - clusterStatus.NumberOfRedisNodesRunning) != 0 {
		glog.V(3).Infof("Not all redis nodes are running, numberOfPods: %d, numberOfRedisNodesRunning: %d", clusterStatus.NumberOfPods, clusterStatus.NumberOfRedisNodesRunning)
		return false, err
	}

	// First, we select the new primaries
	newPrimaries, currPrimaries, allPrimaries, err := clustering.SelectPrimaries(newCluster, nodes, cNbPrimaries)
	if err != nil {
		glog.Errorf("cannot dispatch slots to primaries: %v", err)
		newCluster.Status = rapi.ClusterStatusKO
		return false, err
	}
	if len(newPrimaries) != len(currPrimaries) {
		hasChanged = true
	}

	// Second, select replica nodes
	currentReplicaNodes := nodes.FilterByFunc(redis.IsReplica)

	// New replicas are currently primaries with no slots
	newReplicas := nodes.FilterByFunc(func(nodeA *redis.Node) bool {
		for _, nodeB := range newPrimaries {
			if nodeA.ID == nodeB.ID {
				return false
			}
		}
		for _, nodeB := range currentReplicaNodes {
			if nodeA.ID == nodeB.ID {
				return false
			}
		}
		return true
	})

	// Depending on whether we scale up or down, we will dispatch replicas before/after the dispatch of slots
	if cNbPrimaries < int32(len(currPrimaries)) {
		// this happens after a scale down of the cluster
		// we should dispatch slots before dispatching replicas
		if err := clustering.DispatchSlotsToNewPrimaries(ctx, newCluster, admin, newPrimaries, currPrimaries, allPrimaries); err != nil {
			glog.Errorf("unable to dispatch slot on new primary: %v", err)
			return false, err
		}

		// assign primary/replica roles
		newRedisReplicasByPrimary, err := clustering.PlaceReplicas(newCluster, newPrimaries, currentReplicaNodes, newReplicas, cReplicationFactor)
		if err != nil {
			glog.Errorf("unable to place replicas: %v", err)
			return false, err
		}

		if err := clustering.AttachReplicasToPrimary(ctx, newCluster, admin, newRedisReplicasByPrimary); err != nil {
			glog.Errorf("unable to dispatch replica on new primary: %v", err)
			return false, err
		}
	} else {
		// scaling up the number of primaries or the number of primaries hasn't changed
		// assign primary/replica roles
		newRedisReplicasByPrimary, err := clustering.PlaceReplicas(newCluster, newPrimaries, currentReplicaNodes, newReplicas, cReplicationFactor)
		if err != nil {
			glog.Errorf("unable to place replicas: %v", err)
			return false, err
		}

		if err := clustering.AttachReplicasToPrimary(ctx, newCluster, admin, newRedisReplicasByPrimary); err != nil {
			glog.Errorf("unable to dispatch replica on new primary: %v", err)
			return false, err
		}

		if err := clustering.DispatchSlotsToNewPrimaries(ctx, newCluster, admin, newPrimaries, currPrimaries, allPrimaries); err != nil {
			glog.Errorf("unable to dispatch slot on new primary: %v", err)
			return false, err
		}
	}

	glog.V(4).Infof("new nodes status: \n %v", nodes)

	newCluster.Status = rapi.ClusterStatusOK
	// wait a bit for the cluster to propagate configuration to reduce warning logs because of temporary inconsistency
	time.Sleep(1 * time.Second)
	return hasChanged, nil
}

func newRedisCluster(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, kubeClient kubernetes.Interface) (*redis.Cluster, redis.Nodes, error) {
	infos, err := admin.GetClusterInfos(ctx)
	if redis.IsPartialError(err) {
		glog.Errorf("error getting consolidated view of the cluster: %v", err)
		return nil, nil, err
	}

	// now we can trigger the rebalance
	nodes := infos.GetNodes()

	// build redis cluster vision
	rCluster := redis.NewCluster(cluster.Name, cluster.Namespace)
	rCluster.NodeSelector = cluster.Spec.NodeSelector
	rCluster.KubeNodes = getKubeNodes(ctx, kubeClient, rCluster.NodeSelector)
	for _, node := range nodes {
		rCluster.Nodes[node.ID] = node
	}

	for _, node := range cluster.Status.Cluster.Nodes {
		if rNode, ok := rCluster.Nodes[node.ID]; ok {
			rNode.Pod = node.Pod
			if rNode.Pod != nil {
				rNode.Zone = rCluster.GetZone(rNode.Pod.Spec.NodeName)
				if rNode.Zone == redis.UnknownZone {
					glog.V(4).Infof("Redis node %s has an unknown zone", rNode.ID)
				}
			}
		}
	}

	return rCluster, nodes, nil
}

func getKubeNodes(ctx context.Context, kubeClient kubernetes.Interface, nodeSelector map[string]string) []v1.Node {
	options := metav1.ListOptions{}
	labelSelector := ""
	if len(nodeSelector) > 0 {
		labelSelector = labels.FormatLabels(nodeSelector)
	}
	if labelSelector != "" {
		options.LabelSelector = labelSelector
	}
	nodeList, err := kubeClient.CoreV1().Nodes().List(ctx, options)
	if err != nil {
		glog.Errorf("Error getting nodes with label selector %s: %v", labelSelector, err)
	}
	return nodeList.Items
}
