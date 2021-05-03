package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1"
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
	nbRequirePodForSpec := *cluster.Spec.NumberOfMaster * (1 + *cluster.Spec.ReplicationFactor)
	nbPodByNodeMigration := 1 + *cluster.Spec.ReplicationFactor
	nbPodToCreate := nbRequirePodForSpec + nbPodByNodeMigration - cluster.Status.Cluster.NumberOfPods
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
	newMasterNodes, newSlaveNodes, newNoneNodes := clustering.ClassifyNodesByRole(newNodes)
	oldMasterNodes, oldSlaveNodes, _ := clustering.ClassifyNodesByRole(oldNodes)

	selectedMasters, selectedNewMasters, err := clustering.SelectMastersToReplace(oldMasterNodes, newMasterNodes, newNoneNodes, *cluster.Spec.NumberOfMaster, 1)
	if err != nil {
		return false, err
	}
	currentSlaves := append(oldSlaveNodes, newSlaveNodes...)
	futurSlaves := newNoneNodes.FilterByFunc(func(n *redis.Node) bool {
		for _, newMaster := range selectedNewMasters {
			if n.ID == newMaster.ID {
				return false
			}
		}
		return true
	})

	slavesByMaster, bestEffort := clustering.PlaceSlaves(rCluster, selectedMasters, currentSlaves, futurSlaves, *cluster.Spec.ReplicationFactor)
	if bestEffort {
		cluster.Status.Cluster.NodesPlacement = rapi.NodesPlacementInfoBestEffort
	} else {
		cluster.Status.Cluster.NodesPlacement = rapi.NodesPlacementInfoOptimal
	}

	if err = clustering.AttachingSlavesToMaster(ctx, rCluster, admin, slavesByMaster); err != nil {
		glog.Error("unable to dispatch slave on new master, err:", err)
		return false, err
	}

	currentMasters := append(oldMasterNodes, newMasterNodes...)
	allMasters := append(currentMasters, selectedNewMasters...)
	// now we can move slot from old master to new master
	if err = clustering.DispatchSlotToNewMasters(ctx, rCluster, admin, selectedMasters, currentMasters, allMasters); err != nil {
		glog.Error("unable to dispatch slot on new master, err:", err)
		return false, err
	}

	removedMasters, removeSlaves := getOldNodesToRemove(currentMasters, selectedMasters, nodes)

	nodesToDelete, err := detachAndForgetNodes(ctx, admin, removedMasters, removeSlaves)
	if err != nil {
		glog.Error("unable to detach and forget old masters and associated slaves, err:", err)
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

	if slavesOfSlave, ok := checkSlavesOfSlave(cluster); !ok {
		glog.V(6).Info("checkSlavesOfSlave NOT OK")
		c.removeSlavesOfSlave(ctx, admin, cluster, nodes, slavesOfSlave)
		return true, nil
	}

	if nbMasterToDelete, ok := checkNumberOfMasters(cluster); !ok {
		glog.V(6).Info("checkNumberOfMasters NOT OK")
		if err := c.scaleDownMasters(ctx, admin, cluster, newCluster, nodes, nbMasterToDelete); err != nil {
			return false, err
		}

	}

	if slaveByMaster, ok := checkReplicationFactor(cluster); !ok {
		glog.V(6).Info("checkReplicationFactor NOT OK")
		if err := c.attachSlavesToMaster(ctx, admin, cluster, nodes, slaveByMaster); err != nil {
			return false, err
		}
	}

	return false, nil
}

func (c *Controller) removeSlavesOfSlave(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, nodes redis.Nodes, slavesOfSlave map[string][]*rapi.RedisClusterNode) {
	// Currently the algorithm assumes redis is able to attach a slave to another slave.
	// In practice, we detach the slaves and the controller reassigns the slave to a master if needed.
	for _, slaves := range slavesOfSlave {
		for _, slave := range slaves {
			node, err := nodes.GetNodeByID(slave.ID)
			if err != nil {
				glog.Errorf("unable to find node with ID %s: %v", slave.ID, err)
			} else if err := admin.DetachSlave(ctx, node); err != nil {
				glog.Errorf("unable to detach slave with ID %s: %v", node.ID, err)
			} else if err := c.podControl.DeletePod(cluster, slave.PodName); err != nil {
				glog.Errorf("unable to delete pod %s corresponding to slave with the ID %s: %v", slave.PodName, node.ID, err)
			}
		}
	}
}

func (c *Controller) scaleDownMasters(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, newCluster *redis.Cluster, nodes redis.Nodes, nbMasterToDelete int32) error {
	newNumberOfMaster := cluster.Status.Cluster.NumberOfMasters

	// Decrease the number of masters by one in order to limit the impact on the client
	if nbMasterToDelete > 0 {
		newNumberOfMaster--
	}

	// Create the new masters
	newMasters, curMasters, allMaster, err := clustering.DispatchMasters(newCluster, nodes, newNumberOfMaster)
	if err != nil {
		glog.Errorf("error while dispatching slots to masters: %v", err)
		cluster.Status.Cluster.Status = rapi.ClusterStatusKO
		newCluster.Status = rapi.ClusterStatusKO
		return err
	}

	// Dispatch slots to the new masters
	if err = clustering.DispatchSlotToNewMasters(ctx, newCluster, admin, newMasters, curMasters, allMaster); err != nil {
		glog.Errorf("unable to dispatch slot to new master: %v", err)
		return err
	}

	// Get old masters and slaves to be removed
	removedMasters, removeSlaves := getOldNodesToRemove(curMasters, newMasters, nodes)

	// Detach and forget nodes to be removed
	if _, err = detachAndForgetNodes(ctx, admin, removedMasters, removeSlaves); err != nil {
		glog.Errorf("unable to detach and forget old masters: %v", err)
		return err
	}

	// Delete nodes
	nodesToDelete := append(removedMasters, removeSlaves...)
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

func (c *Controller) attachSlavesToMaster(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, nodes redis.Nodes, slaveByMaster map[string][]string) error {
	var errs []error
	for masterID, slaveIDs := range slaveByMaster {
		diff := int32(len(slaveIDs)) - *cluster.Spec.ReplicationFactor
		if diff < 0 {
			// not enough slaves on this master
			// find an available redis node to be the new slave
			slaves, err := searchForSlaveNodes(nodes, -diff)
			if err != nil {
				return err
			}
			// get the master node
			master, err := nodes.GetNodeByID(masterID)
			if err != nil {
				return err
			}
			for _, node := range slaves {
				if err = admin.AttachSlaveToMaster(ctx, node, master); err != nil {
					glog.Errorf("error during attachSlavesToMaster")
					errs = append(errs, err)
				}
			}
		} else if diff > 0 {
			// TODO (IMP): this if can be remove since it should not happen.
			// too many slave on this master
			// select on slave to be removed
			nodesToDelete, err := selectSlavesToDelete(cluster, nodes, masterID, slaveIDs, diff)
			if err != nil {
				return err
			}
			for _, node := range nodesToDelete {
				admin.DetachSlave(ctx, node)
				if node.Pod != nil {
					if err := c.podControl.DeletePod(cluster, node.Pod.Name); err != nil {
						errs = append(errs, err)
					}
				}
			}
		}
	}
	return errors.NewAggregate(errs)
}

func getOldNodesToRemove(curMasters, newMasters, nodes redis.Nodes) (removedMasters, removeSlaves redis.Nodes) {
	removedMasters = redis.Nodes{}
	for _, node := range curMasters {
		if _, err := newMasters.GetNodeByID(node.ID); err != nil {
			removedMasters = append(removedMasters, node)
		}
	}

	removeSlaves = redis.Nodes{}
	for _, master := range removedMasters {
		slaves := nodes.FilterByFunc(func(node *redis.Node) bool {
			return redis.IsSlave(node) && (node.MasterReferent == master.ID)
		})
		removeSlaves = append(removeSlaves, slaves...)
	}

	return removedMasters, removeSlaves
}

func detachAndForgetNodes(ctx context.Context, admin redis.AdminInterface, masters, slaves redis.Nodes) (redis.Nodes, error) {
	for _, node := range slaves {
		if err := admin.DetachSlave(ctx, node); err != nil {
			glog.Errorf("unable to detach the slave with ID %s: %v", node.ID, err)
		}
	}

	removedNodes := append(masters, slaves...)
	for _, node := range removedNodes {
		if err := admin.ForgetNode(ctx, node.ID); err != nil {
			glog.Errorf("unable to forget the node with ID %s: %v", node.ID, err)
		}
	}
	return removedNodes, nil
}

func searchForSlaveNodes(nodes redis.Nodes, nbSlavedNeeded int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	var err error

	if nbSlavedNeeded <= 0 {
		return selection, err
	}

	for _, node := range nodes {
		if len(selection) == int(nbSlavedNeeded) {
			break
		}
		if redis.IsMasterWithNoSlot(node) {
			selection = append(selection, node)
		}
	}

	if len(selection) < int(nbSlavedNeeded) {
		return selection, fmt.Errorf("insufficient number of slaves")
	}
	return selection, err
}

func selectSlavesToDelete(cluster *rapi.RedisCluster, nodes redis.Nodes, idMaster string, slavesID []string, nbSlavesToDelete int32) (redis.Nodes, error) {
	selection := redis.Nodes{}

	masterNodeName := "default"
	for _, node := range cluster.Status.Cluster.Nodes {
		if node.ID == idMaster {
			if node.Pod != nil {
				// TODO improve this with Node labels
				masterNodeName = node.Pod.Spec.NodeName
			}
		}
	}

	secondSelection := redis.Nodes{}

	for _, slaveID := range slavesID {
		if len(selection) == int(nbSlavesToDelete) {
			return selection, nil
		}
		if slave, err := nodes.GetNodeByID(slaveID); err == nil {
			if slave.Pod.Spec.NodeName == masterNodeName {
				selection = append(selection, slave)
			} else {
				secondSelection = append(secondSelection, slave)
			}
		}
	}

	for _, node := range secondSelection {
		if len(selection) == int(nbSlavesToDelete) {
			return selection, nil
		}
		selection = append(selection, node)

	}

	if len(selection) == int(nbSlavesToDelete) {
		return selection, nil
	}

	return selection, fmt.Errorf("insufficient number of slaves to delete")
}

// applyConfiguration apply new configuration if needed:
// - add or delete pods
// - configure the redis-server process
func (c *Controller) applyConfiguration(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster) (bool, error) {
	glog.V(6).Info("applyConfiguration START")
	defer glog.V(6).Info("applyConfiguration STOP")

	asChanged := false

	// Configuration
	cReplicaFactor := *cluster.Spec.ReplicationFactor
	cNbMaster := *cluster.Spec.NumberOfMaster

	newCluster, nodes, err := newRedisCluster(ctx, admin, cluster)
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

	// First, we define the new masters
	newMasters, curMasters, allMaster, err := clustering.DispatchMasters(newCluster, nodes, cNbMaster)
	if err != nil {
		glog.Errorf("cannot dispatch slots to masters: %v", err)
		newCluster.Status = rapi.ClusterStatusKO
		return false, err
	}
	if len(newMasters) != len(curMasters) {
		asChanged = true
	}

	// Second, select slave nodes
	currentSlaveNodes := nodes.FilterByFunc(redis.IsSlave)

	// New slaves are currently masters with no slots
	newSlave := nodes.FilterByFunc(func(nodeA *redis.Node) bool {
		for _, nodeB := range newMasters {
			if nodeA.ID == nodeB.ID {
				return false
			}
		}
		for _, nodeB := range currentSlaveNodes {
			if nodeA.ID == nodeB.ID {
				return false
			}
		}
		return true
	})

	// Depending on whether we scale up or down, we will dispatch slaves before/after the dispatch of slots
	if cNbMaster < int32(len(curMasters)) {
		// this happens after a scale down of the cluster
		// we should dispatch slots before dispatching slaves
		if err := clustering.DispatchSlotToNewMasters(ctx, newCluster, admin, newMasters, curMasters, allMaster); err != nil {
			glog.Errorf("unable to dispatch slot on new master: %v", err)
			return false, err
		}

		// assign master/slave roles
		newRedisSlavesByMaster, bestEffort := clustering.PlaceSlaves(newCluster, newMasters, currentSlaveNodes, newSlave, cReplicaFactor)
		if bestEffort {
			newCluster.NodesPlacement = rapi.NodesPlacementInfoBestEffort
		}

		if err := clustering.AttachingSlavesToMaster(ctx, newCluster, admin, newRedisSlavesByMaster); err != nil {
			glog.Errorf("unable to dispatch slave on new master: %v", err)
			return false, err
		}
	} else {
		// scaling up the number of masters or the number of masters hasn't changed
		// assign master/slave roles
		newRedisSlavesByMaster, bestEffort := clustering.PlaceSlaves(newCluster, newMasters, currentSlaveNodes, newSlave, cReplicaFactor)
		if bestEffort {
			newCluster.NodesPlacement = rapi.NodesPlacementInfoBestEffort
		}

		if err := clustering.AttachingSlavesToMaster(ctx, newCluster, admin, newRedisSlavesByMaster); err != nil {
			glog.Errorf("unable to dispatch slave on new master: %v", err)
			return false, err
		}

		if err := clustering.DispatchSlotToNewMasters(ctx, newCluster, admin, newMasters, curMasters, allMaster); err != nil {
			glog.Errorf("unable to dispatch slot on new master: %v", err)
			return false, err
		}
	}

	glog.V(4).Infof("new nodes status: \n %v", nodes)

	newCluster.Status = rapi.ClusterStatusOK
	// wait a bit for the cluster to propagate configuration to reduce warning logs because of temporary inconsistency
	time.Sleep(1 * time.Second)
	return asChanged, nil
}

func newRedisCluster(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster) (*redis.Cluster, redis.Nodes, error) {
	infos, err := admin.GetClusterInfos(ctx)
	if redis.IsPartialError(err) {
		glog.Errorf("error getting consolidated view of the cluster: %v", err)
		return nil, nil, err
	}

	// now we can trigger the rebalance
	nodes := infos.GetNodes()

	// build redis cluster vision
	rCluster := &redis.Cluster{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
		Nodes:     make(map[string]*redis.Node),
	}

	for _, node := range nodes {
		rCluster.Nodes[node.ID] = node
	}

	for _, node := range cluster.Status.Cluster.Nodes {
		if rNode, ok := rCluster.Nodes[node.ID]; ok {
			rNode.Pod = node.Pod
		}
	}

	return rCluster, nodes, nil
}
