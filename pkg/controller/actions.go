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
	newMasterNodes, newSlaveNodes, newMasterNodesNoSlots := clustering.ClassifyNodesByRole(newNodes)
	oldMasterNodes, oldSlaveNodes, _ := clustering.ClassifyNodesByRole(oldNodes)

	selectedMasters, selectedNewMasters, err := clustering.SelectMastersToReplace(oldMasterNodes, newMasterNodes, newMasterNodesNoSlots, *cluster.Spec.NumberOfMaster, 1)
	if err != nil {
		return false, err
	}
	currentSlaves := append(oldSlaveNodes, newSlaveNodes...)
	futureSlaves := newMasterNodesNoSlots.FilterByFunc(func(n *redis.Node) bool {
		for _, newMaster := range selectedNewMasters {
			if n.ID == newMaster.ID {
				return false
			}
		}
		return true
	})

	masterToSlaves, err := clustering.PlaceSlaves(rCluster, selectedMasters, currentSlaves, futureSlaves, *cluster.Spec.ReplicationFactor)
	if err != nil {
		glog.Errorf("unable to place slaves: %v", err)
		return false, err
	}
	cluster.Status.Cluster.NodesPlacement = rapi.NodesPlacementInfoOptimal

	if err = clustering.AttachSlavesToMaster(ctx, rCluster, admin, masterToSlaves); err != nil {
		glog.Error("unable to dispatch slave on new master, err:", err)
		return false, err
	}

	currentMasters := append(oldMasterNodes, newMasterNodes...)
	allMasters := append(currentMasters, selectedNewMasters...)
	// now we can move slot from old master to new master
	if err = clustering.DispatchSlotsToNewMasters(ctx, rCluster, admin, selectedMasters, currentMasters, allMasters); err != nil {
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

	if masterToSlaves, ok := checkReplicationFactor(cluster); !ok {
		glog.V(6).Info("checkReplicationFactor NOT OK")
		if err := c.reconcileReplicationFactor(ctx, admin, cluster, newCluster, nodes, masterToSlaves); err != nil {
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
	newMasters, curMasters, allMaster, err := clustering.SelectMasters(newCluster, nodes, newNumberOfMaster)
	if err != nil {
		glog.Errorf("error while dispatching slots to masters: %v", err)
		cluster.Status.Cluster.Status = rapi.ClusterStatusKO
		newCluster.Status = rapi.ClusterStatusKO
		return err
	}

	// Dispatch slots to the new masters
	if err = clustering.DispatchSlotsToNewMasters(ctx, newCluster, admin, newMasters, curMasters, allMaster); err != nil {
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

func (c *Controller) reconcileReplicationFactor(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, nodes redis.Nodes, masterToSlaves map[string][]string) error {
	var errs []error
	var slaves redis.Nodes
	for _, slaveIDs := range masterToSlaves {
		for _, slaveID := range slaveIDs {
			slave, err := rCluster.GetNodeByID(slaveID)
			if err != nil {
				errs = append(errs, err)
			}
			slaves = append(slaves, slave)
		}
	}
	for masterID, slaveIDs := range masterToSlaves {
		diff := int32(len(slaveIDs)) - *cluster.Spec.ReplicationFactor
		if diff < 0 {
			// not enough slaves on this master
			if err := c.attachSlavesToMaster(ctx, rCluster, admin, masterID, nodes, -diff); err != nil {
				errs = append(errs, err)
			}
		}
		if diff > 0 {
			var slaveNodes redis.Nodes
			for _, slaveID := range slaveIDs {
				slave, err := rCluster.GetNodeByID(slaveID)
				if err != nil {
					errs = append(errs, err)
				}
				slaveNodes = append(slaveNodes, slave)
			}
			// too many slaves on this master
			if err := c.removeSlavesFromMaster(ctx, admin, cluster, rCluster, masterID, slaveNodes, slaves, diff); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.NewAggregate(errs)
}

func (c *Controller) removeSlavesFromMaster(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, newCluster *redis.Cluster, masterID string, slaves redis.Nodes, allSlaves redis.Nodes, diff int32) error {
	var errs []error
	// select a slave to be removed
	nodesToDelete, err := selectSlavesToDelete(newCluster, masterID, slaves, allSlaves, diff)
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
	return errors.NewAggregate(errs)
}

func (c *Controller) attachSlavesToMaster(ctx context.Context, cluster *redis.Cluster, admin redis.AdminInterface, masterID string, nodes redis.Nodes, diff int32) error {
	var errs []error
	// get the master node
	master, err := nodes.GetNodeByID(masterID)
	if err != nil {
		return err
	}
	// find an available redis nodes to be the new slaves
	slaves, err := getNewSlaves(cluster, nodes, diff)
	if err != nil {
		return err
	}
	for _, slave := range slaves {
		if err = admin.AttachSlaveToMaster(ctx, slave, master); err != nil {
			glog.Errorf("error during reconcileReplicationFactor")
			errs = append(errs, err)
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

func getNewSlaves(cluster *redis.Cluster, nodes redis.Nodes, nbSlavedNeeded int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	currentSlaves := redis.Nodes{}
	candidateSlaves := redis.Nodes{}

	if nbSlavedNeeded <= 0 {
		return selection, nil
	}

	for _, node := range nodes {
		if redis.IsSlave(node) {
			currentSlaves = append(currentSlaves, node)
		}
		if redis.IsMasterWithNoSlot(node) {
			candidateSlaves = append(candidateSlaves, node)
		}
	}
	nodeAdded := false
	for len(selection) < int(nbSlavedNeeded) && !nodeAdded {
		i := 0
		for _, candidate := range candidateSlaves {
			if clustering.ZonesBalanced(cluster, candidate, currentSlaves) {
				nodeAdded = true
				selection = append(selection, candidate)
			} else {
				candidateSlaves[i] = candidate
				i++
			}
			if len(selection) >= int(nbSlavedNeeded) {
				return selection, nil
			}
		}
		candidateSlaves = candidateSlaves[:i]
	}
	if !nodeAdded {
		for _, candidate := range candidateSlaves {
			selection = append(selection, candidate)
			if len(selection) >= int(nbSlavedNeeded) {
				return selection, nil
			}
		}
	}
	return selection, fmt.Errorf("insufficient number of slaves - expected: %v, actual: %v", nbSlavedNeeded, len(selection))
}

func removeSlaveFromZone(slave *redis.Node, zoneToSlaves map[string]redis.Nodes) {
	for zone, slaves := range zoneToSlaves {
		for i, s := range slaves {
			if slave == s {
				zoneToSlaves[zone][i] = zoneToSlaves[zone][len(zoneToSlaves[zone])-1]
				zoneToSlaves[zone] = zoneToSlaves[zone][:len(zoneToSlaves[zone])-1]
				return
			}
		}
	}
}

func selectSlavesToDelete(cluster *redis.Cluster, masterID string, masterSlaves redis.Nodes, allSlaves redis.Nodes, nbSlavesToDelete int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	masterNode, err := cluster.GetNodeByID(masterID)
	if err != nil {
		return selection, err
	}
	for len(selection) < int(nbSlavesToDelete) {
		if removeSlavesByZone(cluster, &selection, masterNode, masterSlaves, allSlaves, nbSlavesToDelete) {
			return selection, nil
		}
	}
	return selection, nil
}

func sameZoneAsMaster(selection *redis.Nodes, master *redis.Node, slaves redis.Nodes, zoneToSlaves map[string]redis.Nodes) bool {
	for _, slave := range slaves {
		if master.Zone == slave.Zone {
			addToSelection(selection, slave, zoneToSlaves)
			return true
		}
	}
	return false
}

func removeSlavesInLargestZone(selection *redis.Nodes, slaves redis.Nodes, zoneToSlaves map[string]redis.Nodes, nbSlavesToDelete int32) bool {
	nodeAdded := false
	largestZoneSize := largestZone(zoneToSlaves)
	for _, slave := range slaves {
		// check if this slave is in the largest zone and there are no other slave zones of equal size
		if len(zoneToSlaves[slave.Zone]) == largestZoneSize && isLargestZone(zoneToSlaves, largestZoneSize) {
			// add it to the selection of slaves to delete and remove it from zoneToSlaves
			nodeAdded = true
			addToSelection(selection, slave, zoneToSlaves)
			if len(*selection) == int(nbSlavesToDelete) {
				break
			}
		}
	}
	return nodeAdded
}

func removeSlavesByZone(cluster *redis.Cluster, selection *redis.Nodes, master *redis.Node, masterSlaves redis.Nodes, allSlaves redis.Nodes, nbSlavesToDelete int32) bool {
	zoneToSlaves := clustering.ZoneToNodes(cluster, masterSlaves)
	nodeAdded := removeSlavesInLargestZone(selection, masterSlaves, zoneToSlaves, nbSlavesToDelete)
	if len(*selection) == int(nbSlavesToDelete) {
		return true
	}
	if !nodeAdded {
		// all slaves are in zones of equal size
		if sameZoneAsMaster(selection, master, masterSlaves, zoneToSlaves) {
			if len(*selection) == int(nbSlavesToDelete) {
				return true
			}
		}
		// remove slaves that have been selected to be deleted from the list of all slaves
		i := 0
		for _, slave := range allSlaves {
			if !selectionContainsSlave(selection, slave) {
				allSlaves[i] = slave
				i++
			}
		}
		allSlaves = allSlaves[:i]
		// remove the slave in the largest zone cluster-wide
		zoneToAllSlaves := clustering.ZoneToNodes(cluster, allSlaves)
		removeSlavesInLargestZone(selection, masterSlaves, zoneToAllSlaves, nbSlavesToDelete)
		if len(*selection) == int(nbSlavesToDelete) {
			return true
		}
		// if we still do not have enough slaves, pick the first available
		for _, slave := range masterSlaves {
			if !selectionContainsSlave(selection, slave) {
				addToSelection(selection, slave, zoneToSlaves)
				if len(*selection) == int(nbSlavesToDelete) {
					return true
				}
			}
		}
	}
	return false
}

func selectionContainsSlave(selection *redis.Nodes, slave *redis.Node) bool {
	for _, s := range *selection {
		if slave == s {
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

func addToSelection(selection *redis.Nodes, slave *redis.Node, zoneToSlaves map[string]redis.Nodes) {
	*selection = append(*selection, slave)
	removeSlaveFromZone(slave, zoneToSlaves)
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
	cNbMaster := *cluster.Spec.NumberOfMaster

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

	// First, we select the new masters
	newMasters, curMasters, allMasters, err := clustering.SelectMasters(newCluster, nodes, cNbMaster)
	if err != nil {
		glog.Errorf("cannot dispatch slots to masters: %v", err)
		newCluster.Status = rapi.ClusterStatusKO
		return false, err
	}
	if len(newMasters) != len(curMasters) {
		hasChanged = true
	}

	// Second, select slave nodes
	currentSlaveNodes := nodes.FilterByFunc(redis.IsSlave)

	// New slaves are currently masters with no slots
	newSlaves := nodes.FilterByFunc(func(nodeA *redis.Node) bool {
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
		if err := clustering.DispatchSlotsToNewMasters(ctx, newCluster, admin, newMasters, curMasters, allMasters); err != nil {
			glog.Errorf("unable to dispatch slot on new master: %v", err)
			return false, err
		}

		// assign master/slave roles
		newRedisSlavesByMaster, err := clustering.PlaceSlaves(newCluster, newMasters, currentSlaveNodes, newSlaves, cReplicationFactor)
		if err != nil {
			glog.Errorf("unable to place slaves: %v", err)
			return false, err
		}

		if err := clustering.AttachSlavesToMaster(ctx, newCluster, admin, newRedisSlavesByMaster); err != nil {
			glog.Errorf("unable to dispatch slave on new master: %v", err)
			return false, err
		}
	} else {
		// scaling up the number of masters or the number of masters hasn't changed
		// assign master/slave roles
		newRedisSlavesByMaster, err := clustering.PlaceSlaves(newCluster, newMasters, currentSlaveNodes, newSlaves, cReplicationFactor)
		if err != nil {
			glog.Errorf("unable to place slaves: %v", err)
			return false, err
		}

		if err := clustering.AttachSlavesToMaster(ctx, newCluster, admin, newRedisSlavesByMaster); err != nil {
			glog.Errorf("unable to dispatch slave on new master: %v", err)
			return false, err
		}

		if err := clustering.DispatchSlotsToNewMasters(ctx, newCluster, admin, newMasters, curMasters, allMasters); err != nil {
			glog.Errorf("unable to dispatch slot on new master: %v", err)
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
