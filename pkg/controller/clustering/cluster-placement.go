package clustering

import (
	"fmt"
	"math"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
	"github.com/golang/glog"
)

const (
	unknownNodeName = "unknown" // I hope nobody will ever name a node "unknown" because this will impact the algorithm inside this package
)

// PlaceMasters selects master redis nodes by spreading out the masters across zones as much as possible.
func PlaceMasters(cluster *redis.Cluster, currentMasters redis.Nodes, candidateMasters redis.Nodes, nbMaster int32) (redis.Nodes, bool, error) {
	selection := redis.Nodes{}
	selection = append(selection, currentMasters...)

	// in case of scale down, we use the required number of masters instead of
	// current number of masters to limit the size of the selection
	if len(selection) > int(nbMaster) {
		selectedMasters, err := selectMastersByZone(cluster, selection, nbMaster)
		if err != nil {
			glog.Errorf("Error selecting masters by zone: %v", err)
		}
		selection = selectedMasters
	}

	nodeToCandidateMasters := k8sNodeToRedisNodes(cluster, candidateMasters)
	nodeToCurrentMasters := k8sNodeToRedisNodes(cluster, currentMasters)

	return addMasters(cluster, selection, nodeToCurrentMasters, nodeToCandidateMasters, nbMaster)
}

func addMasters(cluster *redis.Cluster, masters redis.Nodes, nodeToCurrentMasters map[string]redis.Nodes, nodeToCandidateMasters map[string]redis.Nodes, nbMaster int32) (redis.Nodes, bool, error) {
	bestEffort := false
	// iterate until we have the requested number of masters or we reach best effort
	for len(masters) < int(nbMaster) {
		nodeAdded := false
		for node, candidates := range nodeToCandidateMasters {
			// discard kubernetes nodes with masters when we are not in best effort mode
			if _, ok := nodeToCurrentMasters[node]; !bestEffort && ok {
				continue
			}
			// iterate over candidate masters and choose one that preserves zone balance between master nodes
			for i, candidate := range candidates {
				if ZonesBalanced(cluster, candidate, masters) {
					glog.Infof("- add node: %s to the master selection", candidate.ID)
					// Add the candidate to the masters list
					masters = append(masters, candidate)
					nodeAdded = true
					// Remove the candidate master from the candidates list
					candidates[i] = candidates[len(candidates)-1]
					nodeToCandidateMasters[node] = candidates[:len(candidates)-1]
					if len(masters) >= int(nbMaster) {
						return masters, bestEffort, nil
					}
					break
				}
			}
		}
		if bestEffort && !nodeAdded {
			glog.Errorf("nothing added since last loop, no more masters are available")
			break
		}
		bestEffort = true
		if glog.V(4) {
			glog.Warning("pods are not sufficiently distributed to have only one master per node")
		}
	}
	glog.Infof("- bestEffort %v", bestEffort)
	for _, master := range masters {
		glog.Infof("- Master %s, ip:%s", master.ID, master.IP)
	}
	if len(masters) >= int(nbMaster) {
		return masters, bestEffort, nil
	}
	return masters, bestEffort, fmt.Errorf("insufficient number of redis nodes for the requested number of masters")
}

func ZoneToNodes(cluster *redis.Cluster, nodes redis.Nodes) map[string]redis.Nodes {
	zones := cluster.GetZones()
	zoneToNodes := make(map[string]redis.Nodes)
	for _, zone := range zones {
		zoneToNodes[zone] = redis.Nodes{}
	}
	for _, node := range nodes {
		zoneToNodes[node.Zone] = append(zoneToNodes[node.Zone], node)
	}
	return zoneToNodes
}

func selectMastersByZone(cluster *redis.Cluster, masters redis.Nodes, nbMaster int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	zoneToNodes := ZoneToNodes(cluster, masters)
	for len(selection) < int(nbMaster) {
		for zone, nodes := range zoneToNodes {
			if len(nodes) > 0 {
				selection = append(selection, nodes[0])
				zoneToNodes[zone] = nodes[1:]
			}
			if len(selection) >= int(nbMaster) {
				return selection, nil
			}
		}
	}
	return selection, fmt.Errorf("insufficient number of redis nodes for the requested number of masters")
}

func ZonesBalanced(cluster *redis.Cluster, candidate *redis.Node, nodes redis.Nodes) bool {
	if len(nodes) == 0 {
		return true
	}
	zoneToNodes := ZoneToNodes(cluster, nodes)
	smallestZoneSize := math.MaxInt32
	for _, zoneNodes := range zoneToNodes {
		if len(zoneNodes) < smallestZoneSize {
			smallestZoneSize = len(zoneNodes)
		}
	}
	return len(zoneToNodes[candidate.Zone]) <= smallestZoneSize
}

// PlaceSlaves selects slave redis nodes by spreading out the slaves across zones as much as possible.
func PlaceSlaves(cluster *redis.Cluster, masters, oldSlaves, newSlaves redis.Nodes, replicationFactor int32) (map[string]redis.Nodes, error) {
	removeOldSlaves(&oldSlaves, &newSlaves)
	zoneToSlaves := ZoneToNodes(cluster, newSlaves)
	masterToSlaves := generateMasterToSlaves(masters, oldSlaves, zoneToSlaves, replicationFactor)
	err := addSlavesToMastersByZone(cluster, masterToSlaves, zoneToSlaves, replicationFactor)
	if err != nil {
		return nil, err
	}
	return masterToSlaves, nil
}

func removeOldSlaves(oldSlaves, newSlaves *redis.Nodes) {
	// be sure that no oldSlaves are present in newSlaves
	for _, newSlave := range *newSlaves {
		for _, oldSlave := range *oldSlaves {
			if newSlave.ID == oldSlave.ID {
				removeIDFunc := func(node *redis.Node) bool {
					return node.ID == newSlave.ID
				}
				newSlaves.FilterByFunc(removeIDFunc)
				if glog.V(4) {
					glog.Warningf("removing old slave with id %s from list of new slaves", newSlave.ID)
				}
			}
		}
	}
}

func generateMasterToSlaves(masters, slaves redis.Nodes, zoneToSlaves map[string]redis.Nodes, replicationFactor int32) map[string]redis.Nodes {
	masterToSlaves := make(map[string]redis.Nodes)
	for _, node := range masters {
		masterToSlaves[node.ID] = redis.Nodes{}
	}
	for _, slave := range slaves {
		for _, master := range masters {
			if slave.MasterReferent == master.ID {
				if len(masterToSlaves[master.ID]) < int(replicationFactor) {
					// master of this slave is among the new master nodes
					masterToSlaves[master.ID] = append(masterToSlaves[master.ID], slave)
					break
				} else {
					zoneToSlaves[master.Zone] = append(masterToSlaves[master.Zone], slave)
				}
			}
		}
	}
	return masterToSlaves
}

func addSlavesToMastersByZone(cluster *redis.Cluster, masterToSlaves map[string]redis.Nodes, zoneToSlaves map[string]redis.Nodes, replicationFactor int32) error {
	zones := cluster.GetZones()
	for masterID, currentSlaves := range masterToSlaves {
		if len(currentSlaves) < int(replicationFactor) {
			master, err := cluster.GetNodeByID(masterID)
			if err != nil {
				glog.Errorf("unable to find master with id: %s", masterID)
				break
			}
			err = addSlavesToMaster(zones, master, currentSlaves, masterToSlaves, zoneToSlaves, replicationFactor)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func addSlavesToMaster(zones []string, master *redis.Node, currentSlaves redis.Nodes, masterToSlaves map[string]redis.Nodes, zoneToSlaves map[string]redis.Nodes, replicationFactor int32) error {
	// calculate zone index for master node
	zoneIndex := 0
	for i, zone := range zones {
		if master.Zone == zone {
			zoneIndex = (i + len(currentSlaves) + 1) % len(zones)
			break
		}
	}
	numEmptyZones := 0
	// iterate while zones are non-empty and the number of slaves is less than RF
	for numEmptyZones < len(zones) && len(masterToSlaves[master.ID]) < int(replicationFactor) {
		zone := zones[zoneIndex]
		zoneSlaves := zoneToSlaves[zone]
		if len(zoneSlaves) > 0 {
			// append slave to master and remove from map
			masterToSlaves[master.ID] = append(masterToSlaves[master.ID], zoneSlaves[0])
			zoneToSlaves[zone] = zoneSlaves[1:]
		} else {
			numEmptyZones++
		}
		zoneIndex = (zoneIndex + 1) % len(zones)
	}
	if len(masterToSlaves[master.ID]) < int(replicationFactor) {
		return fmt.Errorf("insufficient number of slaves for master %s, expected: %v, actual: %v", master.ID, int(replicationFactor), len(masterToSlaves[master.ID]))

	}
	return nil
}

// k8sNodeToRedisNodes returns a mapping from kubernetes nodes to a list of redis nodes that will be hosted on that node
func k8sNodeToRedisNodes(cluster *redis.Cluster, nodes redis.Nodes) map[string]redis.Nodes {
	nodeToRedisNodes := make(map[string]redis.Nodes)
	for _, rnode := range nodes {
		cnode, err := cluster.GetNodeByID(rnode.ID)
		if err != nil {
			glog.Errorf("[k8sNodeToRedisNodes] unable to find the node with redis ID:%s", rnode.ID)
			continue // if not then next line with cnode.Pod will cause a panic since cnode is nil
		}
		nodeName := unknownNodeName
		if cnode.Pod != nil && cnode.Pod.Spec.NodeName != "" {
			nodeName = cnode.Pod.Spec.NodeName
		}
		if _, ok := nodeToRedisNodes[nodeName]; !ok {
			nodeToRedisNodes[nodeName] = redis.Nodes{}
		}
		nodeToRedisNodes[nodeName] = append(nodeToRedisNodes[nodeName], rnode)
	}

	return nodeToRedisNodes
}
