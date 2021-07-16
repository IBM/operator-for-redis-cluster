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

// PlacePrimaries selects primary redis nodes by spreading out the primaries across zones as much as possible.
func PlacePrimaries(cluster *redis.Cluster, currentPrimaries, candidatePrimaries redis.Nodes, nbPrimary int32) (redis.Nodes, bool, error) {
	selection := redis.Nodes{}
	selection = append(selection, currentPrimaries...)
	zones := cluster.GetZones()

	// in case of scale down, we use the required number of primaries instead of
	// current number of primaries to limit the size of the selection
	if len(selection) > int(nbPrimary) {
		selectedPrimaries, err := selectPrimariesByZone(zones, selection, nbPrimary)
		if err != nil {
			glog.Errorf("Error selecting primaries by zone: %v", err)
		}
		selection = selectedPrimaries
	}

	nodeToCurrentPrimaries := k8sNodeToRedisNodes(cluster, currentPrimaries)
	nodeToCandidatePrimaries := k8sNodeToRedisNodes(cluster, candidatePrimaries)

	return addPrimaries(zones, selection, nodeToCurrentPrimaries, nodeToCandidatePrimaries, nbPrimary)
}

func getOptimalNodeIndex(zones []string, nodes, candidates redis.Nodes) int {
	// iterate over candidate nodes and choose one that preserves zone balance
	for i, candidate := range candidates {
		if ZonesBalanced(zones, candidate, nodes) {
			glog.V(4).Infof("- add node: %s to the selection", candidate.ID)
			return i
		}
	}
	return -1
}

func selectOptimalPrimaries(bestEffort bool, zones []string, selection redis.Nodes, nodeToCurrentPrimaries map[string]redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) (bool, redis.Nodes) {
	optimalPrimaries := redis.Nodes{}
	nodeAdded := false
	for nodeID, candidates := range nodeToCandidatePrimaries {
		// discard kubernetes nodes with primaries when we are not in best effort mode
		if _, ok := nodeToCurrentPrimaries[nodeID]; !bestEffort && ok {
			continue
		}
		index := getOptimalNodeIndex(zones, selection, candidates)
		if index != -1 {
			nodeAdded = true
			optimalPrimaries = append(optimalPrimaries, candidates[index])
			candidates[index] = candidates[len(candidates)-1]
			nodeToCandidatePrimaries[nodeID] = candidates[:len(candidates)-1]
			if len(selection)+len(optimalPrimaries) >= int(nbPrimary) {
				return nodeAdded, optimalPrimaries
			}
		}
	}
	return nodeAdded, optimalPrimaries
}

func selectBestEffortPrimaries(selection redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) redis.Nodes {
	bestEffortPrimaries := redis.Nodes{}
	for _, candidates := range nodeToCandidatePrimaries {
		for _, candidate := range candidates {
			glog.V(4).Infof("- add node: %s to the selection", candidate.ID)
			bestEffortPrimaries = append(bestEffortPrimaries, candidate)
			if len(selection)+len(bestEffortPrimaries) >= int(nbPrimary) {
				return bestEffortPrimaries
			}
		}
	}
	return bestEffortPrimaries
}

func selectNewPrimaries(zones []string, selection redis.Nodes, nodeToCurrentPrimaries map[string]redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) (redis.Nodes, bool) {
	bestEffort := false
	// iterate until we have the requested number of primaries
	for len(selection) < int(nbPrimary) {
		// select optimal primaries that preserve zone balance
		nodeAdded, optimalSelection := selectOptimalPrimaries(bestEffort, zones, selection, nodeToCurrentPrimaries, nodeToCandidatePrimaries, nbPrimary)
		selection = append(selection, optimalSelection...)
		if len(selection) >= int(nbPrimary) {
			return selection, bestEffort
		}
		if bestEffort && !nodeAdded {
			glog.Infof("no remaining optimal primaries, entering best effort mode")
			// select any available primaries
			bestEffortSelection := selectBestEffortPrimaries(selection, nodeToCandidatePrimaries, nbPrimary)
			selection = append(selection, bestEffortSelection...)
			break
		}
		bestEffort = true
	}
	return selection, bestEffort
}

func addPrimaries(zones []string, primaries redis.Nodes, nodeToCurrentPrimaries map[string]redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) (redis.Nodes, bool, error) {
	newPrimaries, bestEffort := selectNewPrimaries(zones, primaries, nodeToCurrentPrimaries, nodeToCandidatePrimaries, nbPrimary)
	glog.Infof("- bestEffort %v", bestEffort)
	for _, primary := range newPrimaries {
		glog.Infof("- primary %s, ip: %s", primary.ID, primary.IP)
	}
	if len(newPrimaries) >= int(nbPrimary) {
		return newPrimaries, bestEffort, nil
	}
	return newPrimaries, bestEffort, fmt.Errorf("insufficient number of redis nodes for the requested number of primaries")
}

func ZoneToNodes(zones []string, nodes redis.Nodes) map[string]redis.Nodes {
	zoneToNodes := make(map[string]redis.Nodes)
	for _, zone := range zones {
		zoneToNodes[zone] = redis.Nodes{}
	}
	for _, node := range nodes {
		zoneToNodes[node.Zone] = append(zoneToNodes[node.Zone], node)
	}
	return zoneToNodes
}

func selectPrimariesByZone(zones []string, primaries redis.Nodes, nbPrimary int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	zoneToNodes := ZoneToNodes(zones, primaries)
	for len(selection) < int(nbPrimary) {
		for zone, nodes := range zoneToNodes {
			if len(nodes) > 0 {
				selection = append(selection, nodes[0])
				zoneToNodes[zone] = nodes[1:]
			}
			if len(selection) >= int(nbPrimary) {
				return selection, nil
			}
		}
	}
	return selection, fmt.Errorf("insufficient number of redis nodes for the requested number of primaries")
}

func ZonesBalanced(zones []string, candidate *redis.Node, nodes redis.Nodes) bool {
	if len(nodes) == 0 {
		return true
	}
	zoneToNodes := ZoneToNodes(zones, nodes)
	smallestZoneSize := math.MaxInt32
	for _, zoneNodes := range zoneToNodes {
		if len(zoneNodes) < smallestZoneSize {
			smallestZoneSize = len(zoneNodes)
		}
	}
	return len(zoneToNodes[candidate.Zone]) <= smallestZoneSize
}

// PlaceReplicas selects replica redis nodes for each primary by spreading out the replicas across zones as much as possible.
func PlaceReplicas(cluster *redis.Cluster, primaryToReplicas map[string]redis.Nodes, newReplicas, unusedReplicas redis.Nodes, replicationFactor int32) error {
	zoneToReplicas := ZoneToNodes(cluster.GetZones(), newReplicas)
	addReplicasToZones(zoneToReplicas, unusedReplicas)
	return addReplicasToPrimariesByZone(cluster, primaryToReplicas, zoneToReplicas, replicationFactor)
}

func containsReplica(replicas redis.Nodes, replica *redis.Node) bool {
	for _, r := range replicas {
		if r.ID == replica.ID {
			return true
		}
	}
	return false
}

func RemoveOldReplicas(oldReplicas, newReplicas redis.Nodes) redis.Nodes {
	// be sure that no oldReplicas are present in newReplicas
	i := 0
	for _, newReplica := range newReplicas {
		if containsReplica(oldReplicas, newReplica) {
			glog.V(4).Infof("removing old replica with id %s from list of new replicas", newReplica.ID)
		} else {
			newReplicas[i] = newReplica
			i++
		}
	}
	return newReplicas[:i]
}

func GeneratePrimaryToReplicas(primaries, replicas redis.Nodes, replicationFactor int32) (map[string]redis.Nodes, redis.Nodes) {
	primaryToReplicas := make(map[string]redis.Nodes)
	unusedReplicas := redis.Nodes{}
	for _, node := range primaries {
		primaryToReplicas[node.ID] = redis.Nodes{}
	}
	// build primary to replicas map and save all unused replicas
	for _, replica := range replicas {
		nodeAdded := false
		for _, primary := range primaries {
			if replica.PrimaryReferent == primary.ID {
				if len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
					// primary of this replica is among the new primary nodes
					primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], replica)
					nodeAdded = true
				}
				break
			}
		}
		if !nodeAdded {
			unusedReplicas = append(unusedReplicas, replica)
		}
	}
	return primaryToReplicas, unusedReplicas
}

func addReplicasToPrimariesByZone(cluster *redis.Cluster, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) error {
	zones := cluster.GetZones()
	for primaryID, currentReplicas := range primaryToReplicas {
		if len(currentReplicas) < int(replicationFactor) {
			primary, err := cluster.GetNodeByID(primaryID)
			if err != nil {
				glog.Errorf("unable to find primary with id: %s", primaryID)
				break
			}
			err = addReplicasToPrimary(zones, primary, primaryToReplicas, zoneToReplicas, replicationFactor)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func SameZone(zone string, nodes redis.Nodes) bool {
	for _, node := range nodes {
		if zone == node.Zone {
			return true
		}
	}
	return false
}

func selectOptimalReplicas(zones []string, primary *redis.Node, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) bool {
	zoneIndex := GetZoneIndex(zones, primary.Zone, primaryToReplicas[primary.ID])
	nodeAdded := false
	i := 0
	for i < len(zones) && len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		zone := zones[zoneIndex]
		zoneReplicas := zoneToReplicas[zone]
		zoneIndex = (zoneIndex + 1) % len(zones)
		if len(zoneReplicas) > 0 {
			// if RF < # of zones, we can achieve optimal placement
			if int(replicationFactor) < len(zones) {
				// skip this zone if it is the same as the primary zone or if this primary already has replicas in this zone
				if primary.Zone == zone || SameZone(zone, primaryToReplicas[primary.ID]) {
					continue
				}
			}
			nodeAdded = true
			glog.V(4).Infof("adding replica %s to primary %s", zoneReplicas[0].ID, primary.ID)
			primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], zoneReplicas[0])
			zoneToReplicas[zone] = zoneReplicas[1:]
		}
		i++
	}
	return nodeAdded
}

func selectBestEffortReplicas(zones []string, primary *redis.Node, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) {
	zoneIndex := GetZoneIndex(zones, primary.Zone, primaryToReplicas[primary.ID])
	numEmptyZones := 0
	// iterate while zones are non-empty and the number of replicas is less than RF
	for numEmptyZones < len(zones) && len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		zone := zones[zoneIndex]
		zoneReplicas := zoneToReplicas[zone]
		zoneIndex = (zoneIndex + 1) % len(zones)
		if len(zoneReplicas) > 0 {
			glog.V(4).Infof("adding replica %s to primary %s", zoneReplicas[0].ID, primary.ID)
			primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], zoneReplicas[0])
			zoneToReplicas[zone] = zoneReplicas[1:]
		} else {
			numEmptyZones++
		}
	}
}

func addReplicasToPrimary(zones []string, primary *redis.Node, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) error {
	// iterate while the number of replicas is less than RF
	for len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		if !selectOptimalReplicas(zones, primary, primaryToReplicas, zoneToReplicas, replicationFactor) && len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
			glog.Infof("no remaining optimal replicas, entering best effort mode")
			selectBestEffortReplicas(zones, primary, primaryToReplicas, zoneToReplicas, replicationFactor)
			break
		}
	}
	if len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		return fmt.Errorf("insufficient number of replicas for primary %s, expected: %v, actual: %v", primary.ID, int(replicationFactor), len(primaryToReplicas[primary.ID]))
	}
	return nil
}

func addReplicasToZones(zoneToReplicas map[string]redis.Nodes, replicas redis.Nodes) {
	for _, replica := range replicas {
		zoneToReplicas[replica.Zone] = append(zoneToReplicas[replica.Zone], replica)
	}
}

func GetZoneIndex(zones []string, primaryZone string, replicas redis.Nodes) int {
	for i, zone := range zones {
		if primaryZone == zone {
			return (i + len(replicas) + 1) % len(zones)
		}
	}
	return 0
}

// k8sNodeToRedisNodes returns a mapping from kubernetes nodes to a list of redis nodes that will be hosted on the node
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
