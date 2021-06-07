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
func PlacePrimaries(cluster *redis.Cluster, currentPrimaries redis.Nodes, candidatePrimaries redis.Nodes, nbPrimary int32) (redis.Nodes, bool, error) {
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

	nodeToCandidatePrimaries := k8sNodeToRedisNodes(cluster, candidatePrimaries)
	nodeToCurrentPrimaries := k8sNodeToRedisNodes(cluster, currentPrimaries)

	return addPrimaries(zones, selection, nodeToCurrentPrimaries, nodeToCandidatePrimaries, nbPrimary)
}

func addPrimaries(zones []string, primaries redis.Nodes, nodeToCurrentPrimaries map[string]redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) (redis.Nodes, bool, error) {
	bestEffort := false
	// iterate until we have the requested number of primaries or we reach best effort
	for len(primaries) < int(nbPrimary) {
		nodeAdded := false
		for node, candidates := range nodeToCandidatePrimaries {
			// discard kubernetes nodes with primaries when we are not in best effort mode
			if _, ok := nodeToCurrentPrimaries[node]; !bestEffort && ok {
				continue
			}
			// iterate over candidate primaries and choose one that preserves zone balance between primary nodes
			for i, candidate := range candidates {
				if ZonesBalanced(zones, candidate, primaries) {
					glog.Infof("- add node: %s to the primary selection", candidate.ID)
					// Add the candidate to the primaries list
					primaries = append(primaries, candidate)
					nodeAdded = true
					// Remove the candidate primary from the candidates list
					candidates[i] = candidates[len(candidates)-1]
					nodeToCandidatePrimaries[node] = candidates[:len(candidates)-1]
					if len(primaries) >= int(nbPrimary) {
						return primaries, bestEffort, nil
					}
					break
				}
			}
		}
		if bestEffort && !nodeAdded {
			glog.Errorf("nothing added since last loop, no more primaries are available")
			break
		}
		bestEffort = true
		if glog.V(4) {
			glog.Warning("pods are not sufficiently distributed to have only one primary per node")
		}
	}
	glog.Infof("- bestEffort %v", bestEffort)
	for _, primary := range primaries {
		glog.Infof("- Primary %s, ip:%s", primary.ID, primary.IP)
	}
	if len(primaries) >= int(nbPrimary) {
		return primaries, bestEffort, nil
	}
	return primaries, bestEffort, fmt.Errorf("insufficient number of redis nodes for the requested number of primaries")
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

// PlaceReplicas selects replica redis nodes by spreading out the replicas across zones as much as possible.
func PlaceReplicas(cluster *redis.Cluster, primaries, oldReplicas, newReplicas redis.Nodes, replicationFactor int32) (map[string]redis.Nodes, error) {
	removeOldReplicas(&oldReplicas, &newReplicas)
	zoneToReplicas := ZoneToNodes(cluster.GetZones(), newReplicas)
	primaryToReplicas := generatePrimaryToReplicas(primaries, oldReplicas, zoneToReplicas, replicationFactor)
	err := addReplicasToPrimariesByZone(cluster, primaryToReplicas, zoneToReplicas, replicationFactor)
	if err != nil {
		return nil, err
	}
	return primaryToReplicas, nil
}

func removeOldReplicas(oldReplicas, newReplicas *redis.Nodes) {
	// be sure that no oldReplicas are present in newReplicas
	for _, newReplica := range *newReplicas {
		for _, oldReplica := range *oldReplicas {
			if newReplica.ID == oldReplica.ID {
				removeIDFunc := func(node *redis.Node) bool {
					return node.ID == newReplica.ID
				}
				newReplicas.FilterByFunc(removeIDFunc)
				if glog.V(4) {
					glog.Warningf("removing old replica with id %s from list of new replicas", newReplica.ID)
				}
			}
		}
	}
}

func generatePrimaryToReplicas(primaries, replicas redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) map[string]redis.Nodes {
	primaryToReplicas := make(map[string]redis.Nodes)
	for _, node := range primaries {
		primaryToReplicas[node.ID] = redis.Nodes{}
	}
	for _, replica := range replicas {
		for _, primary := range primaries {
			if replica.PrimaryReferent == primary.ID {
				if len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
					// primary of this replica is among the new primary nodes
					primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], replica)
					break
				} else {
					zoneToReplicas[primary.Zone] = append(primaryToReplicas[primary.Zone], replica)
				}
			}
		}
	}
	return primaryToReplicas
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
			err = addReplicasToPrimary(zones, primary, currentReplicas, primaryToReplicas, zoneToReplicas, replicationFactor)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func addReplicasToPrimary(zones []string, primary *redis.Node, currentReplicas redis.Nodes, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) error {
	// calculate zone index for primary node
	zoneIndex := GetZoneIndex(zones, primary.Zone, currentReplicas)
	numEmptyZones := 0
	// iterate while zones are non-empty and the number of replicas is less than RF
	for numEmptyZones < len(zones) && len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		zone := zones[zoneIndex]
		zoneReplicas := zoneToReplicas[zone]
		if len(zoneReplicas) > 0 {
			// append replica to primary and remove from map
			primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], zoneReplicas[0])
			zoneToReplicas[zone] = zoneReplicas[1:]
		} else {
			numEmptyZones++
		}
		zoneIndex = (zoneIndex + 1) % len(zones)
	}
	if len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		return fmt.Errorf("insufficient number of replicas for primary %s, expected: %v, actual: %v", primary.ID, int(replicationFactor), len(primaryToReplicas[primary.ID]))
	}
	return nil
}

func GetZoneIndex(zones []string, primaryZone string, replicas redis.Nodes) int {
	for i, zone := range zones {
		if primaryZone == zone {
			return (i + len(replicas) + 1) % len(zones)
		}
	}
	return 0
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
