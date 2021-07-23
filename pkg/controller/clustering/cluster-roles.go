package clustering

import (
	"context"

	"k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
	"github.com/golang/glog"
)

// ClassifyNodesByRole use to classify the Nodes by roles
func ClassifyNodesByRole(nodes redis.Nodes) (primaries, replicas, primariesWithNoSlots redis.Nodes) {
	primaries = redis.Nodes{}
	replicas = redis.Nodes{}
	primariesWithNoSlots = redis.Nodes{}

	for _, node := range nodes {
		if redis.IsPrimaryWithSlot(node) {
			primaries = append(primaries, node)
		} else if redis.IsReplica(node) {
			replicas = append(replicas, node)
		} else if redis.IsPrimaryWithNoSlot(node) {
			primariesWithNoSlots = append(primariesWithNoSlots, node)
		}
	}
	return primaries, replicas, primariesWithNoSlots
}

// DispatchReplica aims to dispatch the available redis to replica of the current primaries
func DispatchReplica(ctx context.Context, cluster *redis.Cluster, nodes redis.Nodes, replicationFactor int32, admin redis.AdminInterface) error {
	currentPrimaryNodes, currentReplicaNodes, futureReplicaNodes := ClassifyNodesByRole(nodes)
	glog.Infof("current primaries: %v, current replicas: %v, future replicas: %v", currentPrimaryNodes, currentReplicaNodes, futureReplicaNodes)
	primaryToReplicas, unusedReplicas := GeneratePrimaryToReplicas(currentPrimaryNodes, currentReplicaNodes, replicationFactor)
	err := PlaceReplicas(cluster, primaryToReplicas, RemoveOldReplicas(currentReplicaNodes, futureReplicaNodes), unusedReplicas, replicationFactor)
	if err != nil {
		return err
	}
	cluster.NodesPlacement = rapi.NodesPlacementInfoOptimal
	if len(primaryToReplicas) > 0 {
		return nil
	}
	return AttachReplicasToPrimary(ctx, cluster, admin, primaryToReplicas)
}

// AttachReplicasToPrimary used to attach replicas to their primaries
func AttachReplicasToPrimary(ctx context.Context, cluster *redis.Cluster, admin redis.AdminInterface, primaryToReplicas map[string]redis.Nodes) error {
	var errs []error
	for primaryID, replicas := range primaryToReplicas {
		primaryNode, err := cluster.GetNodeByID(primaryID)
		if err != nil {
			glog.Errorf("[AttachReplicasToPrimary] unable fo found the Cluster.Node with redis ID:%s", primaryID)
			continue
		}
		for _, replica := range replicas {
			glog.V(2).Infof("[AttachReplicasToPrimary] Attaching node %s to primary %s", replica.ID, primaryID)
			err = admin.AttachReplicaToPrimary(ctx, replica, primaryNode)
			if err != nil {
				glog.Errorf("Error while attaching node %s to primary %s: %v", replica.ID, primaryID, err)
				errs = append(errs, err)
			}
		}
	}
	return errors.NewAggregate(errs)
}

// DispatchReplicasToNewPrimaries use to dispatch available Nodes as replica to Primary in the case of rolling update
func DispatchReplicasToNewPrimaries(ctx context.Context, newPrimaryNodesSlice, oldReplicaNodesSlice, newReplicaNodesSlice redis.Nodes, replicationLevel int32, admin redis.AdminInterface) error {
	glog.V(3).Info("DispatchReplicasToNewPrimaries start")
	var err error
	replicasByPrimary := make(map[string]redis.Nodes)
	primaryByID := make(map[string]*redis.Node)

	for _, node := range newPrimaryNodesSlice {
		replicasByPrimary[node.ID] = redis.Nodes{}
		primaryByID[node.ID] = node
	}

	for _, replica := range oldReplicaNodesSlice {
		for _, primary := range newPrimaryNodesSlice {
			if replica.PrimaryReferent == primary.ID {
				//The primary of this replica is among the new primary nodes
				replicasByPrimary[replica.PrimaryReferent] = append(replicasByPrimary[replica.PrimaryReferent], replica)
				break
			}
		}
	}
	for _, replica := range newReplicaNodesSlice {
		selectedPrimary := ""
		minReplicaNumber := int32(200) // max replica replication level
		for id, nodes := range replicasByPrimary {
			len := int32(len(nodes))
			if len == replicationLevel {
				continue
			}
			if len < minReplicaNumber {
				selectedPrimary = id
				minReplicaNumber = len
			}
		}
		if selectedPrimary != "" {
			glog.V(2).Infof("Attaching node %s to primary %s", replica.ID, selectedPrimary)
			if err2 := admin.AttachReplicaToPrimary(ctx, replica, primaryByID[selectedPrimary]); err2 != nil {
				glog.Errorf("Error while attaching node %s to primary %s: %v", replica.ID, selectedPrimary, err)
				break
			}
			replicasByPrimary[selectedPrimary] = append(replicasByPrimary[selectedPrimary], replica)
		} else {
			glog.V(2).Infof("No primary found to attach for new replica : %s", replica.ID)
		}
	}
	return err
}

// DispatchReplicaByPrimary use to dispatch available Nodes as replica to Primary
func DispatchReplicaByPrimary(ctx context.Context, futurPrimaryNodes, currentReplicaNodes, futureReplicaNodes redis.Nodes, replicationLevel int32, admin redis.AdminInterface) error {

	var err error

	glog.Infof("Attaching %d replicas per primary, with %d primaries, %d replicas, %d unassigned", replicationLevel, len(futurPrimaryNodes), len(currentReplicaNodes), len(futureReplicaNodes))

	replicasByPrimary := make(map[string]redis.Nodes)
	primaryByID := make(map[string]*redis.Node)

	for _, node := range futurPrimaryNodes {
		replicasByPrimary[node.ID] = redis.Nodes{}
		primaryByID[node.ID] = node
	}

	for _, node := range currentReplicaNodes {
		replicasByPrimary[node.PrimaryReferent] = append(replicasByPrimary[node.PrimaryReferent], node)
	}

	for id, replicas := range replicasByPrimary {
		// detach replicas that are linked to a node without slots
		if _, err = futureReplicaNodes.GetNodeByID(id); err == nil {
			glog.Infof("Loosing primary role: following replicas previously attached to '%s', will be reassigned: %s", id, replicas)
			futureReplicaNodes = append(futureReplicaNodes, replicas...)
			delete(replicasByPrimary, id)
			continue
		}
		// if too many replicas on a primary, make them available
		len := int32(len(replicas))
		if len > replicationLevel {
			glog.Infof("Too many replicas: following replicas previously attached to '%s', will be reassigned: %s", id, replicas[:len-replicationLevel])
			futureReplicaNodes = append(futureReplicaNodes, replicas[:len-replicationLevel]...)
			replicasByPrimary[id] = replicas[len-replicationLevel:]
		}
	}

	for _, replica := range futureReplicaNodes {
		selectedPrimary := ""
		minLevel := int32(200) // max replica replication level
		for id, nodes := range replicasByPrimary {
			len := int32(len(nodes))
			if len == replicationLevel {
				continue
			}
			if len < minLevel {
				selectedPrimary = id
				minLevel = len
			}
		}
		if selectedPrimary != "" {
			glog.V(2).Infof("Attaching node %s to primary %s", replica.ID, selectedPrimary)
			err = admin.AttachReplicaToPrimary(ctx, replica, primaryByID[selectedPrimary])
			if err != nil {
				glog.Errorf("Error while attaching node %s to primary %s: %v", replica.ID, selectedPrimary, err)
				break
			}
			replicasByPrimary[selectedPrimary] = append(replicasByPrimary[selectedPrimary], replica)
		}
	}
	return err
}
