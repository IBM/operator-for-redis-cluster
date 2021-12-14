package clustering

import (
	"context"

	"k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
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
			glog.Errorf("[AttachReplicasToPrimary] unable to find node with id: %s", primaryID)
			continue
		}
		for _, replica := range replicas {
			glog.V(2).Infof("[AttachReplicasToPrimary] attaching node %s to primary %s", replica.ID, primaryID)
			err = admin.AttachReplicaToPrimary(ctx, replica, primaryNode)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.NewAggregate(errs)
}
