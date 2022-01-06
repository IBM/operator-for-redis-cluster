package sanitycheck

import (
	"context"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

// FixFailedNodes fix failed nodes: in some cases (cluster without enough primary after crash or scale down), some nodes may still know about fail nodes
func FixFailedNodes(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, infos *redis.ClusterInfos, dryRun bool) (bool, error) {
	forgetSet := listGhostNodes(cluster, infos)
	var errs []error
	doneAnAction := false
	for id := range forgetSet {
		doneAnAction = true
		glog.Infof("Sanitychecks: Forgetting failed node %s. This command might fail, but it is not an error", id)
		if !dryRun {
			if err := admin.ForgetNode(ctx, id); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return doneAnAction, errors.NewAggregate(errs)
}

// listGhostNodes: a ghost node is a failed node still known by some redis nodes,
// meaning it is no longer in kubernetes and should be forgotten
func listGhostNodes(cluster *rapi.RedisCluster, infos *redis.ClusterInfos) map[string]bool {
	ghostNodesSet := map[string]bool{}
	if infos == nil || infos.Infos == nil {
		return ghostNodesSet
	}
	for _, nodeinfos := range infos.Infos {
		for _, node := range nodeinfos.Friends {
			// forget it when it no longer has an IP address, or is in a failure state
			if node.HasStatus(redis.NodeStatusNoAddr) {
				ghostNodesSet[node.ID] = true
			}
			if node.HasStatus(redis.NodeStatusFail) || node.HasStatus(redis.NodeStatusPFail) {
				found := false
				for _, pod := range cluster.Status.Cluster.Nodes {
					if pod.ID == node.ID {
						found = true
					}
				}
				if !found {
					ghostNodesSet[node.ID] = true
				}
			}
		}
	}
	return ghostNodesSet
}
