package sanitycheck

import (
	"context"
	"time"

	"github.com/golang/glog"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/config"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller/pod"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

// RunSanityChecks function used to run all the sanity check on the current cluster
// Return actionDone = true if a modification has been made on the cluster
func RunSanityChecks(ctx context.Context, admin redis.AdminInterface, config *config.Redis, podControl pod.RedisClusterControlInterface, cluster *rapi.RedisCluster, infos *redis.ClusterInfos, dryRun bool) (actionDone bool, err error) {
	if cluster.Status.Cluster.Status == rapi.ClusterStatusRollingUpdate {
		return false, nil
	}
	// * fix failed nodes: in some cases (cluster without enough primary after crash or scale down), some nodes may still know about fail nodes
	if actionDone, err = FixFailedNodes(ctx, admin, cluster, infos, dryRun); err != nil {
		return actionDone, err
	} else if actionDone {
		glog.V(2).Infof("FixFailedNodes executed an action on the cluster (dryRun: %v)", dryRun)
		return actionDone, nil
	}

	// forget nodes and delete pods when a redis node is untrusted.
	if actionDone, err = FixUntrustedNodes(ctx, admin, podControl, cluster, infos, dryRun); err != nil {
		return actionDone, err
	} else if actionDone {
		glog.V(2).Infof("FixUntrustedNodes executed an action on the cluster (dryRun: %v)", dryRun)
		return actionDone, nil
	}

	// delete pods that are stuck in terminating state
	if actionDone, err = FixTerminatingPods(cluster, podControl, 1*time.Minute, dryRun); err != nil {
		return actionDone, err
	} else if actionDone {
		glog.V(2).Infof("FixTerminatingPods executed an action on the cluster (dryRun: %v)", dryRun)
		return actionDone, nil
	}

	// detect and fix cluster split
	if actionDone, err = FixClusterSplit(ctx, admin, config, infos, dryRun); err != nil {
		return actionDone, err
	} else if actionDone {
		glog.V(2).Infof("FixClusterSplit executed an action on the cluster (dryRun: %v)", dryRun)
		return actionDone, nil
	}

	// detect and fix zone balance
	if actionDone, err = FixZoneBalance(ctx, admin, cluster, infos, dryRun); err != nil {
		return actionDone, err
	} else if actionDone {
		glog.V(2).Infof("FixZoneBalance executed an action on the cluster (dryRun: %v)", dryRun)
		return actionDone, nil
	}

	return actionDone, err
}
