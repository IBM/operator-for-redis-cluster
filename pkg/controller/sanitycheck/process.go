package sanitycheck

import (
	"context"
	"time"

	"github.com/golang/glog"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/config"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/pod"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
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
		glog.V(2).Infof("FixFailedNodes done an action on the cluster (dryRun:%v)", dryRun)
		return actionDone, nil
	}

	// forget nodes and delete pods when a redis node is untrusted.
	if actionDone, err = FixUntrustedNodes(ctx, admin, podControl, cluster, infos, dryRun); err != nil {
		return actionDone, err
	} else if actionDone {
		glog.V(2).Infof("FixUntrustedNodes done an action on the cluster (dryRun:%v)", dryRun)
		return actionDone, nil
	}

	// delete pods that are stuck in terminating state
	if actionDone, err = FixTerminatingPods(cluster, podControl, 5*time.Minute, dryRun); err != nil {
		return actionDone, err
	} else if actionDone {
		glog.V(2).Infof("FixTerminatingPods done an action on the cluster (dryRun:%v)", dryRun)
		return actionDone, nil
	}

	// detect and fix cluster split
	if actionDone, err = FixClusterSplit(ctx, admin, config, infos, dryRun); err != nil {
		return actionDone, err
	} else if actionDone {
		glog.V(2).Infof("FixClusterSplit done an action on the cluster (dryRun:%v)", dryRun)
		return actionDone, nil
	}

	return actionDone, err
}
