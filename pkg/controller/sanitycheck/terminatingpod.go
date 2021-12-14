package sanitycheck

import (
	"time"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller/pod"
)

// FixTerminatingPods used for the deletion of pods blocked in terminating status.
func FixTerminatingPods(cluster *rapi.RedisCluster, podControl pod.RedisClusterControlInterface, maxDuration time.Duration, dryRun bool) (bool, error) {
	var errs []error
	var actionDone bool

	if maxDuration == time.Duration(0) {
		return actionDone, nil
	}

	currentPods, err := podControl.GetRedisClusterPods(cluster)
	if err != nil {
		glog.Errorf("unable to retrieve the Pod list, err:%v", err)
	}

	now := time.Now()
	for _, p := range currentPods {
		if p.DeletionTimestamp == nil {
			// ignore pod without deletion timestamp
			continue
		}
		maxTime := p.DeletionTimestamp.Add(maxDuration) // adding MaxDuration for configuration
		if maxTime.Before(now) {
			actionDone = true
			// it means that this pod should already been deleted since a wild
			if !dryRun {
				if err := podControl.DeletePod(cluster, p.Name); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}

	return actionDone, errors.NewAggregate(errs)
}
