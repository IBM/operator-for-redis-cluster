package sanitycheck

import (
	"context"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/pod"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

// FixGhostPrimaryNodes used to remove ghost redis nodes
func FixGhostPrimaryNodes(ctx context.Context, admin redis.AdminInterface, podControl pod.RedisClusterControlInterface, cluster *rapi.RedisCluster, info *redis.ClusterInfos) (bool, error) {
	ghosts := listGhostPrimaryNodes(podControl, cluster, info)
	var errs []error
	doneAnAction := false
	for _, nodeID := range ghosts {
		doneAnAction = true
		glog.Infof("forget ghost primary nodes with no slot, id:%s", nodeID)

		if err := admin.ForgetNode(ctx, nodeID); err != nil {
			errs = append(errs, err)
		}
	}

	return doneAnAction, errors.NewAggregate(errs)
}

func listGhostPrimaryNodes(podControl pod.RedisClusterControlInterface, cluster *rapi.RedisCluster, infos *redis.ClusterInfos) []string {
	if infos == nil || infos.Infos == nil {
		return []string{}
	}

	ghostNodes := make(map[string]*redis.Node) // map by id is used to dedouble Node from the different view
	for _, nodeinfos := range infos.Infos {
		for _, node := range nodeinfos.Friends.FilterByFunc(redis.IsPrimaryWithNoSlot) {
			ghostNodes[node.ID] = node
		}
	}

	currentPods, err := podControl.GetRedisClusterPods(cluster)
	if err != nil {
		glog.Errorf("unable to retrieve the Pod list, err:%v", err)
	}

	ghosts := []string{}
	for id := range ghostNodes {
		podExist := false
		podReused := false
		// Check if the Redis primary nodes (with not slot) is still running in a Pod
		// if not it will be added to the ghosts redis node slice
		bomNode, _ := infos.GetNodes().GetNodeByID(id)
		if bomNode != nil && bomNode.Pod != nil {
			podExist, podReused = checkIfPodNameExistAndIsReused(bomNode, currentPods)
		}

		if !podExist || podReused {
			ghosts = append(ghosts, id)
		}
	}

	return ghosts
}
