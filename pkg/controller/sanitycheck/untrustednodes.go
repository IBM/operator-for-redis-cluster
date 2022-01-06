package sanitycheck

import (
	"context"

	"github.com/golang/glog"

	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller/pod"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

// FixUntrustedNodes used to remove nodes that are not trusted by other nodes.
// It is useful when a node is removed from the cluster (with the FORGET command) but tries to rejoin the cluster.
func FixUntrustedNodes(ctx context.Context, admin redis.AdminInterface, podControl pod.RedisClusterControlInterface, cluster *rapi.RedisCluster, infos *redis.ClusterInfos, dryRun bool) (bool, error) {
	untrustedNode := listUntrustedNodes(infos)
	var errs []error
	doAction := false

	currentPods, err := podControl.GetRedisClusterPods(cluster)
	if err != nil {
		glog.Errorf("unable to retrieve the Pod list, err:%v", err)
	}

	for id, uNode := range untrustedNode {
		getByIPFunc := func(n *redis.Node) bool {
			if n.IP == uNode.IP && n.ID != uNode.ID {
				return true
			}
			return false
		}
		node2, err := infos.GetNodes().GetNodesByFunc(getByIPFunc)
		if err != nil && !redis.IsNodeNotFoundedError(err) {
			glog.Errorf("Error with GetNodesByFunc(getByIPFunc) search function")
			errs = append(errs, err)
			continue
		}
		if len(node2) > 0 {
			// pod is used by another Redis node ID, so we should not delete the pod
			continue
		}
		exist, reused := checkIfPodNameExistsAndIsReused(uNode, currentPods)
		if exist && !reused {
			if err := podControl.DeletePod(cluster, uNode.Pod.Name); err != nil {
				errs = append(errs, err)
			}
		}
		doAction = true
		if !dryRun {
			if err := admin.ForgetNode(ctx, id); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return doAction, errors.NewAggregate(errs)
}

func listUntrustedNodes(infos *redis.ClusterInfos) map[string]*redis.Node {
	untrustedNodes := make(map[string]*redis.Node)
	if infos == nil || infos.Infos == nil {
		return untrustedNodes
	}
	for _, nodeInfos := range infos.Infos {
		for _, node := range nodeInfos.Friends {
			// only forget it when no more part of kubernetes, or if noaddress
			if node.HasStatus(redis.NodeStatusHandshake) {
				if _, found := untrustedNodes[node.ID]; !found {
					untrustedNodes[node.ID] = node
				}
			}
		}
	}
	return untrustedNodes
}

func checkIfPodNameExistsAndIsReused(node *redis.Node, podlist []kapi.Pod) (exist bool, reused bool) {
	if node.Pod == nil {
		return exist, reused
	}
	for _, currentPod := range podlist {
		if currentPod.Name == node.Pod.Name {
			exist = true
			if currentPod.Status.PodIP == node.Pod.Status.PodIP {
				// This check is used to see if the pod name is used by another redis node.
				// We compare the IP of the current pod with the pod from the cluster bom.
				// If the pod IP/name from the redis info equals the IP/name from the getPod, it
				// means the pod is still in use and the redis node is not a ghost
				reused = true
				break
			}
		}
	}
	return exist, reused
}
