package testutil

import (
	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	kapiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewRedisPrimaryNode(id, zone, podName, nodeName string, slots []string) (rapi.RedisClusterNode, redis.Node) {
	slotsInt := redis.SlotSlice{}
	for _, s := range slots {
		i, _ := redis.DecodeSlot(s)
		slotsInt = append(slotsInt, i)
	}
	node := newRedisNode(rapi.RedisClusterNodeRolePrimary, id, zone, podName, nodeName, slots)
	redisNode := redis.Node{ID: id, Zone: zone, Slots: slotsInt, Role: string(rapi.RedisClusterNodeRolePrimary), Pod: node.Pod}
	return node, redisNode
}

func NewRedisReplicaNode(id, zone, primaryRef, podName, nodeName string) (rapi.RedisClusterNode, redis.Node) {
	node := newRedisNode(rapi.RedisClusterNodeRoleReplica, id, zone, podName, nodeName, nil)
	node.PrimaryRef = primaryRef
	redisNode := redis.Node{ID: id, PrimaryReferent: primaryRef, Zone: zone, Role: string(rapi.RedisClusterNodeRoleReplica), Pod: node.Pod}
	return node, redisNode
}

func newRedisNode(role rapi.RedisClusterNodeRole, id, zone, podName, nodeName string, slots []string) rapi.RedisClusterNode {
	pod := NewPod(podName, nodeName)

	return rapi.RedisClusterNode{ID: id, Zone: zone, Slots: slots, Role: role, Pod: pod}
}

func NewPod(name, node string) *kapiv1.Pod {
	return &kapiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: kapiv1.PodSpec{
			NodeName: node,
		},
	}
}

func NewNode(name, zone string) *kapiv1.Node {
	return &kapiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			ResourceVersion: "358",
			Labels: map[string]string{
				kapiv1.LabelTopologyZone: zone,
			},
		},
	}
}
