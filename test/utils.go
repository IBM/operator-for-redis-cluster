package test

import (
	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
	kapiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewRedisReplicaNode(id, zone, primaryRef, podName, nodeName string) (rapi.RedisClusterNode, redis.Node) {
	node := NewRedisNode(rapi.RedisClusterNodeRoleReplica, id, podName, nodeName, nil)
	node.PrimaryRef = primaryRef
	role := "replica"
	redisNode := redis.Node{ID: id, PrimaryReferent: primaryRef, Zone: zone, Role: role, Pod: node.Pod}
	return node, redisNode
}

func NewRedisPrimaryNode(id, zone, podName, nodeName string, slots []string) (rapi.RedisClusterNode, redis.Node) {
	role := "primary"
	slotsInt := redis.SlotSlice{}
	for _, s := range slots {
		i, _ := redis.DecodeSlot(s)
		slotsInt = append(slotsInt, i)
	}
	node := NewRedisNode(rapi.RedisClusterNodeRolePrimary, id, podName, nodeName, slots)
	redisNode := redis.Node{ID: id, Zone: zone, Slots: slotsInt, Role: role, Pod: node.Pod}
	return node, redisNode
}

func NewRedisNode(role rapi.RedisClusterNodeRole, id, podName, nodeName string, slots []string) rapi.RedisClusterNode {
	pod := NewPod(podName, nodeName)

	return rapi.RedisClusterNode{ID: id, Slots: slots, Role: role, Pod: pod}
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
