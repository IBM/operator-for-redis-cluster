package v1alpha1

import (
	"github.com/gogo/protobuf/proto"
	kapiv1 "k8s.io/api/core/v1"
)

// IsRedisClusterDefaulted check if the RedisCluster is already defaulted
func IsRedisClusterDefaulted(rc *RedisCluster) bool {
	if rc.Spec.NumberOfPrimaries == nil {
		return false
	}
	if rc.Spec.ReplicationFactor == nil {
		return false
	}
	return true
}

// DefaultRedisCluster defaults RedisCluster
func DefaultRedisCluster(baseRedisCluster *RedisCluster) *RedisCluster {
	rc := baseRedisCluster.DeepCopy()
	if rc.Spec.NumberOfPrimaries == nil {
		rc.Spec.NumberOfPrimaries = NewInt32(3)
	}
	if rc.Spec.ReplicationFactor == nil {
		rc.Spec.ReplicationFactor = NewInt32(1)
	}

	if rc.Spec.PodTemplate == nil {
		rc.Spec.PodTemplate = &kapiv1.PodTemplateSpec{}
	}

	rc.Status.Cluster.NumberOfPrimaries = 0
	rc.Status.Cluster.MinReplicationFactor = 0
	rc.Status.Cluster.MaxReplicationFactor = 0
	rc.Status.Cluster.NumberOfPods = 0
	rc.Status.Cluster.NumberOfPodsReady = 0
	rc.Status.Cluster.NumberOfRedisNodesRunning = 0

	if rc.Spec.ZoneAwareReplication == nil {
		rc.Spec.ZoneAwareReplication = proto.Bool(true)
	}

	if rc.Spec.KeyMigration == nil {
		rc.Spec.KeyMigration = proto.Bool(true)
	}

	if rc.Spec.Migration.SlotBatchSize == 0 {
		rc.Spec.Migration.SlotBatchSize = 100
	}

	if rc.Spec.Migration.KeyBatchSize == 0 {
		rc.Spec.Migration.KeyBatchSize = 100
	}

	if rc.Spec.Migration.IdleTimeoutMillis == 0 {
		rc.Spec.Migration.IdleTimeoutMillis = 30000
	}

	return rc
}

// NewInt32 use to instantiate an int32 pointer
func NewInt32(val int32) *int32 {
	output := new(int32)
	*output = val

	return output
}
