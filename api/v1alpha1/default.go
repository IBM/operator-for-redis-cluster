package v1alpha1

import (
	"github.com/gogo/protobuf/proto"
	kapiv1 "k8s.io/api/core/v1"
)

var (
	defaultNumberOfPrimaries = proto.Int32(3)
	defaultReplicationFactor = proto.Int32(1)
	defaultKeyBatchSize      = proto.Int32(10000)
	defaultSlotBatchSize     = proto.Int32(16)
	defaultIdleTimeoutMillis = proto.Int32(30000)
)

// IsRedisClusterDefaulted check if the RedisCluster is already defaulted
func IsRedisClusterDefaulted(rc *RedisCluster) bool {
	if rc.Spec.NumberOfPrimaries == nil {
		return false
	}
	if rc.Spec.ReplicationFactor == nil {
		return false
	}
	if rc.Spec.RollingUpdate == nil {
		return false
	}
	if rc.Spec.Scaling == nil {
		return false
	}
	return true
}

// DefaultRedisCluster defaults RedisCluster
func DefaultRedisCluster(baseRedisCluster *RedisCluster) *RedisCluster {
	rc := baseRedisCluster.DeepCopy()
	if rc.Spec.NumberOfPrimaries == nil {
		rc.Spec.NumberOfPrimaries = defaultNumberOfPrimaries
	}
	if rc.Spec.ReplicationFactor == nil {
		rc.Spec.ReplicationFactor = defaultReplicationFactor
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

	if rc.Spec.RollingUpdate == nil {
		rc.Spec.RollingUpdate = &RollingUpdate{}
	}

	if rc.Spec.RollingUpdate.KeyMigration == nil {
		rc.Spec.RollingUpdate.KeyMigration = proto.Bool(true)
	}

	if rc.Spec.RollingUpdate.KeyBatchSize == nil {
		rc.Spec.RollingUpdate.KeyBatchSize = defaultKeyBatchSize
	}

	if rc.Spec.RollingUpdate.SlotBatchSize == nil {
		rc.Spec.RollingUpdate.SlotBatchSize = defaultSlotBatchSize
	}

	if rc.Spec.RollingUpdate.IdleTimeoutMillis == nil {
		rc.Spec.RollingUpdate.IdleTimeoutMillis = defaultIdleTimeoutMillis
	}

	if rc.Spec.Scaling == nil {
		rc.Spec.Scaling = &Migration{}
	}

	if rc.Spec.Scaling.KeyBatchSize == nil {
		rc.Spec.Scaling.KeyBatchSize = defaultKeyBatchSize
	}

	if rc.Spec.Scaling.SlotBatchSize == nil {
		rc.Spec.Scaling.SlotBatchSize = defaultSlotBatchSize
	}

	if rc.Spec.Scaling.IdleTimeoutMillis == nil {
		rc.Spec.Scaling.IdleTimeoutMillis = defaultIdleTimeoutMillis
	}

	return rc
}
