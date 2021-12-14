package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kapiv1 "k8s.io/api/core/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RedisCluster represents a Redis Cluster
// +kubebuilder:object:root=true
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:resource:scope=Namespaced,shortName=rdc
// +kubebuilder:subresource:scale:specpath=.spec.numberOfPrimaries,statuspath=.status.cluster.numberOfPrimariesReady,selectorpath=.status.cluster.labelSelectorPath
// +kubebuilder:subresource:status
type RedisCluster struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec represents the desired RedisCluster specification
	Spec RedisClusterSpec `json:"spec,omitempty"`

	// Status represents the current RedisCluster status
	Status RedisClusterStatus `json:"status,omitempty"`
}

// RedisClusterList implements list of RedisCluster.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// More info: http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#metadata
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the list of RedisCluster
	Items []RedisCluster `json:"items"`
}

// RedisClusterSpec contains RedisCluster specification
type RedisClusterSpec struct {
	// NumberOfPrimaries number of primary nodes
	NumberOfPrimaries *int32 `json:"numberOfPrimaries,omitempty"`

	// ReplicationFactor number of replica nodes per primary node
	ReplicationFactor *int32 `json:"replicationFactor,omitempty"`

	// ServiceName name used to create the kubernetes service that fronts the RedisCluster nodes.
	// If ServiceName is empty, the RedisCluster name will be used for creating the service.
	ServiceName string `json:"serviceName,omitempty"`

	// PodTemplate contains the pod specification that should run the redis-server process
	PodTemplate *kapiv1.PodTemplateSpec `json:"podTemplate,omitempty"`

	// ZoneAwareReplication spreads primary and replica nodes across all available zones
	ZoneAwareReplication *bool `json:"zoneAwareReplication,omitempty"`

	// RollingUpdate configuration for redis key migration
	RollingUpdate *RollingUpdate `json:"rollingUpdate,omitempty"`

	// Scaling configuration for redis key migration
	Scaling *Migration `json:"scaling,omitempty"`

	// Labels for created redis-cluster (deployment, rs, pod) (if any)
	AdditionalLabels map[string]string `json:"additionalLabels,omitempty"`
}

// RedisClusterStatus contains RedisCluster status
type RedisClusterStatus struct {
	// Conditions represent the latest available observations of an object's current state.
	Conditions []RedisClusterCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
	// StartTime represents time when the workflow was acknowledged by the Workflow controller
	// It is not guaranteed to be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	// StartTime doesn't consider startime of `ExternalReference`
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// Cluster a view of the current RedisCluster
	Cluster RedisClusterState `json:"cluster"`
}

// RedisClusterCondition represent the condition of the RedisCluster
type RedisClusterCondition struct {
	// Type of workflow condition
	Type RedisClusterConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status kapiv1.ConditionStatus `json:"status"`
	// Last time the condition was checked.
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty"`
	// Last time the condition transited from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	// (brief) reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// Human-readable message indicating details about last transition.
	Message string `json:"message,omitempty"`
}

// RedisClusterState represent the Redis Cluster status
type RedisClusterState struct {
	Status                     ClusterStatus  `json:"status"`
	NumberOfPrimaries          int32          `json:"numberOfPrimaries,omitempty"`
	NumberOfPrimariesReady     int32          `json:"numberOfPrimariesReady,omitempty"`
	NumberOfReplicasPerPrimary map[string]int `json:"numberOfReplicasPerPrimary,omitempty"`
	MinReplicationFactor       int32          `json:"minReplicationFactor,omitempty"`
	MaxReplicationFactor       int32          `json:"maxReplicationFactor,omitempty"`

	NodesPlacement NodesPlacementInfo `json:"nodesPlacementInfo,omitempty"`

	// In theory, we always have NumberOfPods > NumberOfRedisNodesRunning > NumberOfPodsReady
	NumberOfPods              int32 `json:"numberOfPods,omitempty"`
	NumberOfPodsReady         int32 `json:"numberOfPodsReady,omitempty"`
	NumberOfRedisNodesRunning int32 `json:"numberOfRedisNodesRunning,omitempty"`

	LabelSelectorPath string `json:"labelSelectorPath"`

	Nodes []RedisClusterNode `json:"nodes,omitempty"`
}

type RollingUpdate struct {
	Migration `json:",inline"`
	// KeyMigration whether or not to migrate keys during a rolling update
	KeyMigration *bool `json:"keyMigration,omitempty"`
	// Amount of time in between each slot batch iteration
	WarmingDelayMillis int32 `json:"warmingDelayMillis,omitempty"`
}

type Migration struct {
	// Number of keys to get from a single slot during each migration iteration
	KeyBatchSize *int32 `json:"keyBatchSize,omitempty"`
	// Number of slots to to migrate on each iteration
	SlotBatchSize *int32 `json:"slotBatchSize,omitempty"`
	// Maximum idle time at any point during key migration
	IdleTimeoutMillis *int32 `json:"idleTimeoutMillis,omitempty"`
}

func (s RedisClusterState) String() string {
	output := ""
	output += fmt.Sprintf("status:%s\n", s.Status)
	output += fmt.Sprintf("NumberOfPrimaries:%d\n", s.NumberOfPrimaries)
	output += fmt.Sprintf("MinReplicationFactor:%d\n", s.MinReplicationFactor)
	output += fmt.Sprintf("MaxReplicationFactor:%d\n", s.MaxReplicationFactor)
	output += fmt.Sprintf("NodesPlacement:%s\n\n", s.NodesPlacement)
	output += fmt.Sprintf("NumberOfPods:%d\n", s.NumberOfPods)
	output += fmt.Sprintf("NumberOfPodsReady:%d\n", s.NumberOfPodsReady)
	output += fmt.Sprintf("NumberOfRedisNodesRunning:%d\n\n", s.NumberOfRedisNodesRunning)

	output += fmt.Sprintf("Nodes (%d): %s\n", len(s.Nodes), s.Nodes)

	return output
}

// NodesPlacementInfo Redis Nodes placement mode information
type NodesPlacementInfo string

const (
	// NodesPlacementInfoBestEffort the cluster nodes placement is in best effort,
	// it means you can have 2 (or more) primaries on the same kubernetes node
	NodesPlacementInfoBestEffort NodesPlacementInfo = "BestEffort"
	// NodesPlacementInfoOptimal the cluster nodes placement is optimal,
	// it means one primary per kubernetes node
	NodesPlacementInfoOptimal NodesPlacementInfo = "Optimal"
)

// RedisClusterNode represent a RedisCluster node
type RedisClusterNode struct {
	ID         string               `json:"id"`
	Role       RedisClusterNodeRole `json:"role"`
	Zone       string               `json:"zone"`
	IP         string               `json:"ip"`
	Port       string               `json:"port"`
	Slots      []string             `json:"slots,omitempty"`
	PrimaryRef string               `json:"primaryRef,omitempty"`
	PodName    string               `json:"podName"`
	Pod        *kapiv1.Pod          `json:"-"`
}

func (n RedisClusterNode) String() string {
	if n.Role != RedisClusterNodeRoleReplica {
		return fmt.Sprintf("(Primary:%s, Zone:%s, Addr:%s:%s, PodName:%s, Slots:%v)", n.ID, n.Zone, n.IP, n.Port, n.PodName, n.Slots)
	}
	return fmt.Sprintf("(Replica:%s, Zone:%s, Addr:%s:%s, PodName:%s, PrimaryRef:%s)", n.ID, n.Zone, n.IP, n.Port, n.PodName, n.PrimaryRef)
}

// RedisClusterConditionType is the type of RedisClusterCondition
type RedisClusterConditionType string

const (
	// RedisClusterOK means the RedisCluster is in a good shape
	RedisClusterOK RedisClusterConditionType = "ClusterOK"
	// RedisClusterScaling means the RedisCluster is currently in a scaling stage
	RedisClusterScaling RedisClusterConditionType = "Scaling"
	// RedisClusterRebalancing means the RedisCluster is currently rebalancing slots and keys
	RedisClusterRebalancing RedisClusterConditionType = "Rebalancing"
	// RedisClusterRollingUpdate means the RedisCluster is currently performing a rolling update of its nodes
	RedisClusterRollingUpdate RedisClusterConditionType = "RollingUpdate"
)

// RedisClusterNodeRole RedisCluster Node Role type
type RedisClusterNodeRole string

const (
	// RedisClusterNodeRolePrimary RedisCluster Primary node role
	RedisClusterNodeRolePrimary RedisClusterNodeRole = "Primary"
	// RedisClusterNodeRoleReplica RedisCluster Replica node role
	RedisClusterNodeRoleReplica RedisClusterNodeRole = "Replica"
	// RedisClusterNodeRoleHandshake RedisCluster Handshake node role
	RedisClusterNodeRoleHandshake RedisClusterNodeRole = "Handshake"
	// RedisClusterNodeRoleNone None node role
	RedisClusterNodeRoleNone RedisClusterNodeRole = "None"
)

// ClusterStatus Redis Cluster status
type ClusterStatus string

const (
	// ClusterStatusOK ClusterStatus OK
	ClusterStatusOK ClusterStatus = "OK"
	// ClusterStatusKO ClusterStatus KO
	ClusterStatusKO ClusterStatus = "KO"
	// ClusterStatusScaling ClusterStatus Scaling
	ClusterStatusScaling ClusterStatus = "Scaling"
	// ClusterStatusRebalancing ClusterStatus Rebalancing
	ClusterStatusRebalancing ClusterStatus = "Rebalancing"
	// ClusterStatusRollingUpdate ClusterStatus RollingUpdate
	ClusterStatusRollingUpdate ClusterStatus = "RollingUpdate"
)
