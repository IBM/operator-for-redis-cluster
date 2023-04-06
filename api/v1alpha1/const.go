package v1alpha1

const (
	// ClusterNameLabelKey Label key for the ClusterName
	ClusterNameLabelKey string = "redis-operator.k8s.io/cluster-name"
	// PodSpecSHA2LabelKey label key for the PodSpec SHA2 hash
	PodSpecSHA2LabelKey string = "redis-operator.k8s.io/podspec-sha2"
	// UnknownZone label for unknown zone
	UnknownZone string = "unknown"
)
