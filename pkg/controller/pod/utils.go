package pod

import (
	"fmt"

	"k8s.io/apimachinery/pkg/labels"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
)

// GetLabelsSet return labels associated to the redis-node pods
func GetLabelsSet(cluster *rapi.RedisCluster) (labels.Set, error) {
	desiredLabels := labels.Set{}
	if cluster == nil {
		return desiredLabels, fmt.Errorf("redisluster nil pointer")
	}
	if cluster.Spec.AdditionalLabels != nil {
		desiredLabels = cluster.Spec.AdditionalLabels
	}
	if cluster.Spec.PodTemplate != nil {
		for k, v := range cluster.Spec.PodTemplate.Labels {
			desiredLabels[k] = v
		}
	}
	desiredLabels[rapi.ClusterNameLabelKey] = cluster.Name // add redis cluster name to the Pod labels
	return desiredLabels, nil
}

// CreateRedisClusterLabelSelector creates label selector to select the jobs related to a redis cluster
func CreateRedisClusterLabelSelector(cluster *rapi.RedisCluster) (labels.Selector, error) {
	set, err := GetLabelsSet(cluster)
	if err != nil {
		return nil, err
	}
	return labels.SelectorFromSet(set), nil
}

// GetAnnotationsSet return a labels.Set of annotation from the RedisCluster
func GetAnnotationsSet(cluster *rapi.RedisCluster) (labels.Set, error) {
	desiredAnnotations := make(labels.Set)
	for k, v := range cluster.Annotations {
		desiredAnnotations[k] = v
	}

	// TODO: add createdByRef
	return desiredAnnotations, nil // no error for the moment, when we'll add createdByRef an error could be returned
}
