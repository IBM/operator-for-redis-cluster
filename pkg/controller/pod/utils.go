package pod

import (
	"fmt"

	"k8s.io/apimachinery/pkg/labels"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1alpha1"
)

// GetLabelsSet return labels associated to the redis-node pods
func GetLabelsSet(rc *rapi.RedisCluster) (labels.Set, error) {
	desiredLabels := labels.Set{}
	if rc == nil {
		return desiredLabels, fmt.Errorf("redisluster nil pointer")
	}
	if rc.Spec.AdditionalLabels != nil {
		desiredLabels = rc.Spec.AdditionalLabels
	}
	if rc.Spec.PodTemplate != nil {
		for k, v := range rc.Spec.PodTemplate.Labels {
			desiredLabels[k] = v
		}
	}
	desiredLabels[rapi.ClusterNameLabelKey] = rc.Name // add redis cluster name to the Pod labels
	return desiredLabels, nil
}

// CreateRedisClusterLabelSelector creates label selector to select the jobs related to a redis cluster
func CreateRedisClusterLabelSelector(rc *rapi.RedisCluster) (labels.Selector, error) {
	set, err := GetLabelsSet(rc)
	if err != nil {
		return nil, err
	}
	return labels.SelectorFromSet(set), nil
}

// GetAnnotationsSet return a labels.Set of annotation from the RedisCluster
func GetAnnotationsSet(rc *rapi.RedisCluster) (labels.Set, error) {
	desiredAnnotations := make(labels.Set)
	for k, v := range rc.Annotations {
		desiredAnnotations[k] = v
	}

	// TODO: add createdByRef
	return desiredAnnotations, nil // no error for the moment, when we'll add createdByRef an error could be returned
}
