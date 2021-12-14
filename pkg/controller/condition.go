package controller

import (
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
)

// newCondition return a new defaulted instance of a RedisClusterCondition
func newCondition(conditionType rapi.RedisClusterConditionType, status apiv1.ConditionStatus, now metav1.Time, reason, message string) rapi.RedisClusterCondition {
	return rapi.RedisClusterCondition{
		Type:               conditionType,
		Status:             status,
		LastProbeTime:      now,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}
}

// updateCondition return an updated version of the RedisClusterCondition
func updateCondition(from rapi.RedisClusterCondition, status apiv1.ConditionStatus, now metav1.Time, reason, message string) rapi.RedisClusterCondition {
	newCondition := from.DeepCopy()
	newCondition.LastProbeTime = now
	newCondition.Message = message
	newCondition.Reason = reason
	if status != newCondition.Status {
		newCondition.Status = status
		newCondition.LastTransitionTime = now
	}

	return *newCondition
}

func setCondition(clusterStatus *rapi.RedisClusterStatus, conditionType rapi.RedisClusterConditionType, status apiv1.ConditionStatus, now metav1.Time, reason, message string) bool {
	updated := false
	found := false
	for i, c := range clusterStatus.Conditions {
		if c.Type == conditionType {
			found = true
			if c.Status != status {
				updated = true
				clusterStatus.Conditions[i] = updateCondition(c, status, now, reason, message)
			}
		}
	}
	if !found {
		updated = true
		clusterStatus.Conditions = append(clusterStatus.Conditions, newCondition(conditionType, status, now, reason, message))
	}
	return updated
}

func setScalingCondition(clusterStatus *rapi.RedisClusterStatus, status bool) bool {
	statusCondition := apiv1.ConditionFalse
	if status {
		statusCondition = apiv1.ConditionTrue
	}
	return setCondition(clusterStatus, rapi.RedisClusterScaling, statusCondition, metav1.Now(), "cluster needs more pods", "cluster needs more pods")
}

func setRebalancingCondition(clusterStatus *rapi.RedisClusterStatus, status bool) bool {
	statusCondition := apiv1.ConditionFalse
	if status {
		statusCondition = apiv1.ConditionTrue
	}
	return setCondition(clusterStatus, rapi.RedisClusterRebalancing, statusCondition, metav1.Now(), "topology has changed", "topology has changed")
}

func setRollingUpdateCondition(clusterStatus *rapi.RedisClusterStatus, status bool) bool {
	statusCondition := apiv1.ConditionFalse
	if status {
		statusCondition = apiv1.ConditionTrue
	}
	return setCondition(clusterStatus, rapi.RedisClusterRollingUpdate, statusCondition, metav1.Now(), "rolling update in progress", "rolling update in progress")
}

func setClusterStatusCondition(clusterStatus *rapi.RedisClusterStatus, status bool) bool {
	statusCondition := apiv1.ConditionFalse
	if status {
		statusCondition = apiv1.ConditionTrue
	}
	return setCondition(clusterStatus, rapi.RedisClusterOK, statusCondition, metav1.Now(), "cluster is correctly configured", "cluster is correctly configured")
}
