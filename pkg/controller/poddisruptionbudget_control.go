package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	policyv1 "k8s.io/api/policy/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"k8s.io/client-go/tools/record"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller/pod"
)

// PodDisruptionBudgetsControlInterface interface for the PodDisruptionBudgetsControl
type PodDisruptionBudgetsControlInterface interface {
	// CreateRedisClusterPodDisruptionBudget used to create the Kubernetes PodDisruptionBudget needed to access the Redis Cluster
	CreateRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster) (*policyv1.PodDisruptionBudget, error)
	// UpdateRedisClusterPodDisruptionBudget used to update the Kubernetes PodDisruptionBudget needed to access the Redis Cluster
	UpdateRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster, existingPDB *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error)
	// NeedUpdateRedisClusterPodDisruptionBudget used to check if the Kubernetes PodDisruptionBudget needed to access the Redis cluster needs to be updated
	NeedUpdateRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster, existingPDB *policyv1.PodDisruptionBudget) (bool, error)
	// DeleteRedisClusterPodDisruptionBudget used to delete the Kubernetes PodDisruptionBudget linked to the Redis Cluster
	DeleteRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster) error
	// GetRedisClusterPodDisruptionBudget used to retrieve the Kubernetes PodDisruptionBudget associated to the RedisCluster
	GetRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster) (*policyv1.PodDisruptionBudget, error)
}

// PodDisruptionBudgetsControl contains all information for managing Kube PodDisruptionBudgets
type PodDisruptionBudgetsControl struct {
	KubeClient client.Client
	Recorder   record.EventRecorder
}

// NewPodDisruptionBudgetsControl builds and returns new PodDisruptionBudgetsControl instance
func NewPodDisruptionBudgetsControl(client client.Client, rec record.EventRecorder) *PodDisruptionBudgetsControl {
	ctrl := &PodDisruptionBudgetsControl{
		KubeClient: client,
		Recorder:   rec,
	}

	return ctrl
}

// GetRedisClusterPodDisruptionBudget used to retrieve the Kubernetes PodDisruptionBudget associated to the RedisCluster
func (s *PodDisruptionBudgetsControl) GetRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster) (*policyv1.PodDisruptionBudget, error) {
	pdbName := types.NamespacedName{
		Namespace: redisCluster.Namespace,
		Name:      redisCluster.Name,
	}
	pdb := &policyv1.PodDisruptionBudget{}
	err := s.KubeClient.Get(context.Background(), pdbName, pdb)
	if err != nil {
		return nil, err
	}

	return pdb, nil
}

// DeleteRedisClusterPodDisruptionBudget used to delete the Kubernetes PodDisruptionBudget linked to the Redis Cluster
func (s *PodDisruptionBudgetsControl) DeleteRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: redisCluster.Namespace,
			Name:      redisCluster.Name,
		},
	}

	return s.KubeClient.Delete(context.Background(), pdb)
}

// CreateRedisClusterPodDisruptionBudget used to create the Kubernetes PodDisruptionBudget needed to access the Redis Cluster
func (s *PodDisruptionBudgetsControl) CreateRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster) (*policyv1.PodDisruptionBudget, error) {
	desiredPodDisruptionBudget, err := s.desiredRedisClusterPodDisruptionBudget(redisCluster)
	if err != nil {
		return nil, err
	}

	err = s.KubeClient.Create(context.Background(), desiredPodDisruptionBudget)
	if err != nil {
		return nil, err
	}

	return desiredPodDisruptionBudget, nil
}

// NeedUpdateRedisClusterPodDisruptionBudget used to check if the Kubernetes PodDisruptionBudget needed to access the Redis cluster needs to be updated
func (s *PodDisruptionBudgetsControl) NeedUpdateRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster, existingPDB *policyv1.PodDisruptionBudget) (bool, error) {
	desiredPodDisruptionBudget, err := s.desiredRedisClusterPodDisruptionBudget(redisCluster)
	if err != nil {
		return false, err
	}

	if !equality.Semantic.DeepEqual(existingPDB.Labels, desiredPodDisruptionBudget.Labels) ||
		!equality.Semantic.DeepEqual(existingPDB.Annotations, desiredPodDisruptionBudget.Annotations) ||
		!equality.Semantic.DeepEqual(existingPDB.Spec, desiredPodDisruptionBudget.Spec) {
		return true, nil
	}

	return false, nil
}

// UpdateRedisClusterPodDisruptionBudget used to update the Kubernetes PodDisruptionBudget needed to access the Redis Cluster
func (s *PodDisruptionBudgetsControl) UpdateRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster, existingPDB *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error) {
	desiredPodDisruptionBudget, err := s.desiredRedisClusterPodDisruptionBudget(redisCluster)
	if err != nil {
		return nil, err
	}

	newPodDisruptionBudget := existingPDB.DeepCopy()
	newPodDisruptionBudget.ObjectMeta.Labels = desiredPodDisruptionBudget.Labels
	newPodDisruptionBudget.ObjectMeta.Annotations = desiredPodDisruptionBudget.Annotations
	newPodDisruptionBudget.Spec = desiredPodDisruptionBudget.Spec

	err = s.KubeClient.Update(context.Background(), newPodDisruptionBudget)
	if err != nil {
		return nil, err
	}

	return newPodDisruptionBudget, nil
}

func (s *PodDisruptionBudgetsControl) desiredRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster) (*policyv1.PodDisruptionBudget, error) {
	PodDisruptionBudgetName := redisCluster.Name
	desiredLabels, err := pod.GetLabelsSet(redisCluster)
	if err != nil {
		return nil, err

	}

	desiredAnnotations, err := pod.GetClusterAnnotationsSet(redisCluster)
	if err != nil {
		return nil, err
	}

	minAvailable := intstr.FromInt(int(*redisCluster.Spec.NumberOfPrimaries*(1+*redisCluster.Spec.ReplicationFactor) - 1))

	labelSelector := metav1.LabelSelector{
		MatchLabels: desiredLabels,
	}
	newPodDisruptionBudget := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          desiredLabels,
			Annotations:     desiredAnnotations,
			Name:            PodDisruptionBudgetName,
			Namespace:       redisCluster.Namespace,
			OwnerReferences: []metav1.OwnerReference{pod.BuildOwnerReference(redisCluster)},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &labelSelector,
		},
	}
	return newPodDisruptionBudget, nil
}
