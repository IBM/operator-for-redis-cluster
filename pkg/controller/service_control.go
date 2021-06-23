package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/pod"
)

// ServicesControlInterface inferface for the ServicesControl
type ServicesControlInterface interface {
	// CreateRedisClusterService used to create the Kubernetes Service needed to access the Redis Cluster
	CreateRedisClusterService(redisCluster *rapi.RedisCluster) (*v1.Service, error)
	// DeleteRedisClusterService used to delete the Kubernetes Service linked to the Redis Cluster
	DeleteRedisClusterService(redisCluster *rapi.RedisCluster) error
	// GetRedisClusterService used to retrieve the Kubernetes Service associated to the RedisCluster
	GetRedisClusterService(redisCluster *rapi.RedisCluster) (*v1.Service, error)
}

// ServicesControl contains all information for managing Kube Services
type ServicesControl struct {
	KubeClient client.Client
	Recorder   record.EventRecorder
}

// NewServicesControl builds and returns new ServicesControl instance
func NewServicesControl(client client.Client, rec record.EventRecorder) *ServicesControl {
	ctrl := &ServicesControl{
		KubeClient: client,
		Recorder:   rec,
	}

	return ctrl
}

// GetRedisClusterService used to retrieve the Kubernetes Service associated to the RedisCluster
func (s *ServicesControl) GetRedisClusterService(redisCluster *rapi.RedisCluster) (*v1.Service, error) {
	serviceName := types.NamespacedName{
		Name:      getServiceName(redisCluster),
		Namespace: redisCluster.Namespace,
	}
	svc := &v1.Service{}
	err := s.KubeClient.Get(context.Background(), serviceName, svc)
	if err != nil {
		return nil, err
	}
	return svc, nil
}

// CreateRedisClusterService used to create the Kubernetes Service needed to access the Redis Cluster
func (s *ServicesControl) CreateRedisClusterService(redisCluster *rapi.RedisCluster) (*v1.Service, error) {
	serviceName := getServiceName(redisCluster)
	desiredlabels, err := pod.GetLabelsSet(redisCluster)
	if err != nil {
		return nil, err

	}

	desiredAnnotations, err := pod.GetAnnotationsSet(redisCluster)
	if err != nil {
		return nil, err
	}
	newService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          desiredlabels,
			Annotations:     desiredAnnotations,
			Name:            serviceName,
			Namespace:       redisCluster.Namespace,
			OwnerReferences: []metav1.OwnerReference{pod.BuildOwnerReference(redisCluster)},
		},
		Spec: v1.ServiceSpec{
			ClusterIP: "None",
			Ports:     []v1.ServicePort{{Port: 6379, Name: "redis"}},
			Selector:  desiredlabels,
		},
	}

	err = s.KubeClient.Create(context.Background(), newService)
	if err != nil {
		return nil, err
	}
	return newService, nil
}

// DeleteRedisClusterService used to delete the Kubernetes Service linked to the Redis Cluster
func (s *ServicesControl) DeleteRedisClusterService(redisCluster *rapi.RedisCluster) error {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getServiceName(redisCluster),
			Namespace: redisCluster.Namespace,
		},
	}
	return s.KubeClient.Delete(context.Background(), svc)
}

func getServiceName(redisCluster *rapi.RedisCluster) string {
	serviceName := redisCluster.Name
	if redisCluster.Spec.ServiceName != "" {
		serviceName = redisCluster.Spec.ServiceName
	}
	return serviceName
}
