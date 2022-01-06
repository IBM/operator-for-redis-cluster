package garbagecollector

import (
	"context"
	"fmt"
	"path"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/golang/glog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
)

const (
	// Interval represents the interval to run Garbage Collection
	Interval = 5 * time.Second
)

// Interface GarbageCollector interface
type Interface interface {
	CollectRedisClusterGarbage() error
}

var _ Interface = &GarbageCollector{}

// GarbageCollector represents a Workflow Garbage Collector.
// It collects orphaned Jobs.
type GarbageCollector struct {
	kubeClient client.Client
}

// NewGarbageCollector initializes and returns a GarbageCollector
func NewGarbageCollector(kubeClient client.Client) *GarbageCollector {
	return &GarbageCollector{
		kubeClient: kubeClient,
	}
}

func (c *GarbageCollector) Run(stop <-chan struct{}) {
	wait.Until(func() {
		err := c.CollectRedisClusterGarbage()
		if err != nil {
			glog.Errorf("collecting rediscluster pods and services: %v", err)
		}
	}, Interval, stop)
	// run garbage collection before stopping the process
	err := c.CollectRedisClusterGarbage()
	if err != nil {
		glog.Errorf("collecting rediscluster pods and services: %v", err)
	}
}

// CollectRedisClusterGarbage collects the orphaned pods and services.
// Looks through the informer list and removes missing pods via DeleteCollection primitive.
func (c *GarbageCollector) CollectRedisClusterGarbage() error {
	errs := []error{}
	if err := c.collectRedisClusterPods(); err != nil {
		errs = append(errs, err)
	}
	if err := c.collectRedisClusterServices(); err != nil {
		errs = append(errs, err)
	}
	return utilerrors.NewAggregate(errs)
}

// collectRedisClusterPods collects the orphaned pods
func (c *GarbageCollector) collectRedisClusterPods() error {
	glog.V(4).Infof("Collecting garbage pods")
	pods := &v1.PodList{}
	err := c.kubeClient.List(context.Background(), pods, client.InNamespace(metav1.NamespaceAll), client.HasLabels{rapi.ClusterNameLabelKey})
	if err != nil {
		return fmt.Errorf("unable to list rediscluster pods to be collected: %v", err)
	}
	errs := []error{}
	collected := make(map[string]struct{})
	for _, pod := range pods.Items {
		redisclusterName, found := pod.Labels[rapi.ClusterNameLabelKey]
		if !found || len(redisclusterName) == 0 {
			errs = append(errs, fmt.Errorf("Unable to find rediscluster name for pod: %s/%s", pod.Namespace, pod.Name))
			continue
		}
		if _, done := collected[path.Join(pod.Namespace, redisclusterName)]; done {
			continue // already collected so skip
		}

		err := c.kubeClient.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: redisclusterName}, &rapi.RedisCluster{})
		if err == nil || !apierrors.IsNotFound(err) {
			if err != nil {
				errs = append(errs, fmt.Errorf("Unexpected error retrieving rediscluster %s/%s for pod %s/%s: %v", pod.Namespace, redisclusterName, pod.Namespace, pod.Name, err))
			}
			continue
		}

		// NotFound error: Hence remove all the pods.
		err = c.kubeClient.DeleteAllOf(
			context.Background(),
			&v1.Pod{},
			client.InNamespace(pod.Namespace),
			client.MatchingLabels{rapi.ClusterNameLabelKey: redisclusterName},
			client.GracePeriodSeconds(0),
			client.PropagationPolicy(metav1.DeletePropagationBackground),
		)
		if err != nil {
			errs = append(errs, fmt.Errorf("Unable to delete Collection of pods for rediscluster %s/%s", pod.Namespace, redisclusterName))
			continue
		}
		collected[path.Join(pod.Namespace, redisclusterName)] = struct{}{} // inserted in the collected map
		glog.Infof("Removed all pods for rediscluster %s/%s", pod.Namespace, redisclusterName)
	}
	return utilerrors.NewAggregate(errs)
}

// collectRedisClusterServices collects the orphaned services
func (c *GarbageCollector) collectRedisClusterServices() error {
	glog.V(4).Infof("Collecting garbage services")
	services := &v1.ServiceList{}
	err := c.kubeClient.List(context.Background(), services, client.InNamespace(metav1.NamespaceAll), client.HasLabels{rapi.ClusterNameLabelKey})
	if err != nil {
		return fmt.Errorf("unable to list rediscluster services to be collected: %v", err)
	}
	errs := []error{}
	collected := make(map[string]struct{})
	for _, service := range services.Items {
		clusterName, found := service.Labels[rapi.ClusterNameLabelKey]
		if !found || len(clusterName) == 0 {
			errs = append(errs, fmt.Errorf("Unable to find rediscluster name for service: %s/%s", service.Namespace, service.Name))
			continue
		}
		if _, done := collected[path.Join(service.Namespace, clusterName)]; done {
			continue // already collected so skip
		}
		err = c.kubeClient.Get(context.Background(), types.NamespacedName{Namespace: service.Namespace, Name: clusterName}, &rapi.RedisCluster{})
		if err == nil || !apierrors.IsNotFound(err) {
			if err != nil {
				errs = append(errs, fmt.Errorf("Unexpected error retrieving rediscluster %s/%s cache: %v", service.Namespace, clusterName, err))
			}
			continue
		}
		// NotFound error: Hence remove all the pods.
		if err := c.deleteRedisClusterServices(service.Namespace, clusterName); err != nil {
			errs = append(errs, fmt.Errorf("Unable to delete Collection of services for rediscluster %s/%s", service.Namespace, clusterName))
			continue
		}

		collected[path.Join(service.Namespace, clusterName)] = struct{}{} // inserted in the collected map
		glog.Infof("Removed all services for rediscluster %s/%s", service.Namespace, clusterName)
	}
	return utilerrors.NewAggregate(errs)
}

func (c *GarbageCollector) deleteRedisClusterServices(namespace, redisClusterName string) error {
	services := &v1.ServiceList{}
	err := c.kubeClient.List(context.Background(), services, client.InNamespace(namespace), client.MatchingLabels{rapi.ClusterNameLabelKey: redisClusterName})
	if err != nil {
		return err
	}

	if len(services.Items) == 0 {
		return nil
	}

	for _, srv := range services.Items {
		err := c.kubeClient.Delete(context.Background(), &srv, client.GracePeriodSeconds(0), client.PropagationPolicy(metav1.DeletePropagationBackground))
		if err != nil {
			return err
		}
	}

	return nil
}
