package garbagecollector

import (
	"testing"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	cfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
)

func TestGarbageCollector_CollectRedisClusterJobs(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rapi.AddToScheme(scheme))

	tests := map[string]struct {
		TweakGarbageCollector func(*GarbageCollector) *GarbageCollector
		errorMessage          string
	}{
		"nominal case": {
			// getting 4 pods:
			//   2 to be removed 2 (no rediscluster)
			//   2 to be preserved (found rediscluster)
			TweakGarbageCollector: func(gc *GarbageCollector) *GarbageCollector {
				fakeClient := cfake.NewClientBuilder()
				fakeClient.WithScheme(scheme)
				fakeClient.WithLists(&corev1.PodList{
					Items: []corev1.Pod{
						createPodWithLabels("testpod1", map[string]string{
							rapi.ClusterNameLabelKey: "rediscluster1",
						}),
						createPodWithLabels("testpod2", map[string]string{
							rapi.ClusterNameLabelKey: "rediscluster1",
						}),
						createPodWithLabels("testpod3", map[string]string{
							rapi.ClusterNameLabelKey: "rediscluster2",
						}),
						createPodWithLabels("testpod4", map[string]string{
							rapi.ClusterNameLabelKey: "rediscluster2",
						}),
					},
				})
				fakeClient.WithLists(&corev1.ServiceList{
					Items: []corev1.Service{
						createServiceWithLabels("testsvc1", map[string]string{
							rapi.ClusterNameLabelKey: "rediscluster1",
						}),
						createServiceWithLabels("testsvc2", map[string]string{
							rapi.ClusterNameLabelKey: "rediscluster2",
						}),
					},
				})
				fakeClient.WithObjects(createRedisCluster("rediscluster2"))
				gc.kubeClient = fakeClient.Build()
				return gc
			},
			errorMessage: "",
		},
		"no pods and services": {
			errorMessage: "",
		},
		"error getting pod with label without value": {
			TweakGarbageCollector: func(gc *GarbageCollector) *GarbageCollector {
				fakeClient := cfake.NewClientBuilder()
				fakeClient.WithScheme(scheme)
				fakeClient.WithLists(&corev1.PodList{
					Items: []corev1.Pod{
						createPodWithLabels("testpod", map[string]string{rapi.ClusterNameLabelKey: ""}),
					},
				})
				gc.kubeClient = fakeClient.Build()
				return gc
			},
			errorMessage: "Unable to find rediscluster name for pod: /testpod",
		},
		"error getting service with label without value": {
			TweakGarbageCollector: func(gc *GarbageCollector) *GarbageCollector {
				fakeClient := cfake.NewClientBuilder()
				fakeClient.WithScheme(scheme)
				fakeClient.WithLists(&corev1.ServiceList{
					Items: []corev1.Service{
						createServiceWithLabels("testservice", map[string]string{rapi.ClusterNameLabelKey: ""}),
					},
				})
				gc.kubeClient = fakeClient.Build()
				return gc
			},
			errorMessage: "Unable to find rediscluster name for service: /testservice",
		},
		"no rediscluster found for pod": {
			TweakGarbageCollector: func(gc *GarbageCollector) *GarbageCollector {
				fakeClient := cfake.NewClientBuilder()
				fakeClient.WithScheme(scheme)
				fakeClient.WithLists(&corev1.PodList{
					Items: []corev1.Pod{
						createPodWithLabels("testpod", map[string]string{rapi.ClusterNameLabelKey: "foo"}),
					},
				})
				gc.kubeClient = fakeClient.Build()
				return gc
			},
			errorMessage: "",
		},
	}
	for tn, tt := range tests {
		t.Run(tn, func(t *testing.T) {
			GC := NewGarbageCollector(cfake.NewClientBuilder().Build())
			if tt.TweakGarbageCollector != nil {
				GC = tt.TweakGarbageCollector(GC)
			}
			err := GC.CollectRedisClusterGarbage()
			errorMessage := ""
			if err != nil {
				errorMessage = err.Error()
			}
			if tt.errorMessage != errorMessage {
				t.Errorf("%q\nExpected error: `%s`\nBut got       : `%s`\n", tn, tt.errorMessage, errorMessage)
			}
		})
	}
}

func createPodWithLabels(name string, labels map[string]string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: corev1.PodSpec{},
	}
}

func createServiceWithLabels(name string, labels map[string]string) corev1.Service {
	return corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: corev1.ServiceSpec{},
	}
}

func createRedisCluster(name string) *rapi.RedisCluster {
	return &rapi.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
}
