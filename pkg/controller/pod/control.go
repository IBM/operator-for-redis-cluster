package pod

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"sigs.k8s.io/controller-runtime/pkg/client"

	kapiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/golang/glog"
)

// RedisClusterControlInterface interface for the RedisClusterPodControl
type RedisClusterControlInterface interface {
	// GetRedisClusterPods return list of Pod attached to a RedisCluster
	GetRedisClusterPods(redisCluster *rapi.RedisCluster) ([]kapiv1.Pod, error)
	// CreatePod used to create a Pod from the RedisCluster pod template
	CreatePod(redisCluster *rapi.RedisCluster) (*kapiv1.Pod, error)
	// CreatePodOnNode used to create a Pod on the given node
	CreatePodOnNode(redisCluster *rapi.RedisCluster, nodeName string) (*kapiv1.Pod, error)
	// DeletePod used to delete a pod from its name
	DeletePod(redisCluster *rapi.RedisCluster, podName string) error
	// DeletePodNow used to delete now (force) a pod from its name
	DeletePodNow(redisCluster *rapi.RedisCluster, podName string) error
}

var _ RedisClusterControlInterface = &RedisClusterControl{}

// RedisClusterControl contains requires accessor to managing the RedisCluster pods
type RedisClusterControl struct {
	KubeClient client.Client
	Recorder   record.EventRecorder
}

// NewRedisClusterControl builds and returns new NewRedisClusterControl instance
func NewRedisClusterControl(client client.Client, rec record.EventRecorder) *RedisClusterControl {
	ctrl := &RedisClusterControl{
		KubeClient: client,
		Recorder:   rec,
	}
	return ctrl
}

// GetRedisClusterPods return list of Pod attached to a RedisCluster
func (p *RedisClusterControl) GetRedisClusterPods(redisCluster *rapi.RedisCluster) ([]kapiv1.Pod, error) {
	selector, err := CreateRedisClusterLabelSelector(redisCluster)
	if err != nil {
		return nil, err
	}
	podList := &kapiv1.PodList{}
	if err = p.KubeClient.List(context.Background(), podList, client.InNamespace(redisCluster.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// CreatePod used to create a Pod from the RedisCluster pod template
func (p *RedisClusterControl) CreatePod(redisCluster *rapi.RedisCluster) (*kapiv1.Pod, error) {
	pod, err := initPod(redisCluster)
	if err != nil {
		return pod, err
	}
	glog.V(6).Infof("CreatePod: %s/%s", redisCluster.Namespace, pod.Name)
	if err = p.KubeClient.Create(context.Background(), pod); err != nil {
		return nil, err
	}
	return pod, nil
}

// CreatePodOnNode used to create a Pod on the given node
func (p *RedisClusterControl) CreatePodOnNode(redisCluster *rapi.RedisCluster, nodeName string) (*kapiv1.Pod, error) {
	pod, err := initPod(redisCluster)
	if err != nil {
		return pod, err
	}
	pod.Spec.NodeName = nodeName
	glog.V(6).Infof("CreatePodOnNode: %s/%s", redisCluster.Namespace, pod.Name)
	if err = p.KubeClient.Create(context.Background(), pod); err != nil {
		return nil, err
	}
	return pod, nil
}

// DeletePod used to delete a pod
func (p *RedisClusterControl) DeletePod(redisCluster *rapi.RedisCluster, podName string) error {
	glog.V(6).Infof("DeletePod: %s/%s", redisCluster.Namespace, podName)
	return p.deletePodGracePeriod(redisCluster, podName, nil)
}

// DeletePodNow used to for delete a pod now
func (p *RedisClusterControl) DeletePodNow(redisCluster *rapi.RedisCluster, podName string) error {
	glog.V(6).Infof("DeletePod: %s/%s", redisCluster.Namespace, podName)
	now := int64(0)
	return p.deletePodGracePeriod(redisCluster, podName, &now)
}

// deletePodGracePeriod used to delete a pod in a given grace period
func (p *RedisClusterControl) deletePodGracePeriod(redisCluster *rapi.RedisCluster, podName string, period *int64) error {
	pod := &kapiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: redisCluster.Namespace,
		},
	}

	var deleteOptions []client.DeleteOption
	if period != nil {
		deleteOptions = append(deleteOptions, client.GracePeriodSeconds(*period))
	}
	return p.KubeClient.Delete(context.Background(), pod, deleteOptions...)
}

func initPod(redisCluster *rapi.RedisCluster) (*kapiv1.Pod, error) {
	if redisCluster == nil {
		return nil, fmt.Errorf("rediscluster nil pointer")
	}

	desiredLabels, err := GetLabelsSet(redisCluster)
	if err != nil {
		return nil, err
	}
	desiredAnnotations, err := GetAnnotationsSet(redisCluster)
	if err != nil {
		return nil, err
	}
	podName := fmt.Sprintf("rediscluster-%s-", redisCluster.Name)
	pod := &kapiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       redisCluster.Namespace,
			Labels:          desiredLabels,
			Annotations:     desiredAnnotations,
			GenerateName:    podName,
			OwnerReferences: []metav1.OwnerReference{BuildOwnerReference(redisCluster)},
		},
	}

	if redisCluster.Spec.PodTemplate == nil {
		return nil, fmt.Errorf("rediscluster[%s/%s] PodTemplate missing", redisCluster.Namespace, redisCluster.Name)
	}
	pod.Spec = *redisCluster.Spec.PodTemplate.Spec.DeepCopy()

	// Generate a MD5 representing the PodSpec send
	hash, err := GenerateMD5Spec(&pod.Spec)
	if err != nil {
		return nil, err
	}
	pod.Annotations[rapi.PodSpecMD5LabelKey] = hash
	return pod, nil
}

// GenerateMD5Spec used to generate the PodSpec MD5 hash
func GenerateMD5Spec(spec *kapiv1.PodSpec) (string, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	hash := md5.New()
	if _, err = io.Copy(hash, bytes.NewReader(b)); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// BuildOwnerReference used to build the OwnerReference from a RedisCluster
func BuildOwnerReference(cluster *rapi.RedisCluster) metav1.OwnerReference {
	controllerRef := metav1.OwnerReference{
		APIVersion: rapi.GroupVersion.String(),
		Kind:       rapi.ResourceKind,
		Name:       cluster.Name,
		UID:        cluster.UID,
		Controller: boolPtr(true),
	}
	return controllerRef
}

func boolPtr(value bool) *bool {
	return &value
}
