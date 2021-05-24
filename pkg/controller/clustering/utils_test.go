package clustering

import (
	kapiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newPod(name, nodeName string) *kapiv1.Pod {
	return &kapiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: kapiv1.PodSpec{NodeName: nodeName}}
}
