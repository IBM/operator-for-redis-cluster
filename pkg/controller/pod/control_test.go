package pod

import (
	"encoding/json"
	"reflect"
	"testing"

	kapiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
)

func Test_initPod(t *testing.T) {
	emptyPodSpecSHA2, _ := GenerateSHA2Spec(&kapiv1.PodSpec{})

	type args struct {
		redisCluster *rapi.RedisCluster
	}
	tests := []struct {
		name    string
		args    args
		want    *kapiv1.Pod
		wantErr bool
	}{
		{
			name: "empty spec",
			args: args{
				redisCluster: &rapi.RedisCluster{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "basic spec",
			args: args{
				redisCluster: &rapi.RedisCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testcluster",
						Namespace: "foo",
					},
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{},
					},
				},
			},
			want: &kapiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "rediscluster-testcluster-",
					Namespace:    "foo",
					OwnerReferences: []metav1.OwnerReference{{
						Name:       "testcluster",
						APIVersion: rapi.GroupVersion.String(),
						Kind:       rapi.ResourceKind,
						Controller: boolPtr(true),
					}},
					Labels:      map[string]string{rapi.ClusterNameLabelKey: "testcluster"},
					Annotations: map[string]string{rapi.PodSpecSHA2LabelKey: string(emptyPodSpecSHA2)},
				},
			},
			wantErr: false,
		},
		{
			name: "annotations merged and passed",
			args: args{
				redisCluster: &rapi.RedisCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testcluster",
						Namespace: "foo",
						Annotations: map[string]string{
							"annotation1": "foo",
						},
					},
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{
									"annotation2": "bar",
								},
							},
						},
					},
				},
			},
			want: &kapiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "rediscluster-testcluster-",
					Namespace:    "foo",
					OwnerReferences: []metav1.OwnerReference{{
						Name:       "testcluster",
						APIVersion: rapi.GroupVersion.String(),
						Kind:       rapi.ResourceKind,
						Controller: boolPtr(true),
					}},
					Labels: map[string]string{rapi.ClusterNameLabelKey: "testcluster"},
					Annotations: map[string]string{
						rapi.PodSpecSHA2LabelKey: string(emptyPodSpecSHA2),
						"annotation1":            "foo",
						"annotation2":            "bar",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := initPod(tt.args.redisCluster)
			if (err != nil) != tt.wantErr {
				t.Errorf("initPod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				bgot, _ := json.Marshal(got)
				bwant, _ := json.Marshal(tt.want)
				t.Errorf("initPod() = %s, want %s", string(bgot), string(bwant))
			}
		})
	}
}
