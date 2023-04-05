package controller

import (
	"testing"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/gogo/protobuf/proto"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPodDisruptionBudgetsControl_desiredRedisClusterPodDisruptionBudget(t *testing.T) {
	boolPtr := func(value bool) *bool {
		return &value
	}

	type fields struct {
		KubeClient client.Client
		Recorder   record.EventRecorder
	}
	type args struct {
		redisCluster *rapi.RedisCluster
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *policyv1.PodDisruptionBudget
		wantErr bool
	}{
		{
			name: "3 primary, replica=1",
			args: args{
				redisCluster: &rapi.RedisCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name: "rediscluster",
					},
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
				},
			},
			want: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name: "rediscluster",
					Labels: map[string]string{
						rapi.ClusterNameLabelKey: "rediscluster",
					},
					Annotations: map[string]string{},
					OwnerReferences: []metav1.OwnerReference{
						{
							Name:       "rediscluster",
							APIVersion: rapi.GroupVersion.String(),
							Kind:       rapi.ResourceKind,
							Controller: boolPtr(true),
						},
					},
				},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MinAvailable: &intstr.IntOrString{
						IntVal: 5,
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							rapi.ClusterNameLabelKey: "rediscluster",
						},
					},
				},
			},
		},
		{
			name: "2 primary, replica=1",
			args: args{
				redisCluster: &rapi.RedisCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name: "rediscluster",
					},
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(2),
						ReplicationFactor: proto.Int32(1),
					},
				},
			},
			want: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name: "rediscluster",
					Labels: map[string]string{
						rapi.ClusterNameLabelKey: "rediscluster",
					},
					Annotations: map[string]string{},
					OwnerReferences: []metav1.OwnerReference{
						{
							Name:       "rediscluster",
							APIVersion: rapi.GroupVersion.String(),
							Kind:       rapi.ResourceKind,
							Controller: boolPtr(true),
						},
					},
				},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MinAvailable: &intstr.IntOrString{
						IntVal: 3,
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							rapi.ClusterNameLabelKey: "rediscluster",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &PodDisruptionBudgetsControl{
				KubeClient: tt.fields.KubeClient,
				Recorder:   tt.fields.Recorder,
			}
			got, err := s.desiredRedisClusterPodDisruptionBudget(tt.args.redisCluster)
			if (err != nil) != tt.wantErr {
				t.Errorf("PodDisruptionBudgetsControl.desiredRedisClusterPodDisruptionBudget() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !equality.Semantic.DeepEqual(got, tt.want) {
				t.Errorf("PodDisruptionBudgetsControl.desiredRedisClusterPodDisruptionBudget() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
