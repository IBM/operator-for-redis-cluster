package clustering

import (
	"reflect"
	"testing"

	"github.com/IBM/operator-for-redis-cluster/internal/testutil"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

func TestPlacePrimaries(t *testing.T) {
	_, primary1 := testutil.NewRedisPrimaryNode("1", "zone1", "pod1", "node1", []string{""})
	_, primary2 := testutil.NewRedisPrimaryNode("2", "zone1", "pod2", "node1", []string{""})
	_, primary3 := testutil.NewRedisPrimaryNode("3", "zone2", "pod3", "node2", []string{""})
	_, primary4 := testutil.NewRedisPrimaryNode("4", "zone3", "pod4", "node3", []string{""})

	_, primary5 := testutil.NewRedisPrimaryNode("2", "zone2", "pod2", "node2", []string{""})
	_, primary6 := testutil.NewRedisPrimaryNode("4", "zone2", "pod4", "node2", []string{""})

	node1 := v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			Labels: map[string]string{
				v1.LabelTopologyZone: "zone1",
			},
		},
	}
	node2 := v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
			Labels: map[string]string{
				v1.LabelTopologyZone: "zone2",
			},
		},
	}
	node3 := v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node3",
			Labels: map[string]string{
				v1.LabelTopologyZone: "zone3",
			},
		},
	}

	kubeNodes := []v1.Node{node1, node2, node3}

	type args struct {
		cluster              *redis.Cluster
		currentPrimary       redis.Nodes
		allPossiblePrimaries redis.Nodes
		nbPrimary            int32
	}
	tests := []struct {
		name           string
		args           args
		want           redis.Nodes
		wantBestEffort bool
		wantError      bool
	}{
		{
			name: "found 3 primary on 3 nodes, discard node 2 since node 1 and node 2 are on the same k8s node",
			args: args{
				cluster: &redis.Cluster{
					Name:      "clustertest",
					Namespace: "default",
					Nodes: map[string]*redis.Node{
						"1": &primary1,
						"2": &primary2,
						"3": &primary3,
						"4": &primary4,
					},
					KubeNodes: kubeNodes,
				},
				currentPrimary:       redis.Nodes{},
				allPossiblePrimaries: redis.Nodes{&primary1, &primary2, &primary3, &primary4},
				nbPrimary:            3,
			},
			want:           redis.Nodes{&primary1, &primary3, &primary4},
			wantBestEffort: false,
			wantError:      false,
		},
		{
			name: "best effort mode, 4 primaries on 2 nodes",
			args: args{
				cluster: &redis.Cluster{
					Name:      "clustertest",
					Namespace: "default",
					Nodes: map[string]*redis.Node{
						"1": &primary1,
						"2": &primary2,
						"3": &primary3,
						"4": &primary6,
					},
					KubeNodes: []v1.Node{node1, node2},
				},
				currentPrimary:       redis.Nodes{},
				allPossiblePrimaries: redis.Nodes{&primary1, &primary2, &primary3, &primary6},
				nbPrimary:            4,
			},
			want:           redis.Nodes{&primary1, &primary2, &primary3, &primary6},
			wantBestEffort: true,
			wantError:      false,
		},
		{
			name: "error case not enough node",
			args: args{
				cluster: &redis.Cluster{
					Name:      "clustertest",
					Namespace: "default",
					Nodes: map[string]*redis.Node{
						"1": &primary1,
						"2": &primary5,
					},
					KubeNodes: kubeNodes,
				},
				currentPrimary:       redis.Nodes{},
				allPossiblePrimaries: redis.Nodes{&primary1, &primary5},
				nbPrimary:            3,
			},
			want:           redis.Nodes{&primary1, &primary5},
			wantBestEffort: true,
			wantError:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.args.cluster
			gotNodes, gotBestEffort, gotError := PlacePrimaries(c, tt.args.currentPrimary, tt.args.allPossiblePrimaries, tt.args.nbPrimary)
			gotNodes = gotNodes.SortNodes()
			tt.want = tt.want.SortNodes()
			if gotBestEffort != tt.wantBestEffort {
				t.Errorf("SmartPrimariesPlacement() best effort :%v, want:%v", gotBestEffort, tt.wantBestEffort)
			}
			if (gotError != nil && !tt.wantError) || (tt.wantError && gotError == nil) {
				t.Errorf("SmartPrimariesPlacement() return error:%v, want:%v", gotError, tt.wantError)
			}
			if !reflect.DeepEqual(gotNodes, tt.want) {
				t.Errorf("SmartPrimariesPlacement() = %v, want %v", gotNodes, tt.want)
			}
		})
	}
}
