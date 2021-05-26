package clustering

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

func TestPlacePrimaries(t *testing.T) {
	redisNode1 := &redis.Node{ID: "1", Role: "primary", Zone: "zone1", IP: "1.1.1.1", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod1", "node1")}
	redisNode2 := &redis.Node{ID: "2", Role: "primary", Zone: "zone1", IP: "1.1.1.2", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod2", "node1")}
	redisNode3 := &redis.Node{ID: "3", Role: "primary", Zone: "zone2", IP: "1.1.1.3", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod3", "node2")}
	redisNode4 := &redis.Node{ID: "4", Role: "primary", Zone: "zone3", IP: "1.1.1.4", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod4", "node3")}

	redisNode5 := &redis.Node{ID: "2", Role: "primary", Zone: "zone2", IP: "1.1.1.2", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod2", "node2")}
	redisNode6 := &redis.Node{ID: "4", Role: "primary", Zone: "zone2", IP: "1.1.1.4", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod4", "node2")}

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
						"1": redisNode1,
						"2": redisNode2,
						"3": redisNode3,
						"4": redisNode4,
					},
					KubeNodes: kubeNodes,
				},
				currentPrimary:       redis.Nodes{},
				allPossiblePrimaries: redis.Nodes{redisNode1, redisNode2, redisNode3, redisNode4},
				nbPrimary:            3,
			},
			want:           redis.Nodes{redisNode1, redisNode3, redisNode4},
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
						"1": redisNode1,
						"2": redisNode2,
						"3": redisNode3,
						"4": redisNode6,
					},
					KubeNodes: []v1.Node{node1, node2},
				},
				currentPrimary:       redis.Nodes{},
				allPossiblePrimaries: redis.Nodes{redisNode1, redisNode2, redisNode3, redisNode6},
				nbPrimary:            4,
			},
			want:           redis.Nodes{redisNode1, redisNode2, redisNode3, redisNode6},
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
						"1": redisNode1,
						"2": redisNode5,
					},
					KubeNodes: kubeNodes,
				},
				currentPrimary:       redis.Nodes{},
				allPossiblePrimaries: redis.Nodes{redisNode1, redisNode5},
				nbPrimary:            3,
			},
			want:           redis.Nodes{redisNode1, redisNode5},
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
