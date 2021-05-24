package clustering

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

func TestPlaceMasters(t *testing.T) {
	redisNode1 := &redis.Node{ID: "1", Role: "master", Zone: "zone1", IP: "1.1.1.1", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod1", "node1")}
	redisNode2 := &redis.Node{ID: "2", Role: "master", Zone: "zone1", IP: "1.1.1.2", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod2", "node1")}
	redisNode3 := &redis.Node{ID: "3", Role: "master", Zone: "zone2", IP: "1.1.1.3", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod3", "node2")}
	redisNode4 := &redis.Node{ID: "4", Role: "master", Zone: "zone3", IP: "1.1.1.4", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod4", "node3")}

	redisNode5 := &redis.Node{ID: "2", Role: "master", Zone: "zone2", IP: "1.1.1.2", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod2", "node2")}
	redisNode6 := &redis.Node{ID: "4", Role: "master", Zone: "zone2", IP: "1.1.1.4", Port: "1234", Slots: redis.SlotSlice{}, Pod: newPod("pod4", "node2")}

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
		cluster            *redis.Cluster
		currentMaster      redis.Nodes
		allPossibleMasters redis.Nodes
		nbMaster           int32
	}
	tests := []struct {
		name           string
		args           args
		want           redis.Nodes
		wantBestEffort bool
		wantError      bool
	}{
		{
			name: "found 3 master on 3 nodes, discard node 2 since node 1 and node 2 are on the same k8s node",
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
				currentMaster:      redis.Nodes{},
				allPossibleMasters: redis.Nodes{redisNode1, redisNode2, redisNode3, redisNode4},
				nbMaster:           3,
			},
			want:           redis.Nodes{redisNode1, redisNode3, redisNode4},
			wantBestEffort: false,
			wantError:      false,
		},
		{
			name: "best effort mode, 4 masters on 2 nodes",
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
				currentMaster:      redis.Nodes{},
				allPossibleMasters: redis.Nodes{redisNode1, redisNode2, redisNode3, redisNode6},
				nbMaster:           4,
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
				currentMaster:      redis.Nodes{},
				allPossibleMasters: redis.Nodes{redisNode1, redisNode5},
				nbMaster:           3,
			},
			want:           redis.Nodes{redisNode1, redisNode5},
			wantBestEffort: true,
			wantError:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.args.cluster
			gotNodes, gotBestEffort, gotError := PlaceMasters(c, tt.args.currentMaster, tt.args.allPossibleMasters, tt.args.nbMaster)
			gotNodes = gotNodes.SortNodes()
			tt.want = tt.want.SortNodes()
			if gotBestEffort != tt.wantBestEffort {
				t.Errorf("SmartMastersPlacement() best effort :%v, want:%v", gotBestEffort, tt.wantBestEffort)
			}
			if (gotError != nil && !tt.wantError) || (tt.wantError && gotError == nil) {
				t.Errorf("SmartMastersPlacement() return error:%v, want:%v", gotError, tt.wantError)
			}
			if !reflect.DeepEqual(gotNodes, tt.want) {
				t.Errorf("SmartMastersPlacement() = %v, want %v", gotNodes, tt.want)
			}
		})
	}
}
