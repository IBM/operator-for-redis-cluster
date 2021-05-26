package clustering

import (
	"reflect"
	"sort"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis/fake/admin"
)

func TestDispatchSlotToPrimary(t *testing.T) {
	simpleAdmin := admin.NewFakeAdmin()
	pod1 := newPod("pod1", "node1")
	pod2 := newPod("pod2", "node2")
	pod3 := newPod("pod3", "node3")
	pod4 := newPod("pod4", "node4")

	primaryRole := "primary"

	redisNode1 := &redis.Node{ID: "1", Role: primaryRole, Zone: "zone1", IP: "1.1.1.1", Port: "1234", Slots: redis.BuildSlotSlice(0, simpleAdmin.GetHashMaxSlot()), Pod: pod1}
	redisNode2 := &redis.Node{ID: "2", Role: primaryRole, Zone: "zone2", IP: "1.1.1.2", Port: "1234", Slots: redis.SlotSlice{}, Pod: pod2}
	redisNode3 := &redis.Node{ID: "3", Role: primaryRole, Zone: "zone3", IP: "1.1.1.3", Port: "1234", Slots: redis.SlotSlice{}, Pod: pod3}
	redisNode4 := &redis.Node{ID: "4", Role: primaryRole, Zone: "zone4", IP: "1.1.1.4", Port: "1234", Slots: redis.SlotSlice{}, Pod: pod4}

	kubeNodes := []v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node1",
				Labels: map[string]string{
					v1.LabelTopologyZone: "zone1",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node2",
				Labels: map[string]string{
					v1.LabelTopologyZone: "zone2",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node3",
				Labels: map[string]string{
					v1.LabelTopologyZone: "zone3",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node4",
				Labels: map[string]string{
					v1.LabelTopologyZone: "zone4",
				},
			},
		},
	}

	testCases := []struct {
		cluster     *redis.Cluster
		nodes       redis.Nodes
		nbPrimaries int32
		err         bool
	}{
		// append force copy, because DispatchSlotToPrimary updates the slice
		{
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
			nodes: redis.Nodes{
				redisNode1,
				redisNode2,
				redisNode3,
				redisNode4,
			},
			nbPrimaries: 6, err: true,
		},
		// not enough primaries
		{
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
			nodes: redis.Nodes{
				redisNode1,
				redisNode2,
				redisNode3,
				redisNode4,
			},
			nbPrimaries: 2, err: false,
		},
		// initial config
		{
			cluster: &redis.Cluster{
				Name:      "clustertest",
				Namespace: "default",
				Nodes: map[string]*redis.Node{
					"1": redisNode1,
				},
				KubeNodes: kubeNodes,
			},
			nodes: redis.Nodes{
				redisNode1,
			},
			nbPrimaries: 1, err: false,
		},
		// only one node with no slots
		{
			cluster: &redis.Cluster{
				Name:      "clustertest",
				Namespace: "default",
				Nodes: map[string]*redis.Node{
					"2": redisNode2,
				},
				KubeNodes: kubeNodes,
			},
			nodes: redis.Nodes{
				redisNode2,
			},
			nbPrimaries: 1, err: false,
		},
		// empty
		{
			cluster: &redis.Cluster{
				Name:      "clustertest",
				Namespace: "default",
				KubeNodes: kubeNodes,
			},
			nodes: redis.Nodes{}, nbPrimaries: 0, err: false,
		},
	}

	for i, tc := range testCases {
		_, _, _, err := SelectPrimaries(tc.cluster, tc.nodes, tc.nbPrimaries)
		if (err != nil) != tc.err {
			t.Errorf("[case: %d] Unexpected error status, expected error to be %t, got '%v'", i, tc.err, err)
		}
	}
}

func Test_retrieveLostSlots(t *testing.T) {
	redis1 := &redis.Node{ID: "redis1"}
	redis2 := &redis.Node{ID: "redis2"}
	redis3MissingSlots := &redis.Node{ID: "redis3"}
	nbslots := 16384
	for i := 0; i < nbslots; i++ {
		if i < (nbslots / 2) {
			redis1.Slots = append(redis1.Slots, redis.Slot(i))
		} else {
			redis2.Slots = append(redis2.Slots, redis.Slot(i))
			if i != 16383 {
				redis3MissingSlots.Slots = append(redis3MissingSlots.Slots, redis.Slot(i))
			}
		}
	}
	type args struct {
		oldPrimaryNodes redis.Nodes
		nbSlots         int
	}
	tests := []struct {
		name string
		args args
		want redis.SlotSlice
	}{
		{
			name: "no lost slots",
			args: args{
				oldPrimaryNodes: redis.Nodes{
					redis1,
					redis2,
				},
				nbSlots: nbslots,
			},
			want: redis.SlotSlice{},
		},
		{
			name: "one lost slot",
			args: args{
				oldPrimaryNodes: redis.Nodes{
					redis1,
					redis3MissingSlots,
				},
				nbSlots: nbslots,
			},
			want: redis.SlotSlice{16383},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retrieveLostSlots(tt.args.oldPrimaryNodes, tt.args.nbSlots); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("retrieveLostSlots() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buildSlotByNodeFromAvailableSlots(t *testing.T) {
	redis1 := &redis.Node{ID: "redis1", Slots: redis.SlotSlice{1, 2, 3, 4, 5, 6, 7, 8, 9}}
	redis2 := &redis.Node{ID: "redis2", Slots: redis.SlotSlice{}}
	redis3 := &redis.Node{ID: "redis3", Slots: redis.SlotSlice{}}

	type args struct {
		newPrimaryNodes     redis.Nodes
		nbSlotByNode        int
		slotToMigrateByNode map[string]redis.SlotSlice
	}
	tests := []struct {
		name string
		args args
		// we don't know on which nodes the slots will be assign (due to map ordering).
		// the only think that we know is the repartition between nodes (nb slots by node)
		want []redis.SlotSlice
	}{
		{
			name: "no nodes",
			args: args{
				newPrimaryNodes:     redis.Nodes{},
				nbSlotByNode:        0,
				slotToMigrateByNode: map[string]redis.SlotSlice{},
			},
			want: []redis.SlotSlice{},
		},
		{
			name: "no slot to dispatch",
			args: args{
				newPrimaryNodes: redis.Nodes{
					&redis.Node{ID: "redis1", Slots: redis.SlotSlice{1, 2, 3}},
					&redis.Node{ID: "redis2", Slots: redis.SlotSlice{4, 5, 6}},
					&redis.Node{ID: "redis3", Slots: redis.SlotSlice{7, 8, 9}},
				},
				nbSlotByNode: 3,
				slotToMigrateByNode: map[string]redis.SlotSlice{
					redis1.ID: {},
					redis2.ID: {},
					redis3.ID: {},
				},
			},
			want: []redis.SlotSlice{},
		},
		{
			name: "scale from 1 node to 3 nodes",
			args: args{
				newPrimaryNodes: redis.Nodes{redis1, redis2, redis3},
				nbSlotByNode:    3,
				slotToMigrateByNode: map[string]redis.SlotSlice{
					redis1.ID: {4, 5, 6, 7, 8, 9},
					redis2.ID: {},
					redis3.ID: {},
				},
			},
			want: []redis.SlotSlice{
				// we don't know on which nodes the slots will be assign (due to map ordering).
				// the only think that we know is the repartition between nodes (nb slots by node)
				{4, 5, 6},
				{7, 8, 9},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildSlotByNodeFromAvailableSlots(tt.args.newPrimaryNodes, tt.args.nbSlotByNode, tt.args.slotToMigrateByNode)
			gotslice := []redis.SlotSlice{}
			for _, slots := range got {
				gotslice = append(gotslice, slots)
			}
			sort.Slice(gotslice, func(a, b int) bool {
				i := gotslice[a]
				j := gotslice[b]
				if len(i) == 0 && len(j) == 0 {
					return false
				}
				if len(i) < len(j) {
					return true
				} else if len(j) >= 1 {
					if i[0] < j[0] {
						return true
					}
				}
				return false
			})

			if !reflect.DeepEqual(gotslice, tt.want) {
				t.Errorf("buildSlotByNodeFromAvailableSlots() = %v, want %v", gotslice, tt.want)
			}
		})
	}
}

func Test_buildSlotsByNode(t *testing.T) {
	redis1 := &redis.Node{ID: "redis1", Slots: redis.SlotSlice{0, 1, 2, 3, 4, 5, 6, 7, 8}}
	redis2 := &redis.Node{ID: "redis2", Slots: redis.SlotSlice{}}
	redis3 := &redis.Node{ID: "redis3", Slots: redis.SlotSlice{}}

	redis4 := &redis.Node{ID: "redis4", Slots: redis.SlotSlice{0, 1, 2, 3, 4}}
	redis5 := &redis.Node{ID: "redis5", Slots: redis.SlotSlice{5, 6, 7, 8}}

	type args struct {
		newPrimaryNodes redis.Nodes
		oldPrimaryNodes redis.Nodes
		allPrimaryNodes redis.Nodes
		nbSlots         int
	}
	tests := []struct {
		name string
		args args
		want map[string]int
	}{
		{
			name: "2 new nodes",
			args: args{
				newPrimaryNodes: redis.Nodes{redis1, redis2, redis3},
				oldPrimaryNodes: redis.Nodes{redis1},
				allPrimaryNodes: redis.Nodes{redis1, redis2, redis3},
				nbSlots:         9,
			},
			want: map[string]int{
				redis2.ID: 3,
				redis3.ID: 3,
			},
		},
		{
			name: "1 new node",
			args: args{
				newPrimaryNodes: redis.Nodes{redis1, redis2},
				oldPrimaryNodes: redis.Nodes{redis1},
				allPrimaryNodes: redis.Nodes{redis1, redis2},
				nbSlots:         9,
			},
			want: map[string]int{
				redis2.ID: 4,
			},
		},
		{
			name: "2 new nodes, one removed",
			args: args{
				newPrimaryNodes: redis.Nodes{redis4, redis2, redis3},
				oldPrimaryNodes: redis.Nodes{redis4, redis5},
				allPrimaryNodes: redis.Nodes{redis4, redis2, redis3, redis5},
				nbSlots:         9,
			},
			want: map[string]int{
				redis2.ID: 3,
				redis3.ID: 3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildSlotsByNode(tt.args.newPrimaryNodes, tt.args.oldPrimaryNodes, tt.args.allPrimaryNodes, tt.args.nbSlots)
			gotSlotByNodeID := make(map[string]int)
			for id, slots := range got {
				gotSlotByNodeID[id] = len(slots)
			}
			if !reflect.DeepEqual(gotSlotByNodeID, tt.want) {
				t.Errorf("buildSlotsByNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_feedMigInfo(t *testing.T) {
	redis1 := &redis.Node{ID: "redis1", Slots: redis.SlotSlice{0, 1, 2, 3, 4, 5, 6, 7, 8}}
	redis2 := &redis.Node{ID: "redis2", Slots: redis.SlotSlice{}}
	redis3 := &redis.Node{ID: "redis3", Slots: redis.SlotSlice{}}

	type args struct {
		newPrimaryNodes redis.Nodes
		oldPrimaryNodes redis.Nodes
		allPrimaryNodes redis.Nodes
		nbSlots         int
	}
	tests := []struct {
		name       string
		args       args
		wantMapOut mapSlotByMigInfo
	}{
		{
			name: "basic usecase",
			args: args{
				newPrimaryNodes: redis.Nodes{redis1, redis2, redis3},
				oldPrimaryNodes: redis.Nodes{redis1},
				allPrimaryNodes: redis.Nodes{redis1, redis2, redis3},
				nbSlots:         9,
			},
			wantMapOut: mapSlotByMigInfo{
				migrationInfo{From: redis1, To: redis2}: redis.SlotSlice{3, 4, 5},
				migrationInfo{From: redis1, To: redis3}: redis.SlotSlice{6, 7, 8},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotMapOut, _ := feedMigInfo(tt.args.newPrimaryNodes, tt.args.oldPrimaryNodes, tt.args.allPrimaryNodes, tt.args.nbSlots); !reflect.DeepEqual(gotMapOut, tt.wantMapOut) {
				t.Errorf("feedMigInfo() = %v, want %v", gotMapOut, tt.wantMapOut)
			}
		})
	}
}
