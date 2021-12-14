package clustering

import (
	"reflect"
	"sort"
	"testing"

	"github.com/IBM/operator-for-redis-cluster/internal/testutil"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis/fake/admin"
)

func TestDispatchSlotToPrimary(t *testing.T) {
	simpleAdmin := admin.NewFakeAdmin()
	_, primary1 := testutil.NewRedisPrimaryNode("1", "zone1", "pod1", "node1", []string{redis.BuildSlotSlice(0, simpleAdmin.GetHashMaxSlot()).String()})
	_, primary2 := testutil.NewRedisPrimaryNode("2", "zone2", "pod2", "node2", []string{""})
	_, primary3 := testutil.NewRedisPrimaryNode("3", "zone3", "pod3", "node3", []string{""})
	_, primary4 := testutil.NewRedisPrimaryNode("4", "zone4", "pod4", "node4", []string{""})

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
		cluster            *redis.Cluster
		currentPrimaries   redis.Nodes
		candidatePrimaries redis.Nodes
		nbPrimaries        int32
		err                bool
	}{
		// append force copy, because DispatchSlotToPrimary updates the slice
		{
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
			currentPrimaries: redis.Nodes{
				&primary1,
			},
			candidatePrimaries: redis.Nodes{
				&primary2,
				&primary3,
				&primary4,
			},
			nbPrimaries: 6, err: true,
		},
		// not enough primaries
		{
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
			currentPrimaries: redis.Nodes{
				&primary1,
			},
			candidatePrimaries: redis.Nodes{
				&primary1,
				&primary2,
				&primary3,
				&primary4,
			},
			nbPrimaries: 2, err: false,
		},
		// initial config
		{
			cluster: &redis.Cluster{
				Name:      "clustertest",
				Namespace: "default",
				Nodes: map[string]*redis.Node{
					"1": &primary1,
				},
				KubeNodes: kubeNodes,
			},
			currentPrimaries: redis.Nodes{
				&primary1,
			},
			nbPrimaries: 1, err: false,
		},
		// only one node with no slots
		{
			cluster: &redis.Cluster{
				Name:      "clustertest",
				Namespace: "default",
				Nodes: map[string]*redis.Node{
					"2": &primary2,
				},
				KubeNodes: kubeNodes,
			},
			candidatePrimaries: redis.Nodes{
				&primary2,
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
			currentPrimaries:   redis.Nodes{},
			candidatePrimaries: redis.Nodes{},
			nbPrimaries:        0, err: false,
		},
	}

	for i, tc := range testCases {
		_, err := SelectPrimaries(tc.cluster, tc.currentPrimaries, tc.candidatePrimaries, tc.nbPrimaries)
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
