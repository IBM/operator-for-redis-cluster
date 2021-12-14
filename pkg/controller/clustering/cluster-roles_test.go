package clustering

import (
	"context"
	"reflect"
	"testing"

	"github.com/IBM/operator-for-redis-cluster/internal/testutil"

	v1 "k8s.io/api/core/v1"
	kmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis/fake/admin"
)

func TestDispatchReplica(t *testing.T) {
	// TODO currently only test there is no error, more accurate testing is needed
	primaryRole := "primary"
	replicaRole := "replica"
	ctx := context.Background()
	redisNode1 := &redis.Node{ID: "1", Role: primaryRole, IP: "1.1.1.1", Zone: "zone1", Port: "1234", Slots: append(redis.BuildSlotSlice(10, 20), 0), Pod: testutil.NewPod("pod1", "node1")}
	redisNode2 := &redis.Node{ID: "2", Role: primaryRole, IP: "1.1.1.2", Zone: "zone2", Port: "1234", Slots: append(redis.BuildSlotSlice(1, 5), redis.BuildSlotSlice(21, 30)...), Pod: testutil.NewPod("pod2", "node2")}
	redisNode3 := &redis.Node{ID: "3", Role: replicaRole, PrimaryReferent: "1", IP: "1.1.1.3", Zone: "zone2", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod3", "node3")}
	redisNode4 := &redis.Node{ID: "4", Role: replicaRole, PrimaryReferent: "1", IP: "1.1.1.4", Zone: "zone3", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod4", "node4")}
	redisNode5 := &redis.Node{ID: "5", Role: replicaRole, PrimaryReferent: "1", IP: "1.1.1.5", Zone: "zone1", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod5", "node5")}
	redisNode6 := &redis.Node{ID: "6", Role: primaryRole, IP: "1.1.1.6", Zone: "zone3", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod6", "node6")}
	redisNode7 := &redis.Node{ID: "7", Role: primaryRole, IP: "1.1.1.7", Zone: "zone1", Port: "1234", Slots: redis.BuildSlotSlice(31, 40), Pod: testutil.NewPod("pod7", "node7")}
	redisNode8 := &redis.Node{ID: "8", Role: replicaRole, PrimaryReferent: "7", IP: "1.1.1.8", Zone: "zone2", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod8", "node8")}
	redisNode9 := &redis.Node{ID: "9", Role: replicaRole, PrimaryReferent: "7", IP: "1.1.1.9", Zone: "zone3", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod9", "node9")}

	nodes := redis.Nodes{redisNode1, redisNode2, redisNode3, redisNode4, redisNode5, redisNode6, redisNode7, redisNode8, redisNode9}

	c := &redis.Cluster{
		Name:      "clustertest",
		Namespace: "default",
		Nodes: map[string]*redis.Node{
			"1": redisNode1,
			"2": redisNode2,
			"3": redisNode3,
			"4": redisNode4,
			"5": redisNode5,
			"6": redisNode6,
			"7": redisNode7,
			"8": redisNode8,
			"9": redisNode9,
		},
		KubeNodes: []v1.Node{
			{
				ObjectMeta: kmetav1.ObjectMeta{
					Name: "node1",
					Labels: map[string]string{
						v1.LabelTopologyZone: "zone1",
					},
				},
			},
			{
				ObjectMeta: kmetav1.ObjectMeta{
					Name: "node2",
					Labels: map[string]string{
						v1.LabelTopologyZone: "zone2",
					},
				},
			},
			{
				ObjectMeta: kmetav1.ObjectMeta{
					Name: "node3",
					Labels: map[string]string{
						v1.LabelTopologyZone: "zone3",
					},
				},
			},
		},
	}

	err := DispatchReplica(ctx, c, nodes, 2, admin.NewFakeAdmin())
	if err != nil {
		t.Errorf("Unexpected error returned: %v", err)
	}
}

func TestClassifyNodesByRole(t *testing.T) {
	primaryRole := "primary"
	replicaRole := "replica"
	redisNode1 := &redis.Node{ID: "1", Role: primaryRole, IP: "1.1.1.1", Port: "1234", Slots: append(redis.BuildSlotSlice(10, 20), 0), Pod: testutil.NewPod("pod1", "node1")}
	redisNode2 := &redis.Node{ID: "2", Role: primaryRole, IP: "1.1.1.2", Port: "1234", Slots: append(redis.BuildSlotSlice(1, 5), redis.BuildSlotSlice(21, 30)...), Pod: testutil.NewPod("pod2", "node2")}
	redisNode3 := &redis.Node{ID: "3", Role: replicaRole, PrimaryReferent: "1", IP: "1.1.1.3", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod3", "node3")}
	redisNode4 := &redis.Node{ID: "4", Role: replicaRole, PrimaryReferent: "1", IP: "1.1.1.4", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod4", "node4")}
	redisNode5 := &redis.Node{ID: "5", Role: replicaRole, PrimaryReferent: "1", IP: "1.1.1.5", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod5", "node5")}
	redisNode6 := &redis.Node{ID: "6", Role: primaryRole, IP: "1.1.1.6", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod6", "node6")}
	redisNode7 := &redis.Node{ID: "7", Role: primaryRole, IP: "1.1.1.7", Port: "1234", Slots: redis.BuildSlotSlice(31, 40), Pod: testutil.NewPod("pod7", "node7")}
	redisNode8 := &redis.Node{ID: "8", Role: replicaRole, PrimaryReferent: "7", IP: "1.1.1.8", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod8", "node8")}
	redisNode9 := &redis.Node{ID: "9", Role: replicaRole, PrimaryReferent: "7", IP: "1.1.1.9", Port: "1234", Slots: redis.SlotSlice{}, Pod: testutil.NewPod("pod9", "node9")}

	nodes := redis.Nodes{redisNode1, redisNode2, redisNode3, redisNode4, redisNode5, redisNode6, redisNode7, redisNode8, redisNode9}

	type args struct {
		nodes redis.Nodes
	}
	tests := []struct {
		name            string
		args            args
		wantPrimaries   redis.Nodes
		wantReplicas    redis.Nodes
		wantNodesNoRole redis.Nodes
	}{
		{
			name: "Empty input Nodes slice",
			args: args{
				nodes: redis.Nodes{},
			},
			wantPrimaries:   redis.Nodes{},
			wantReplicas:    redis.Nodes{},
			wantNodesNoRole: redis.Nodes{},
		},
		{
			name: "all type of roles",
			args: args{
				nodes: nodes,
			},
			wantPrimaries:   redis.Nodes{redisNode1, redisNode2, redisNode7},
			wantReplicas:    redis.Nodes{redisNode3, redisNode4, redisNode5, redisNode8, redisNode9},
			wantNodesNoRole: redis.Nodes{redisNode6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPrimaries, gotReplicas, gotPrimariesWithNoSlots := ClassifyNodesByRole(tt.args.nodes)
			if !reflect.DeepEqual(gotPrimaries, tt.wantPrimaries) {
				t.Errorf("ClassifyNodes() gotPrimaries = %v, want %v", gotPrimaries, tt.wantPrimaries)
			}
			if !reflect.DeepEqual(gotReplicas, tt.wantReplicas) {
				t.Errorf("ClassifyNodes() gotReplicas = %v, want %v", gotReplicas, tt.wantReplicas)
			}
			if !reflect.DeepEqual(gotPrimariesWithNoSlots, tt.wantNodesNoRole) {
				t.Errorf("ClassifyNodes() gotPrimariesWithNoSlots = %v, want %v", gotPrimariesWithNoSlots, tt.wantNodesNoRole)
			}
		})
	}
}
