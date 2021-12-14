package controller

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/IBM/operator-for-redis-cluster/internal/testutil"

	"github.com/gogo/protobuf/proto"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	kapiv1 "k8s.io/api/core/v1"
	kmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	ctrlpod "github.com/IBM/operator-for-redis-cluster/pkg/controller/pod"
)

func Test_checkReplicationFactor(t *testing.T) {
	type args struct {
		cluster  *rapi.RedisCluster
		rCluster *redis.Cluster
	}
	redisPrimary1, primary1 := testutil.NewRedisPrimaryNode("primary1", "zone1", "pod1", "node1", []string{})
	redisPrimary2, primary2 := testutil.NewRedisPrimaryNode("primary2", "zone2", "pod2", "node2", []string{})
	redisReplica1, replica1 := testutil.NewRedisReplicaNode("replica1", "zone2", primary1.ID, "pod3", "node2")
	redisReplica2, replica2 := testutil.NewRedisReplicaNode("replica2", "zone3", primary1.ID, "pod4", "node3")
	node1 := testutil.NewNode("node1", "zone1")
	node2 := testutil.NewNode("node2", "zone2")
	node3 := testutil.NewNode("node3", "zone3")
	tests := []struct {
		name   string
		args   args
		want   map[string]redis.Nodes
		wantOK bool
	}{
		{
			name:   "On primary no replica, as requested",
			want:   map[string]redis.Nodes{primary1.ID: {}},
			wantOK: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						ReplicationFactor: proto.Int32(0),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							MinReplicationFactor: 0,
							MaxReplicationFactor: 0,
							Nodes:                []rapi.RedisClusterNode{redisPrimary1},
						},
					},
				},
				rCluster: &redis.Cluster{
					Name: "redis-cluster",
					Nodes: map[string]*redis.Node{
						"primary1": &primary1,
					},
					KubeNodes: []kapiv1.Node{
						*node1,
					},
				},
			},
		},
		{
			name:   "On primary no replica, missing one replica",
			want:   map[string]redis.Nodes{primary1.ID: {}},
			wantOK: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							MinReplicationFactor: 0,
							MaxReplicationFactor: 0,
							Nodes:                []rapi.RedisClusterNode{redisPrimary1},
						},
					},
				},
				rCluster: &redis.Cluster{
					Name: "redis-cluster",
					Nodes: map[string]*redis.Node{
						"primary1": &primary1,
					},
					KubeNodes: []kapiv1.Node{*node1},
				},
			},
		},
		{
			name:   "2 primaries, replica=1, missing one replica",
			want:   map[string]redis.Nodes{primary1.ID: {&replica1}, primary2.ID: {}},
			wantOK: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							MinReplicationFactor: 0,
							MaxReplicationFactor: 1,
							Nodes:                []rapi.RedisClusterNode{redisPrimary1, redisPrimary2, redisReplica1},
						},
					},
				},
				rCluster: &redis.Cluster{
					Name: "redis-cluster",
					Nodes: map[string]*redis.Node{
						primary1.ID: &primary1,
						primary2.ID: &primary2,
						replica1.ID: &replica1,
					},
					KubeNodes: []kapiv1.Node{*node1, *node2},
				},
			},
		},
		{
			name:   "1 primary, replica=1, to many replica",
			want:   map[string]redis.Nodes{primary1.ID: {&replica1, &replica2}},
			wantOK: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							MinReplicationFactor: 2,
							MaxReplicationFactor: 2,
							Nodes:                []rapi.RedisClusterNode{redisPrimary1, redisReplica1, redisReplica2},
						},
					},
				},
				rCluster: &redis.Cluster{
					Name: "redis-cluster",
					Nodes: map[string]*redis.Node{
						primary1.ID: &primary1,
						replica1.ID: &replica1,
						replica2.ID: &replica2,
					},
					KubeNodes: []kapiv1.Node{*node1, *node2, *node3},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := checkReplicationFactor(tt.args.cluster, tt.args.rCluster)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkReplicationFactor() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.wantOK {
				t.Errorf("checkReplicationFactor() got1 = %v, want %v", got1, tt.wantOK)
			}
		})
	}
}

func Test_compareStatus(t *testing.T) {
	type args struct {
		old *rapi.RedisClusterStatus
		new *rapi.RedisClusterStatus
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nothing changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{},
			},
			want: false,
		},
		{
			name: "Cluster state changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					Status: rapi.ClusterStatusKO,
				}},
			},
			want: true,
		},
		{
			name: "NumberOfPrimaries changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					NumberOfPrimaries: 5,
				}},
			},
			want: true,
		},
		{
			name: "MaxReplicationFactor changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					MaxReplicationFactor: 5,
				}},
			},
			want: true,
		},
		{
			name: "MinReplicationFactor changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					MinReplicationFactor: 1,
				}},
			},
			want: true,
		},
		{
			name: "NumberOfPods changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					NumberOfPods: 1,
				}},
			},
			want: true,
		},
		{
			name: "NumberOfPodsReady changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					NumberOfPodsReady: 1,
				}},
			},
			want: true,
		},
		{
			name: "NumberOfRedisNodesRunning changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					NumberOfRedisNodesRunning: 5,
				}},
			},
			want: true,
		},
		{
			name: "NodesPlacement changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					NodesPlacement: rapi.NodesPlacementInfoOptimal,
				}},
			},
			want: true,
		},
		{
			name: "len(Nodes) changed",
			args: args{
				old: &rapi.RedisClusterStatus{},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					Nodes: []rapi.RedisClusterNode{{ID: "A"}},
				}},
			},
			want: true,
		},
		{
			name: "Nodes ID changed",
			args: args{
				old: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					Nodes: []rapi.RedisClusterNode{{ID: "B"}},
				}},
				new: &rapi.RedisClusterStatus{Cluster: rapi.RedisClusterState{
					Nodes: []rapi.RedisClusterNode{{ID: "A"}},
				}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareStatus(tt.args.old, tt.args.new); got != tt.want {
				t.Errorf("compareStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_compareNodes(t *testing.T) {
	type args struct {
		nodeA *rapi.RedisClusterNode
		nodeB *rapi.RedisClusterNode
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nothing change",
			args: args{
				nodeA: &rapi.RedisClusterNode{},
				nodeB: &rapi.RedisClusterNode{},
			},
			want: false,
		},
		{
			name: "Ip change",
			args: args{
				nodeA: &rapi.RedisClusterNode{IP: "A"},
				nodeB: &rapi.RedisClusterNode{IP: "B"},
			},
			want: true,
		},
		{
			name: "PodName change",
			args: args{
				nodeA: &rapi.RedisClusterNode{PodName: "A"},
				nodeB: &rapi.RedisClusterNode{PodName: "B"},
			},
			want: true,
		},
		{
			name: "PrimaryRef change",
			args: args{
				nodeA: &rapi.RedisClusterNode{PrimaryRef: "A"},
				nodeB: &rapi.RedisClusterNode{PrimaryRef: "B"},
			},
			want: true,
		},
		{
			name: "Port change",
			args: args{
				nodeA: &rapi.RedisClusterNode{Port: "10"},
				nodeB: &rapi.RedisClusterNode{Port: "20"},
			},
			want: true,
		},
		{
			name: "Role change",
			args: args{
				nodeA: &rapi.RedisClusterNode{Role: rapi.RedisClusterNodeRolePrimary},
				nodeB: &rapi.RedisClusterNode{Role: rapi.RedisClusterNodeRoleReplica},
			},
			want: true,
		},
		{
			name: "Slots change",
			args: args{
				nodeA: &rapi.RedisClusterNode{Slots: []string{"1"}},
				nodeB: &rapi.RedisClusterNode{Slots: []string{"1", "2"}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareNodes(tt.args.nodeA, tt.args.nodeB); got != tt.want {
				t.Errorf("compareNodes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_needMorePods(t *testing.T) {
	type args struct {
		cluster *rapi.RedisCluster
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "no need more pods",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 6, NumberOfPodsReady: 6},
					},
				},
			},
		},
		{
			name: "correct number of pod but all pods not ready",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 6, NumberOfPodsReady: 4},
					},
				},
			},
		},
		{
			name: "missing pods but all pods not ready",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 4, NumberOfPodsReady: 3},
					},
				},
			},
		},
		{
			name: "missing pods and all pods ready",
			want: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 4, NumberOfPodsReady: 4},
					},
				},
			},
		},
		{
			name: "to many pods",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 10, NumberOfPodsReady: 10},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := needMorePods(tt.args.cluster); got != tt.want {
				t.Errorf("needMorePods() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_needLessPods(t *testing.T) {
	type args struct {
		cluster *rapi.RedisCluster
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "no need less pods",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 6, NumberOfPodsReady: 6},
					},
				},
			},
		},
		{
			name: "correct number of pod but all pods not ready",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 6, NumberOfPodsReady: 4},
					},
				},
			},
		},
		{
			name: "missing pods but all pods not ready",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 4, NumberOfPodsReady: 3},
					},
				},
			},
		},
		{
			name: "missing pods and all pods ready",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 4, NumberOfPodsReady: 4},
					},
				},
			},
		},
		{
			name: "to many pods",
			want: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPods: 10, NumberOfPodsReady: 10},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := needLessPods(tt.args.cluster); got != tt.want {
				t.Errorf("needLessPods() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkNumberOfPrimaries(t *testing.T) {
	type args struct {
		cluster *rapi.RedisCluster
	}
	tests := []struct {
		name  string
		args  args
		want  int32
		want1 bool
	}{
		{
			name:  "number of primary OK",
			want:  0,
			want1: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPrimaries: 3},
					},
				},
			},
		},
		{
			name:  "to many primaries",
			want:  3,
			want1: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPrimaries: 6},
					},
				},
			},
		},
		{
			name:  "not enough primaries",
			want:  -3,
			want1: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(6),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{NumberOfPrimaries: 3},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := checkNumberOfPrimaries(tt.args.cluster)
			if got != tt.want {
				t.Errorf("checkNumberOfPrimaries() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("checkNumberOfPrimaries() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_checkShouldDeletePods(t *testing.T) {
	redisPrimary1, primary1 := testutil.NewRedisPrimaryNode("primary1", "zone1", "pod1", "node1", []string{"1"})
	redisPrimary2, primary2 := testutil.NewRedisPrimaryNode("primary2", "zone2", "pod2", "node2", []string{"2"})
	redisPrimary3, primary3 := testutil.NewRedisPrimaryNode("primary3", "zone3", "pod3", "node3", []string{"3"})
	redisPrimary4, primary4 := testutil.NewRedisPrimaryNode("primary4", "zone1", "pod4", "node1", []string{})
	redisReplica1, replica1 := testutil.NewRedisReplicaNode("replica1", "zone2", primary1.ID, "pod5", "node2")
	redisReplica2, replica2 := testutil.NewRedisReplicaNode("replica2", "zone3", primary2.ID, "pod6", "node3")
	redisReplica3, replica3 := testutil.NewRedisReplicaNode("replica3", "zone1", primary3.ID, "pod7", "node1")

	node1 := testutil.NewNode("node1", "zone1")
	node2 := testutil.NewNode("node2", "zone2")
	node3 := testutil.NewNode("node3", "zone3")

	type args struct {
		cluster  *rapi.RedisCluster
		rCluster *redis.Cluster
	}
	tests := []struct {
		name  string
		args  args
		want  []*rapi.RedisClusterNode
		want1 bool
	}{
		{
			name:  "no useless pod",
			want:  []*rapi.RedisClusterNode{},
			want1: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							MinReplicationFactor: 1,
							MaxReplicationFactor: 1,
							NumberOfPods:         6,
							NumberOfPodsReady:    6,
							Nodes: []rapi.RedisClusterNode{
								redisPrimary1, redisPrimary2, redisPrimary3, redisReplica1, redisReplica2, redisReplica3,
							},
						},
					},
				},
				rCluster: &redis.Cluster{
					Name: "redis-cluster",
					Nodes: map[string]*redis.Node{
						primary1.ID: &primary1,
						primary2.ID: &primary2,
						primary3.ID: &primary3,
						replica1.ID: &replica1,
						replica2.ID: &replica2,
						replica3.ID: &replica3,
					},
					KubeNodes: []kapiv1.Node{*node1, *node2, *node3},
				},
			},
		},
		{
			name:  "useless primary pod",
			want:  []*rapi.RedisClusterNode{&redisPrimary4},
			want1: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						NumberOfPrimaries: proto.Int32(3),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							MinReplicationFactor: 1,
							MaxReplicationFactor: 1,
							NumberOfPrimaries:    3,
							NumberOfPods:         7,
							NumberOfPodsReady:    7,
							Nodes: []rapi.RedisClusterNode{
								redisPrimary1, redisPrimary2, redisPrimary3, redisReplica1, redisReplica2, redisReplica3, redisPrimary4,
							},
						},
					},
				},
				rCluster: &redis.Cluster{
					Name: "redis-cluster",
					Nodes: map[string]*redis.Node{
						primary1.ID: &primary1,
						primary2.ID: &primary2,
						primary3.ID: &primary3,
						primary4.ID: &primary4,
						replica1.ID: &replica1,
						replica2.ID: &replica2,
						replica3.ID: &replica3,
					},
					KubeNodes: []kapiv1.Node{*node1, *node2, *node3},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := shouldDeleteNodes(tt.args.cluster, tt.args.rCluster)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("canDeleteNodes() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("canDeleteNodes() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_checkReplicasOfReplica(t *testing.T) {
	primary1 := rapi.RedisClusterNode{ID: "primary1", Slots: []string{"1"}, Role: rapi.RedisClusterNodeRolePrimary}
	primary2 := rapi.RedisClusterNode{ID: "primary2", Slots: []string{"2"}, Role: rapi.RedisClusterNodeRolePrimary}
	primary3 := rapi.RedisClusterNode{ID: "primary3", Slots: []string{"3"}, Role: rapi.RedisClusterNodeRolePrimary}
	replica1 := rapi.RedisClusterNode{ID: "replica1", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "primary1"}
	replica2 := rapi.RedisClusterNode{ID: "replica2", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "primary2"}
	replica3 := rapi.RedisClusterNode{ID: "replica3", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "primary3"}
	node4 := rapi.RedisClusterNode{ID: "node4", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "replica1"}
	node5 := rapi.RedisClusterNode{ID: "node5", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "replica2"}

	type args struct {
		cluster *rapi.RedisCluster
	}
	tests := []struct {
		name  string
		args  args
		want  map[string][]*rapi.RedisClusterNode
		want1 bool
	}{
		{
			name:  "no replica of replica",
			want:  map[string][]*rapi.RedisClusterNode{},
			want1: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							Nodes: []rapi.RedisClusterNode{
								primary1, primary2, primary3, replica1, replica2, replica3,
							},
						},
					},
				},
			},
		},
		{
			name: "2 replicas of replica",
			want: map[string][]*rapi.RedisClusterNode{
				"replica1": {&node4},
				"replica2": {&node5},
			},
			want1: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							Nodes: []rapi.RedisClusterNode{
								primary1, primary2, primary3, replica1, replica2, replica3, node4, node5,
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := checkReplicasOfReplica(tt.args.cluster)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkReplicasOfReplica() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("checkReplicasOfReplica() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_needClusterOperation(t *testing.T) {
	type args struct {
		cluster *rapi.RedisCluster
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "no operation required",
			want: false,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{
							Spec: kapiv1.PodSpec{},
						},
						NumberOfPrimaries: proto.Int32(1),
						ReplicationFactor: proto.Int32(2),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:    1,
							MinReplicationFactor: 2,
							MaxReplicationFactor: 2,
							NumberOfPods:         3,
							NumberOfPodsReady:    3,
							Nodes: []rapi.RedisClusterNode{
								{ID: "Primary1", Role: rapi.RedisClusterNodeRolePrimary},
								{ID: "Replica1", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
								{ID: "Replica2", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
							},
						},
					},
				},
			},
		},
		{
			name: "need rollingupdate",
			want: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{
							Spec: kapiv1.PodSpec{
								Containers: []kapiv1.Container{{Name: "redis", Image: "redis:4.0.6"}},
							},
						},
						NumberOfPrimaries: proto.Int32(1),
						ReplicationFactor: proto.Int32(2),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:    1,
							MinReplicationFactor: 2,
							MaxReplicationFactor: 2,
							NumberOfPods:         3,
							NumberOfPodsReady:    3,
							Nodes: []rapi.RedisClusterNode{
								{ID: "Primary1", Role: rapi.RedisClusterNodeRolePrimary, Pod: newPodWithContainer("pod1", map[string]string{"redis": "redis:4.0.0"})},
								{ID: "Replica1", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1", Pod: newPodWithContainer("pod2", map[string]string{"redis": "redis:4.0.0"})},
								{ID: "Replica2", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1", Pod: newPodWithContainer("pod3", map[string]string{"redis": "redis:4.0.0"})},
							},
						},
					},
				},
			},
		},
		{
			name: "need more pods",
			want: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{
							Spec: kapiv1.PodSpec{},
						},
						NumberOfPrimaries: proto.Int32(2),
						ReplicationFactor: proto.Int32(2),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:    1,
							MinReplicationFactor: 2,
							MaxReplicationFactor: 2,
							NumberOfPods:         3,
							NumberOfPodsReady:    3,
							Nodes: []rapi.RedisClusterNode{
								{ID: "Primary1", Role: rapi.RedisClusterNodeRolePrimary},
								{ID: "Replica1", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
								{ID: "Replica2", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
							},
						},
					},
				},
			},
		},
		{
			name: "need less pods",
			want: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate:       &kapiv1.PodTemplateSpec{},
						NumberOfPrimaries: proto.Int32(1),
						ReplicationFactor: proto.Int32(2),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:    1,
							MinReplicationFactor: 2,
							MaxReplicationFactor: 2,
							NumberOfPods:         4,
							NumberOfPodsReady:    4,
							Nodes: []rapi.RedisClusterNode{
								{ID: "Primary1", Role: rapi.RedisClusterNodeRolePrimary},
								{ID: "Replica1", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
								{ID: "Replica2", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
							},
						},
					},
				},
			},
		},
		{
			name: "not enough primary",
			want: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate:       &kapiv1.PodTemplateSpec{},
						NumberOfPrimaries: proto.Int32(1),
						ReplicationFactor: proto.Int32(2),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:    0,
							MinReplicationFactor: 2,
							MaxReplicationFactor: 2,
							NumberOfPods:         3,
							NumberOfPodsReady:    3,
							Nodes: []rapi.RedisClusterNode{
								{ID: "Primary1", Role: rapi.RedisClusterNodeRolePrimary},
								{ID: "Replica1", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
								{ID: "Replica2", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
							},
						},
					},
				},
			},
		},
		{
			name: "min replica error",
			want: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate:       &kapiv1.PodTemplateSpec{},
						NumberOfPrimaries: proto.Int32(1),
						ReplicationFactor: proto.Int32(2),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:    1,
							MinReplicationFactor: 1,
							MaxReplicationFactor: 2,
							NumberOfPods:         3,
							NumberOfPodsReady:    3,
							Nodes: []rapi.RedisClusterNode{
								{ID: "Primary1", Role: rapi.RedisClusterNodeRolePrimary},
								{ID: "Replica1", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
								{ID: "Replica2", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
							},
						},
					},
				},
			},
		},
		{
			name: "max replica error",
			want: true,
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate:       &kapiv1.PodTemplateSpec{},
						NumberOfPrimaries: proto.Int32(1),
						ReplicationFactor: proto.Int32(2),
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:    1,
							MinReplicationFactor: 2,
							MaxReplicationFactor: 3,
							NumberOfPods:         3,
							NumberOfPodsReady:    3,
							Nodes: []rapi.RedisClusterNode{
								{ID: "Primary1", Role: rapi.RedisClusterNodeRolePrimary},
								{ID: "Replica1", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
								{ID: "Replica2", Role: rapi.RedisClusterNodeRoleReplica, PrimaryRef: "Primary1"},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := needClusterOperation(tt.args.cluster); got != tt.want {
				t.Errorf("needClusterOperation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_comparePodsWithPodTemplate(t *testing.T) {
	Node1 := rapi.RedisClusterNode{ID: "replica1", Pod: newPodWithContainer("pod1", map[string]string{"redis": "redis:4.0.0"})}
	Node1bis := rapi.RedisClusterNode{ID: "primary1", Pod: newPodWithContainer("pod3", map[string]string{"redis": "redis:4.0.0"})}

	Node2 := rapi.RedisClusterNode{ID: "primary2", Pod: newPodWithContainer("pod2", map[string]string{"redis": "redis:4.0.6"})}
	Node2bis := rapi.RedisClusterNode{ID: "primary3", Pod: newPodWithContainer("pod4", map[string]string{"redis": "redis:4.0.6"})}

	type args struct {
		cluster *rapi.RedisCluster
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test, no nodes running",
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{
							Spec: kapiv1.PodSpec{
								Containers: []kapiv1.Container{{Name: "redis", Image: "redis:4.0.0"}},
							},
						},
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							Nodes: []rapi.RedisClusterNode{},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "test, two nodes with same version",
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{
							Spec: kapiv1.PodSpec{
								Containers: []kapiv1.Container{{Name: "redis", Image: "redis:4.0.0"}},
							},
						},
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							Nodes: []rapi.RedisClusterNode{
								Node1,
								Node1bis,
							},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "test, one nodes with different version",
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{
							Spec: kapiv1.PodSpec{
								Containers: []kapiv1.Container{{Name: "redis", Image: "redis:4.0.6"}},
							},
						},
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							Nodes: []rapi.RedisClusterNode{
								Node1,
								Node2,
								Node2bis,
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "test, spec with new container",
			args: args{
				cluster: &rapi.RedisCluster{
					Spec: rapi.RedisClusterSpec{
						PodTemplate: &kapiv1.PodTemplateSpec{
							Spec: kapiv1.PodSpec{
								Containers: []kapiv1.Container{{Name: "redis", Image: "redis:4.0.6"}, {Name: "redis-exporter", Image: "exporter:latest"}},
							},
						},
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							Nodes: []rapi.RedisClusterNode{
								Node2,
								Node2bis,
							},
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := comparePodsWithPodTemplate(tt.args.cluster); got != tt.want {
				t.Errorf("comparePodsWithPodTemplate() = %v, want %v", got, tt.want)

				bwant, _ := json.Marshal(&tt.args.cluster.Spec.PodTemplate.Spec)
				t.Errorf("comparePodsWithPodTemplate() want %s", string(bwant))
				for id, node := range tt.args.cluster.Status.Cluster.Nodes {
					bgot, _ := json.Marshal(&node.Pod.Spec)
					t.Errorf("comparePodsWithPodTemplate() have id:%d %s", id, string(bgot))
				}
			}
		})
	}
}

func Test_comparePodSpec(t *testing.T) {
	podSpec1 := kapiv1.PodSpec{Containers: []kapiv1.Container{{Name: "redis-node", Image: "redis-node:3.0.3"}}}
	podSpec2 := kapiv1.PodSpec{Containers: []kapiv1.Container{{Name: "redis-node", Image: "redis-node:4.0.8"}}}
	podSpec3 := kapiv1.PodSpec{Containers: []kapiv1.Container{{Name: "redis-node", Image: "redis-node:3.0.3"}, {Name: "prometheus", Image: "prometheus-exporter:latest"}}}
	hashspec1, _ := ctrlpod.GenerateMD5Spec(&podSpec1)
	hashspec2, _ := ctrlpod.GenerateMD5Spec(&podSpec2)
	hashspec3, _ := ctrlpod.GenerateMD5Spec(&podSpec3)
	type args struct {
		spec string
		pod  *kapiv1.Pod
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "PodSpecs similar",
			args: args{
				spec: hashspec1,
				pod: &kapiv1.Pod{
					ObjectMeta: kmetav1.ObjectMeta{
						Annotations: map[string]string{rapi.PodSpecMD5LabelKey: string(hashspec1)},
					},
					Spec: podSpec1,
				},
			},
			want: true,
		},
		{
			name: "PodSpecs not equal",
			args: args{
				spec: hashspec1,
				pod: &kapiv1.Pod{
					ObjectMeta: kmetav1.ObjectMeta{
						Annotations: map[string]string{rapi.PodSpecMD5LabelKey: string(hashspec2)},
					},
					Spec: podSpec2},
			},
			want: false,
		},
		{
			name: "additional container",
			args: args{
				spec: hashspec1,
				pod: &kapiv1.Pod{
					ObjectMeta: kmetav1.ObjectMeta{
						Annotations: map[string]string{rapi.PodSpecMD5LabelKey: string(hashspec3)},
					},
					Spec: podSpec3},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := comparePodSpecMD5Hash(tt.args.spec, tt.args.pod); got != tt.want {
				t.Errorf("comparePodSpec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_filterLostNodes(t *testing.T) {
	var pods []kapiv1.Pod
	pods = append(pods, kapiv1.Pod{Status: kapiv1.PodStatus{Reason: "Running"}})
	pods = append(pods, kapiv1.Pod{Status: kapiv1.PodStatus{Reason: "Finished"}})
	pods = append(pods, kapiv1.Pod{Status: kapiv1.PodStatus{Reason: "NodeLost"}})
	ok, ko := filterLostNodes(pods)
	if !(len(ok) == 2 || len(ko) == 1) {
		t.Errorf("filterLostNodes() wrong result ok: %v, ko: %v", ok, ko)
	}
}

func newPodWithContainer(name string, containersInfos map[string]string) *kapiv1.Pod {
	var containers []kapiv1.Container
	for name, image := range containersInfos {
		containers = append(containers, kapiv1.Container{Name: name, Image: image})
	}

	spec := kapiv1.PodSpec{
		Containers: containers,
	}

	hash, _ := ctrlpod.GenerateMD5Spec(&spec)

	pod := &kapiv1.Pod{
		ObjectMeta: kmetav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{rapi.PodSpecMD5LabelKey: string(hash)},
		},
		Spec: spec,
	}

	return pod
}
