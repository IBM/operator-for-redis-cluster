package controller

import (
	"context"
	"reflect"

	"github.com/IBM/operator-for-redis-cluster/internal/testutil"

	"github.com/gogo/protobuf/proto"

	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis/fake/admin"
	kapiv1 "k8s.io/api/core/v1"
	kmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var scheme *runtime.Scheme

func init() {
	scheme = runtime.NewScheme()

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rapi.AddToScheme(scheme))
}

func Test_selectNewReplicasByZone(t *testing.T) {
	_, redisPrimary1 := testutil.NewRedisPrimaryNode("primary1", "zone1", "pod1", "node1", []string{"1"})

	_, redisReplica1 := testutil.NewRedisReplicaNode("replica1", "zone2", redisPrimary1.ID, "pod2", "node2")
	_, redisReplica2 := testutil.NewRedisPrimaryNode("replica2", "zone3", "pod3", "node3", []string{})
	_, redisReplica3 := testutil.NewRedisPrimaryNode("replica3", "zone1", "pod4", "node1", []string{})
	_, redisReplica4 := testutil.NewRedisPrimaryNode("replica4", "zone2", "pod5", "node2", []string{})

	type args struct {
		zones            []string
		primary          *redis.Node
		nodes            redis.Nodes
		nbReplicasNeeded int32
	}
	tests := []struct {
		name    string
		args    args
		want    redis.Nodes
		wantErr bool
	}{
		{
			name: "no replicas needed",
			args: args{
				zones:            []string{"zone1", "zone2"},
				primary:          &redisPrimary1,
				nodes:            redis.Nodes{&redisPrimary1, &redisReplica1},
				nbReplicasNeeded: 0,
			},
			want:    redis.Nodes{},
			wantErr: false,
		},
		{
			name: "one replica in same zone",
			args: args{
				zones:            []string{"zone1", "zone2"},
				primary:          &redisPrimary1,
				nodes:            redis.Nodes{&redisPrimary1, &redisReplica1, &redisReplica3},
				nbReplicasNeeded: 1,
			},
			want:    redis.Nodes{&redisReplica3},
			wantErr: false,
		},
		{
			name: "one replica in different zone",
			args: args{
				zones:            []string{"zone1", "zone2", "zone3"},
				primary:          &redisPrimary1,
				nodes:            redis.Nodes{&redisPrimary1, &redisReplica1, &redisReplica2},
				nbReplicasNeeded: 1,
			},
			want:    redis.Nodes{&redisReplica2},
			wantErr: false,
		},
		{
			name: "one replica in different zone, one in same zone",
			args: args{
				zones:            []string{"zone1", "zone2", "zone3"},
				primary:          &redisPrimary1,
				nodes:            redis.Nodes{&redisPrimary1, &redisReplica1, &redisReplica2, &redisReplica3},
				nbReplicasNeeded: 2,
			},
			want:    redis.Nodes{&redisReplica2, &redisReplica3},
			wantErr: false,
		},
		{
			name: "two replicas in different zones",
			args: args{
				zones:            []string{"zone1", "zone2", "zone3"},
				primary:          &redisPrimary1,
				nodes:            redis.Nodes{&redisPrimary1, &redisReplica2, &redisReplica3, &redisReplica4},
				nbReplicasNeeded: 2,
			},
			want:    redis.Nodes{&redisReplica4, &redisReplica2},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectNewReplicasByZone(tt.args.zones, tt.args.primary, tt.args.nodes, tt.args.nbReplicasNeeded)
			if (err != nil) != tt.wantErr {
				t.Errorf("selectNewReplicasByZone() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("selectNewReplicasByZone() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_selectReplicasToDelete(t *testing.T) {
	_, redisPrimary1 := testutil.NewRedisPrimaryNode("primary1", "zone1", "pod1", "node1", []string{"1"})
	_, redisPrimary2 := testutil.NewRedisPrimaryNode("primary2", "zone2", "pod2", "node2", []string{"2"})
	_, redisPrimary3 := testutil.NewRedisPrimaryNode("primary3", "zone3", "pod3", "node3", []string{"3"})

	_, redisReplica1 := testutil.NewRedisReplicaNode("replica1", "zone2", redisPrimary1.ID, "pod4", "node2")
	_, redisReplica2 := testutil.NewRedisReplicaNode("replica2", "zone3", redisPrimary2.ID, "pod5", "node3")
	_, redisReplica3 := testutil.NewRedisReplicaNode("replica3", "zone1", redisPrimary3.ID, "pod6", "node1")
	_, redisReplica4 := testutil.NewRedisReplicaNode("replica4", "zone1", redisPrimary2.ID, "pod7", "node1")
	_, redisReplica5 := testutil.NewRedisReplicaNode("replica5", "zone3", redisPrimary3.ID, "pod8", "node2")
	_, redisReplica6 := testutil.NewRedisReplicaNode("replica6", "zone2", redisPrimary3.ID, "pod9", "node3")

	node1 := testutil.NewNode("node1", "zone1")
	node2 := testutil.NewNode("node2", "zone2")
	node3 := testutil.NewNode("node3", "zone3")

	type args struct {
		cluster            *redis.Cluster
		primaryID          string
		primaryReplicas    redis.Nodes
		replicas           redis.Nodes
		nbReplicasToDelete int32
	}
	tests := []struct {
		name    string
		args    args
		want    redis.Nodes
		wantErr bool
	}{
		{
			name:    "one replica to delete in same zone as primary",
			want:    redis.Nodes{&redisReplica5},
			wantErr: false,
			args: args{
				cluster: &redis.Cluster{
					Nodes: map[string]*redis.Node{
						"primary1": &redisPrimary1,
						"primary2": &redisPrimary2,
						"primary3": &redisPrimary3,
						"replica1": &redisReplica1,
						"replica2": &redisReplica2,
						"replica3": &redisReplica3,
						"replica4": &redisReplica4,
					},
					KubeNodes: []kapiv1.Node{*node1, *node2, *node3},
				},
				primaryID:          redisPrimary3.ID,
				primaryReplicas:    redis.Nodes{&redisReplica3, &redisReplica5},
				nbReplicasToDelete: 1,
			},
		},
		{
			name:    "one replica to delete in largest zone cluster-wide",
			want:    redis.Nodes{&redisReplica4},
			wantErr: false,
			args: args{
				cluster: &redis.Cluster{
					Nodes: map[string]*redis.Node{
						"primary1": &redisPrimary1,
						"primary2": &redisPrimary2,
						"primary3": &redisPrimary3,
						"replica1": &redisReplica1,
						"replica2": &redisReplica2,
						"replica3": &redisReplica3,
						"replica4": &redisReplica4,
					},
					KubeNodes: []kapiv1.Node{*node1, *node2, *node3},
				},
				primaryID:          redisPrimary2.ID,
				primaryReplicas:    redis.Nodes{&redisReplica2, &redisReplica4},
				replicas:           redis.Nodes{&redisReplica1, &redisReplica2, &redisReplica3, &redisReplica4},
				nbReplicasToDelete: 1,
			},
		},
		{
			name:    "multiple replicas to delete",
			want:    redis.Nodes{&redisReplica5, &redisReplica3},
			wantErr: false,
			args: args{
				cluster: &redis.Cluster{
					Nodes: map[string]*redis.Node{
						"primary1": &redisPrimary1,
						"primary2": &redisPrimary2,
						"primary3": &redisPrimary3,
						"replica1": &redisReplica1,
						"replica2": &redisReplica2,
						"replica3": &redisReplica3,
						"replica4": &redisReplica4,
						"replica5": &redisReplica5,
						"replica6": &redisReplica6,
					},
					KubeNodes: []kapiv1.Node{*node1, *node2, *node3},
				},
				primaryID:          redisPrimary3.ID,
				primaryReplicas:    redis.Nodes{&redisReplica3, &redisReplica5, &redisReplica6},
				replicas:           redis.Nodes{&redisReplica1, &redisReplica2, &redisReplica3, &redisReplica4, &redisReplica5, &redisReplica6},
				nbReplicasToDelete: 2,
			},
		},
		{
			name: "no replica to delete",
			args: args{
				cluster: &redis.Cluster{
					Nodes: map[string]*redis.Node{
						"primary1": &redisPrimary1,
						"primary2": &redisPrimary2,
						"primary3": &redisPrimary3,
						"replica1": &redisReplica1,
						"replica2": &redisReplica2,
						"replica3": &redisReplica3,
					},
					KubeNodes: []kapiv1.Node{*node1, *node2, *node3},
				},
				primaryID:          redisPrimary2.ID,
				primaryReplicas:    redis.Nodes{&redisReplica2, &redisReplica4},
				replicas:           redis.Nodes{&redisReplica1, &redisReplica2, &redisReplica3},
				nbReplicasToDelete: 0,
			},
			want:    redis.Nodes{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectReplicasToDelete(tt.args.cluster, tt.args.primaryID, tt.args.primaryReplicas, tt.args.replicas, tt.args.nbReplicasToDelete)
			if (err != nil) != tt.wantErr {
				t.Errorf("selectReplicasToDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("selectReplicasToDelete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newRedisCluster(t *testing.T) {
	_, redis1 := testutil.NewRedisReplicaNode("redis1", "zone1", "", "pod1", "node1")
	_, redis2 := testutil.NewRedisPrimaryNode("redis2", "zone2", "pod2", "node2", []string{"1"})
	node1 := testutil.NewNode("node1", "zone1")
	node2 := testutil.NewNode("node2", "zone2")
	ctx := context.Background()
	nodes := &kapiv1.NodeList{
		Items: []kapiv1.Node{*node1, *node2},
	}
	fakeKubeClient := fake.NewClientBuilder().WithScheme(scheme).WithLists(nodes).Build()
	fakeAdmin := admin.NewFakeAdmin()
	fakeAdmin.GetClusterInfosRet = admin.ClusterInfosRetType{
		ClusterInfos: &redis.ClusterInfos{
			Infos: map[string]*redis.NodeInfos{
				redis1.ID: {Node: &redis1, Friends: redis.Nodes{&redis2}},
				redis2.ID: {Node: &redis2, Friends: redis.Nodes{&redis1}},
			},
			Status: redis.ClusterInfoConsistent,
		},
		Err: nil,
	}

	type args struct {
		kubeClient client.Client
		admin      redis.AdminInterface
		cluster    *rapi.RedisCluster
	}
	tests := []struct {
		name    string
		args    args
		want    *redis.Cluster
		want1   redis.Nodes
		wantErr bool
	}{
		{
			name: "create redis cluster",
			args: args{
				kubeClient: fakeKubeClient,
				admin:      fakeAdmin,
				cluster: &rapi.RedisCluster{
					ObjectMeta: kmetav1.ObjectMeta{
						Name:      "myCluster",
						Namespace: "myNamespace",
					},
					Status: rapi.RedisClusterStatus{
						Cluster: rapi.RedisClusterState{
							Nodes: []rapi.RedisClusterNode{},
						},
					},
				},
			},
			want: &redis.Cluster{
				Name:      "myCluster",
				Namespace: "myNamespace",
				Nodes: map[string]*redis.Node{
					redis1.ID: &redis1,
					redis2.ID: &redis2,
				},
				KubeNodes: []kapiv1.Node{*node1, *node2},
			},
			want1:   redis.Nodes{&redis1, &redis2},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := newRedisCluster(ctx, tt.args.admin, tt.args.cluster, tt.args.kubeClient)
			if (err != nil) != tt.wantErr {
				t.Errorf("newRedisCluster() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newRedisCluster() got = %#v, want %#v", got, tt.want)
			}
			got1.SortNodes()
			tt.want1.SortNodes()
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("newRedisCluster() got1 = %#v, want %#v", got1, tt.want1)
			}
		})
	}
}

func TestController_applyConfiguration(t *testing.T) {
	redis2 := redis.Node{ID: "redis2", Role: "primary", IP: "10.0.0.2", Zone: "zone2", Pod: testutil.NewPod("pod2", "node2"), Slots: redis.SlotSlice{1}}
	redis3 := redis.Node{ID: "redis3", Role: "primary", IP: "10.0.0.3", Zone: "zone3", Pod: testutil.NewPod("pod3", "node3"), Slots: redis.SlotSlice{}}
	redis4 := redis.Node{ID: "redis4", Role: "primary", IP: "10.0.0.4", Zone: "zone1", Pod: testutil.NewPod("pod4", "node1"), Slots: redis.SlotSlice{}}
	redis1 := redis.Node{ID: "redis1", Role: "replica", PrimaryReferent: redis2.ID, IP: "10.0.0.1", Zone: "zone1", Pod: testutil.NewPod("pod1", "node1")}
	node1 := testutil.NewNode("node1", "zone1")
	node2 := testutil.NewNode("node2", "zone2")
	node3 := testutil.NewNode("node3", "zone3")
	ctx := context.Background()
	nodes := &kapiv1.NodeList{
		Items: []kapiv1.Node{*node1, *node2, *node3},
	}
	fakeKubeClient := fake.NewClientBuilder().WithScheme(scheme).WithLists(nodes).Build()

	type args struct {
		cluster             *rapi.RedisCluster
		updateFakeAdminFunc func(adm *admin.Admin)
	}
	tests := []struct {
		name    string
		args    args
		want    ctrl.Result
		wantErr bool
	}{
		{
			name: "nothing to apply",
			args: args{
				updateFakeAdminFunc: func(adm *admin.Admin) {
					adm.GetClusterInfosRet = admin.ClusterInfosRetType{
						ClusterInfos: &redis.ClusterInfos{
							Infos: map[string]*redis.NodeInfos{
								redis1.ID: {Node: &redis1, Friends: redis.Nodes{&redis2}},
								redis2.ID: {Node: &redis2, Friends: redis.Nodes{&redis1}},
							},
							Status: redis.ClusterInfoConsistent,
						},
						Err: nil,
					}
				},
				cluster: &rapi.RedisCluster{
					ObjectMeta: kmetav1.ObjectMeta{
						Name:      "myCluster",
						Namespace: "myNamespace",
					},
					Spec: rapi.RedisClusterSpec{
						PodTemplate:       &kapiv1.PodTemplateSpec{},
						NumberOfPrimaries: proto.Int32(1),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Conditions: []rapi.RedisClusterCondition{},
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:         1,
							MinReplicationFactor:      1,
							MaxReplicationFactor:      1,
							NumberOfPods:              2,
							NumberOfPodsReady:         2,
							NumberOfRedisNodesRunning: 2,
							Nodes:                     []rapi.RedisClusterNode{},
						},
					},
				},
			},
			want:    ctrl.Result{},
			wantErr: false,
		},
		{
			name: "new primary and replica to apply",
			args: args{
				updateFakeAdminFunc: func(adm *admin.Admin) {
					adm.GetClusterInfosRet = admin.ClusterInfosRetType{
						ClusterInfos: &redis.ClusterInfos{
							Infos: map[string]*redis.NodeInfos{
								redis1.ID: {Node: &redis1, Friends: redis.Nodes{&redis2, &redis3, &redis4}},
								redis2.ID: {Node: &redis2, Friends: redis.Nodes{&redis1, &redis3, &redis4}},
								redis3.ID: {Node: &redis3, Friends: redis.Nodes{&redis2, &redis1, &redis4}},
								redis4.ID: {Node: &redis4, Friends: redis.Nodes{&redis1, &redis2, &redis3}},
							},
							Status: redis.ClusterInfoConsistent,
						},
						Err: nil,
					}
				},
				cluster: &rapi.RedisCluster{
					ObjectMeta: kmetav1.ObjectMeta{
						Name:      "myCluster",
						Namespace: "myNamespace",
					},
					Spec: rapi.RedisClusterSpec{
						PodTemplate:       &kapiv1.PodTemplateSpec{},
						NumberOfPrimaries: proto.Int32(2),
						ReplicationFactor: proto.Int32(1),
					},
					Status: rapi.RedisClusterStatus{
						Conditions: []rapi.RedisClusterCondition{},
						Cluster: rapi.RedisClusterState{
							NumberOfPrimaries:         1,
							MinReplicationFactor:      1,
							MaxReplicationFactor:      1,
							NumberOfPods:              4,
							NumberOfPodsReady:         4,
							NumberOfRedisNodesRunning: 4,
							Nodes:                     []rapi.RedisClusterNode{},
						},
					},
				},
			},
			want:    ctrl.Result{Requeue: true},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Controller{
				client: fakeKubeClient,
			}
			fakeAdmin := admin.NewFakeAdmin()
			tt.args.updateFakeAdminFunc(fakeAdmin)

			got, err := c.applyConfiguration(ctx, fakeAdmin, tt.args.cluster)
			if (err != nil) != tt.wantErr {
				t.Errorf("Controller.applyConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Controller.applyConfiguration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getOldNodesToRemove(t *testing.T) {
	redis1 := &redis.Node{ID: "redis1", Role: "replica", PrimaryReferent: "redis2", IP: "10.0.0.1", Pod: testutil.NewPod("pod1", "node1")}
	redis2 := &redis.Node{ID: "redis2", Role: "primary", IP: "10.0.0.2", Pod: testutil.NewPod("pod2", "node2"), Slots: redis.SlotSlice{1}}
	redis3 := &redis.Node{ID: "redis3", Role: "primary", IP: "10.0.0.3", Pod: testutil.NewPod("pod3", "node3"), Slots: redis.SlotSlice{2}}
	redis4 := &redis.Node{ID: "redis4", Role: "primary", IP: "10.0.0.4", Pod: testutil.NewPod("pod4", "node4"), Slots: redis.SlotSlice{3}}
	redis5 := &redis.Node{ID: "redis5", Role: "replica", PrimaryReferent: "redis3", IP: "10.0.0.5", Pod: testutil.NewPod("pod5", "node5")}
	redis6 := &redis.Node{ID: "redis6", Role: "replica", PrimaryReferent: "redis4", IP: "10.0.0.6", Pod: testutil.NewPod("pod6", "node6")}

	type args struct {
		curPrimaries redis.Nodes
		newPrimaries redis.Nodes
		nodes        redis.Nodes
	}
	tests := []struct {
		name                 string
		args                 args
		wantRemovedPrimaries redis.Nodes
		wantRemoveReplicas   redis.Nodes
	}{
		{
			name: "basic test",
			args: args{
				curPrimaries: redis.Nodes{redis2, redis3, redis4},
				newPrimaries: redis.Nodes{redis2, redis3},
				nodes:        redis.Nodes{redis1, redis2, redis3, redis4, redis5, redis6},
			},
			wantRemovedPrimaries: redis.Nodes{redis4},
			wantRemoveReplicas:   redis.Nodes{redis6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRemovedPrimaries, gotRemoveReplicas := getOldNodesToRemove(tt.args.curPrimaries, tt.args.newPrimaries, tt.args.nodes)
			if !reflect.DeepEqual(gotRemovedPrimaries, tt.wantRemovedPrimaries) {
				t.Errorf("getOldNodesToRemove() gotRemovedPrimaries = %v, want %v", gotRemovedPrimaries, tt.wantRemovedPrimaries)
			}
			if !reflect.DeepEqual(gotRemoveReplicas, tt.wantRemoveReplicas) {
				t.Errorf("getOldNodesToRemove() gotRemoveReplicas = %v, want %v", gotRemoveReplicas, tt.wantRemoveReplicas)
			}
		})
	}
}
