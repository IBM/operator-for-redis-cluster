package utils_test

import (
	"math"
	"reflect"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/IBM/operator-for-redis-cluster/internal/testutil"
	"github.com/IBM/operator-for-redis-cluster/pkg/utils"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
)

var (
	redisPrimary1, _ = testutil.NewRedisPrimaryNode("primary1", "zone1", "pod1", "node1", []string{})
	redisPrimary2, _ = testutil.NewRedisPrimaryNode("primary2", "zone2", "pod2", "node2", []string{})
	redisPrimary3, _ = testutil.NewRedisPrimaryNode("primary3", "zone3", "pod3", "node3", []string{})
	redisPrimary4, _ = testutil.NewRedisPrimaryNode("primary4", "zone1", "pod4", "node1", []string{})
	redisReplica1, _ = testutil.NewRedisReplicaNode("replica1", "zone2", redisPrimary1.ID, "pod5", "node2")
	redisReplica2, _ = testutil.NewRedisReplicaNode("replica2", "zone3", redisPrimary1.ID, "pod6", "node3")
	redisReplica3, _ = testutil.NewRedisReplicaNode("replica3", "zone1", redisPrimary2.ID, "pod7", "node3")

	node1 = testutil.NewNode("node1", "zone1")
	node2 = testutil.NewNode("node2", "zone2")
	node3 = testutil.NewNode("node3", "zone3")
	node4 = testutil.NewNode("node4", "zone1")
	node5 = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node5",
			Labels: map[string]string{},
		},
	}
)

func TestIsPodReady(t *testing.T) {
	readyPod := testutil.NewPod("pod1", node1.Name)
	readyPod.Status.Conditions = append(readyPod.Status.Conditions, corev1.PodCondition{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	})
	unreadyPod := testutil.NewPod("pod1", node1.Name)
	unreadyPod.Status.Conditions = append(unreadyPod.Status.Conditions, corev1.PodCondition{
		Type:   corev1.PodReady,
		Status: corev1.ConditionFalse,
	})
	tests := []struct {
		name    string
		pod     *corev1.Pod
		want    bool
		wantErr bool
	}{
		{
			name:    "nil pod",
			pod:     nil,
			want:    false,
			wantErr: true,
		},
		{
			name:    "pod not ready",
			pod:     unreadyPod,
			want:    false,
			wantErr: true,
		},
		{
			name:    "pod is ready",
			pod:     readyPod,
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := utils.IsPodReady(tt.pod)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsPodReady() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("IsPodReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetZoneSkew(t *testing.T) {
	tests := []struct {
		name        string
		zoneToNodes map[string][]string
		want        int
	}{
		{
			name:        "empty",
			zoneToNodes: map[string][]string{},
			want:        0,
		},
		{
			name: "no nodes",
			zoneToNodes: map[string][]string{
				"zone1": {},
				"zone2": {},
				"zone3": {},
			},
			want: 0,
		},
		{
			name: "one extra node in zone",
			zoneToNodes: map[string][]string{
				"zone1": {redisPrimary1.ID, redisPrimary4.ID},
				"zone2": {redisPrimary2.ID},
				"zone3": {redisPrimary3.ID},
			},
			want: 1,
		},
		{
			name: "no nodes in one zone",
			zoneToNodes: map[string][]string{
				"zone1": {},
				"zone2": {redisReplica1.ID},
				"zone3": {redisReplica2.ID},
			},
			want: 1,
		},
		{
			name: "balanced zones",
			zoneToNodes: map[string][]string{
				"zone1": {redisPrimary1.ID},
				"zone2": {redisPrimary2.ID},
				"zone3": {redisPrimary3.ID},
			},
			want: 0,
		},
		{
			name: "greatly unbalanced zones",
			zoneToNodes: map[string][]string{
				"zone1": {redisPrimary1.ID, redisPrimary4.ID, redisReplica3.ID},
				"zone2": {redisPrimary2.ID},
				"zone3": {redisPrimary3.ID},
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := utils.GetZoneSkew(tt.zoneToNodes)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetZoneSkew() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetZone(t *testing.T) {
	tests := []struct {
		name     string
		nodeName string
		nodes    []corev1.Node
		want     string
	}{
		{
			name:     "no nodes",
			nodeName: node1.Name,
			nodes:    []corev1.Node{},
			want:     "unknown",
		},
		{
			name:     "no name",
			nodeName: "",
			nodes:    []corev1.Node{*node1, *node2, *node3},
			want:     "unknown",
		},
		{
			name:     "no zone label",
			nodeName: node1.Name,
			nodes:    []corev1.Node{*node5},
			want:     "unknown",
		},
		{
			name:     "nodes in different zones",
			nodeName: node1.Name,
			nodes:    []corev1.Node{*node1, *node2, *node3},
			want:     "zone1",
		},
		{
			name:     "some nodes in same zone",
			nodeName: node1.Name,
			nodes:    []corev1.Node{*node1, *node2, *node4},
			want:     "zone1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := utils.GetZone(tt.nodeName, tt.nodes)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetZone() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetZones(t *testing.T) {
	tests := []struct {
		name  string
		nodes []corev1.Node
		want  []string
	}{
		{
			name:  "no nodes",
			nodes: []corev1.Node{},
			want:  []string{"unknown"},
		},
		{
			name:  "node with no zone label",
			nodes: []corev1.Node{*node5},
			want:  []string{"unknown"},
		},
		{
			name:  "nodes in different zones",
			nodes: []corev1.Node{*node1, *node2, *node3},
			want:  []string{"zone1", "zone2", "zone3"},
		},
		{
			name:  "some nodes in same zone",
			nodes: []corev1.Node{*node1, *node2, *node4},
			want:  []string{"zone1", "zone2"},
		},
		{
			name:  "one node with unknown zone",
			nodes: []corev1.Node{*node1, *node2, *node3, *node5},
			want:  []string{"unknown", "zone1", "zone2", "zone3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := utils.GetZones(tt.nodes)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetZones() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetZoneSkewByRole(t *testing.T) {
	redisPrimary5, _ := testutil.NewRedisPrimaryNode("primary5", "zone1", "pod8", "node1", []string{})
	tests := []struct {
		name            string
		zoneToPrimaries map[string][]string
		zoneToReplicas  map[string][]string
		want1           int
		want2           int
		ok              bool
	}{
		{
			name:            "empty",
			zoneToPrimaries: map[string][]string{},
			zoneToReplicas:  map[string][]string{},
			want1:           0,
			want2:           0,
			ok:              true,
		},
		{
			name: "only primaries",
			zoneToPrimaries: map[string][]string{
				redisPrimary1.Zone: {redisPrimary1.ID},
				redisPrimary2.Zone: {redisPrimary2.ID},
				redisPrimary3.Zone: {redisPrimary3.ID},
			},
			zoneToReplicas: map[string][]string{},
			want1:          0,
			want2:          0,
			ok:             true,
		},
		{
			name:            "only replicas",
			zoneToPrimaries: map[string][]string{},
			zoneToReplicas: map[string][]string{
				redisReplica1.Zone: {redisReplica1.ID},
				redisReplica2.Zone: {redisPrimary2.ID},
				redisReplica3.Zone: {redisPrimary3.ID},
			},
			want1: 0,
			want2: 0,
			ok:    true,
		},
		{
			name: "balanced primaries and replicas",
			zoneToPrimaries: map[string][]string{
				redisPrimary1.Zone: {redisPrimary1.ID},
				redisPrimary2.Zone: {redisPrimary2.ID},
				redisPrimary3.Zone: {redisPrimary3.ID},
			},
			zoneToReplicas: map[string][]string{
				redisReplica1.Zone: {redisReplica1.ID},
				redisReplica2.Zone: {redisPrimary2.ID},
				redisReplica3.Zone: {redisPrimary3.ID},
			},
			want1: 0,
			want2: 0,
			ok:    true,
		},
		{
			name: "unbalanced primaries and unbalanced replicas",
			zoneToPrimaries: map[string][]string{
				redisPrimary1.Zone: {redisPrimary1.ID},
				redisPrimary2.Zone: {redisPrimary2.ID},
				redisPrimary3.Zone: {},
			},
			zoneToReplicas: map[string][]string{
				redisReplica1.Zone: {},
				redisReplica2.Zone: {redisPrimary2.ID},
				redisReplica3.Zone: {redisPrimary3.ID},
			},
			want1: 1,
			want2: 1,
			ok:    true,
		},
		{
			name: "unbalanced primaries and balanced replicas",
			zoneToPrimaries: map[string][]string{
				redisPrimary1.Zone: {redisPrimary1.ID},
				redisPrimary2.Zone: {redisPrimary2.ID},
				redisPrimary3.Zone: {},
			},
			zoneToReplicas: map[string][]string{
				redisReplica1.Zone: {redisPrimary1.ID},
				redisReplica2.Zone: {redisPrimary2.ID},
				redisReplica3.Zone: {redisPrimary3.ID},
			},
			want1: 1,
			want2: 0,
			ok:    true,
		},
		{
			name: "greatly unbalanced primaries and balanced replicas",
			zoneToPrimaries: map[string][]string{
				redisPrimary1.Zone: {redisPrimary1.ID, redisPrimary4.ID, redisPrimary5.ID},
				redisPrimary2.Zone: {},
				redisPrimary3.Zone: {},
			},
			zoneToReplicas: map[string][]string{
				redisReplica1.Zone: {redisPrimary1.ID},
				redisReplica2.Zone: {redisPrimary2.ID},
				redisReplica3.Zone: {redisPrimary3.ID},
			},
			want1: 3,
			want2: 0,
			ok:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2, ok := utils.GetZoneSkewByRole(tt.zoneToPrimaries, tt.zoneToReplicas)
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("GetZoneSkewByRole() = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("GetZoneSkewByRole() = %v, want %v", got2, tt.want2)
			}
			if !reflect.DeepEqual(ok, tt.ok) {
				t.Errorf("GetZoneSkewByRole() = %v, want %v", ok, tt.ok)
			}
		})
	}
}

func TestZoneToRole(t *testing.T) {
	tests := []struct {
		name  string
		nodes []rapi.RedisClusterNode
		want1 map[string][]string
		want2 map[string][]string
	}{
		{
			name:  "nothing",
			nodes: []rapi.RedisClusterNode{},
			want1: map[string][]string{},
			want2: map[string][]string{},
		},
		{
			name:  "only primaries",
			nodes: []rapi.RedisClusterNode{redisPrimary1, redisPrimary2},
			want1: map[string][]string{
				redisPrimary1.Zone: {redisPrimary1.ID},
				redisPrimary2.Zone: {redisPrimary2.ID},
			},
			want2: map[string][]string{},
		},
		{
			name:  "only replicas",
			nodes: []rapi.RedisClusterNode{redisReplica1, redisReplica2},
			want1: map[string][]string{},
			want2: map[string][]string{
				redisReplica1.Zone: {redisReplica1.ID},
				redisReplica2.Zone: {redisReplica2.ID},
			},
		},
		{
			name:  "primaries and replicas",
			nodes: []rapi.RedisClusterNode{redisPrimary1, redisPrimary2, redisPrimary3, redisPrimary4, redisReplica1, redisReplica2},
			want1: map[string][]string{
				"zone1": {redisPrimary1.ID, redisPrimary4.ID},
				"zone2": {redisPrimary2.ID},
				"zone3": {redisPrimary3.ID},
			},
			want2: map[string][]string{
				redisReplica1.Zone: {redisReplica1.ID},
				redisReplica2.Zone: {redisReplica2.ID},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2 := utils.ZoneToRole(tt.nodes)
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ZoneToRole() = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("ZoneToRole() = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func TestStringToByteString(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{
			name:  "bytes",
			value: "123456",
			want:  "123456",
		},
		{
			name:  "kilobyte",
			value: "10k",
			want:  strconv.Itoa(utils.Kilobyte * 10),
		},
		{
			name:  "kibibyte",
			value: "10kb",
			want:  strconv.Itoa(utils.Kibibyte * 10),
		},
		{
			name:  "megabyte",
			value: "10m",
			want:  strconv.Itoa(int(math.Pow(utils.Kilobyte, 2)) * 10),
		},
		{
			name:  "mebibyte",
			value: "10mb",
			want:  strconv.Itoa(int(math.Pow(utils.Kibibyte, 2)) * 10),
		},
		{
			name:  "gigabyte",
			value: "10g",
			want:  strconv.Itoa(int(math.Pow(utils.Kilobyte, 3)) * 10),
		},
		{
			name:  "gibibyte",
			value: "10gb",
			want:  strconv.Itoa(int(math.Pow(utils.Kibibyte, 3)) * 10),
		},
		{
			name:  "capital unit",
			value: "10K",
			want:  strconv.Itoa(utils.Kilobyte * 10),
		},
		{
			name:  "capital units",
			value: "10KB",
			want:  strconv.Itoa(utils.Kibibyte * 10),
		},
		{
			name:  "mixed capitalization units",
			value: "10Kb",
			want:  strconv.Itoa(utils.Kibibyte * 10),
		},
		{
			name:    "invalid unit",
			value:   "10t",
			wantErr: true,
		},
		{
			name:    "invalid digits",
			value:   "0.5k",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := utils.StringToByteString(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("StringToByteString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("StringToByteString() = %v, want %v", got, tt.want)
			}
		})
	}
}
