package redis

import (
	"reflect"
	"sort"
	"testing"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	kapiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	pod1  = &kapiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "Pod1", Namespace: "ns"}}
	pod2  = &kapiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "Pod2", Namespace: "ns"}}
	pod3  = &kapiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "Pod3", Namespace: "ns"}}
	pod4  = &kapiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "Pod4", Namespace: "ns"}}
	node1 = NewNode("abcd", "1.2.3.1", pod1)
	node2 = NewNode("edfg", "1.2.3.2", pod2)
	node3 = NewNode("igkl", "1.2.3.3", pod3)
	node4 = NewNode("mnop", "1.2.3.4", pod4)
)

func TestNode_ToAPINode(t *testing.T) {
	type fields struct {
		ID              string
		IP              string
		PrimaryReferent string
		Slots           SlotSlice
		Pod             *kapiv1.Pod
	}
	tests := []struct {
		name   string
		fields fields
		want   rapi.RedisClusterNode
	}{
		{
			name: "default test",
			fields: fields{
				ID: "id1",
				IP: "1.2.3.4",
				Pod: &kapiv1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "name1", Namespace: "NS1"},
				},
				Slots: SlotSlice{},
			},
			want: rapi.RedisClusterNode{
				ID: "id1",
				IP: "1.2.3.4",
				Pod: &kapiv1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "name1", Namespace: "NS1"},
				},
				PodName: "name1",
				Role:    rapi.RedisClusterNodeRoleNone,
				Slots:   []string{},
			},
		},
		{
			name: "convert a primary",
			fields: fields{
				ID: "id1",
				IP: "1.2.3.4",
				Pod: &kapiv1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "name1", Namespace: "NS1"},
				},
				Slots: SlotSlice{Slot(1), Slot(2)},
			},
			want: rapi.RedisClusterNode{
				ID: "id1",
				IP: "1.2.3.4",
				Pod: &kapiv1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "name1", Namespace: "NS1"},
				},
				PodName: "name1",
				Role:    rapi.RedisClusterNodeRolePrimary,
				Slots:   []string{},
			},
		},
		{
			name: "convert a replica",
			fields: fields{
				ID: "id1",
				IP: "1.2.3.4",
				Pod: &kapiv1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "name1", Namespace: "NS1"},
				},
				PrimaryReferent: "idPrimary",
				Slots:           SlotSlice{},
			},
			want: rapi.RedisClusterNode{
				ID: "id1",
				IP: "1.2.3.4",
				Pod: &kapiv1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "name1", Namespace: "NS1"},
				},
				PodName: "name1",
				Role:    rapi.RedisClusterNodeRoleReplica,
				Slots:   []string{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &Node{
				ID:              tt.fields.ID,
				IP:              tt.fields.IP,
				Pod:             tt.fields.Pod,
				PrimaryReferent: tt.fields.PrimaryReferent,
				Slots:           tt.fields.Slots,
			}
			if got := n.ToAPINode(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Node.ToAPINode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodes_SortNodes(t *testing.T) {
	sortedNodes := Nodes{node1, node2, node3, node4}
	sort.Sort(sortedNodes)
	unsertedNodes := Nodes{node4, node3, node2, node1}

	tests := []struct {
		name string
		ns   Nodes
		want Nodes
	}{
		{
			name: "empty nodes",
			ns:   Nodes{},
			want: Nodes{},
		},
		{
			name: "already sorted nodes",
			ns:   sortedNodes,
			want: sortedNodes,
		},
		{
			name: "unsorted nodes",
			ns:   unsertedNodes,
			want: sortedNodes,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ns.SortNodes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Nodes.SortNodes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeSetRolePrimaryValid(t *testing.T) {
	node := &Node{}

	flags := "primary"
	err := node.SetRole(flags)
	if err != nil {
		t.Error("Failed to set Primary as role [err]:", err)
	}
	if node.Role != redisPrimaryRole {
		t.Error("Role should be Primary")
	}
}

func TestNodeSetRoleReplicaValid(t *testing.T) {
	node := &Node{}

	flags := "replica"
	err := node.SetRole(flags)
	if err != nil {
		t.Error("Failed to set Replica as role [err]:", err)
	}
	if node.Role != redisReplicaRole {
		t.Error("Role should be Replica")
	}
}

func TestNodeSetRoleNotValid(t *testing.T) {
	node := &Node{}

	flags := "king"
	err := node.SetRole(flags)
	if err == nil {
		t.Error("SetRole should return an error ")
	}

	if node.Role != "" {
		t.Error("Role should be empty current:", node.Role)
	}
}

func TestNodeSetRoleMultFlags(t *testing.T) {
	node := &Node{}

	flags := "myself,replica"
	err := node.SetRole(flags)
	if err != nil {
		t.Error("Failed to set Replica as role [err]:", err)
	}
	if node.Role != redisReplicaRole {
		t.Error("Role should be Replica")
	}
}

func TestNodeSetLinkStatusConnected(t *testing.T) {
	node := &Node{}

	status := "connected"
	err := node.SetLinkStatus(status)
	if err != nil {
		t.Error("Failed to set link status [err]:", err)
	}
	if node.LinkState != RedisLinkStateConnected {
		t.Error("State should be connected")
	}
}

func TestNodeSetLinkStatusDisconnected(t *testing.T) {
	node := &Node{}

	status := "disconnected"
	err := node.SetLinkStatus(status)
	if err != nil {
		t.Error("Failed to set link status [err]:", err)
	}
	if node.LinkState != RedisLinkStateDisconnected {
		t.Error("State should be disconnected")
	}
}

func TestNodeSetLinkStatusKO(t *testing.T) {
	node := &Node{}

	status := "blabla"
	err := node.SetLinkStatus(status)
	if err == nil {
		t.Error("SetLinkStatus should return an error ")
	}

	if node.LinkState != "" {
		t.Error("State should be empty current:", node.LinkState)
	}
}

func TestNodeSetFailureStateFail(t *testing.T) {
	node := &Node{}

	flags := "primary,myself,fail"
	node.SetFailureStatus(flags)

	if !node.HasStatus(NodeStatusFail) {
		t.Error("Failure Status should be NodeStatusFail current:", node.FailStatus)
	}
}

func TestNodeSetFailureStatePFail(t *testing.T) {
	node := &Node{}

	flags := "primary,myself,fail?"
	node.SetFailureStatus(flags)

	if !node.HasStatus(NodeStatusPFail) {
		t.Error("Failure Status should be NodeStatusFail current:", node.FailStatus)
	}
}

func TestNodeSetFailureStateOK(t *testing.T) {
	node := &Node{}

	flags := "primary,myself"
	node.SetFailureStatus(flags)

	if len(node.FailStatus) > 0 {
		t.Error("Failure Status should be empty current:", node.FailStatus)
	}
}

func TestNodeSliceTestSearchInSlde(t *testing.T) {
	node := &Node{}

	flags := "primary,myself"
	node.SetFailureStatus(flags)

	if len(node.FailStatus) > 0 {
		t.Error("Failure Status should be empty current:", node.FailStatus)
	}
}

func TestNodeSetReferentPrimary(t *testing.T) {
	node := &Node{}

	ref := "899809809808343434342323"
	node.SetPrimaryReferent(ref)
	if node.PrimaryReferent != ref {
		t.Error("Node PrimaryReferent is not correct [current]:", node.PrimaryReferent)
	}
}

func TestNodeSetReferentPrimaryNone(t *testing.T) {
	node := &Node{}

	ref := "-"
	node.SetPrimaryReferent(ref)
	if node.PrimaryReferent != "" {
		t.Error("Node PrimaryReferent should be empty  [current]:", node.PrimaryReferent)
	}
}
func TestNodeWhereP(t *testing.T) {
	var slice Nodes
	nodePrimary := &Node{ID: "A", Role: redisPrimaryRole, Slots: SlotSlice{0, 1, 4, 10}}
	slice = append(slice, nodePrimary)
	nodeReplica := &Node{ID: "B", Role: redisReplicaRole, Slots: SlotSlice{}}
	slice = append(slice, nodeReplica)
	nodeUnset := &Node{ID: "C", Role: redisPrimaryRole, Slots: SlotSlice{}}
	slice = append(slice, nodeUnset)

	primarySlice, err := slice.GetNodesByFunc(IsPrimaryWithSlot)
	if err != nil {
		t.Error("slice.GetNodesByFunc(IsPrimaryWithSlot) sould not return an error, current err:", err)
	}
	if len(primarySlice) != 1 {
		t.Error("primarySlice should have a size of 1, current:", len(primarySlice))
	}
	if primarySlice[0].ID != "A" {
		t.Error("primarySlice[0].ID should be A current:", primarySlice[0].ID)
	}

	unsetSlice, err := slice.GetNodesByFunc(IsPrimaryWithNoSlot)
	if err != nil {
		t.Error("slice.GetNodesByFunc(IsPrimaryWithSlot) sould not return an error, current err:", err)
	}
	if len(unsetSlice) != 1 {
		t.Error("unsetSlice should have a size of 1, current:", len(unsetSlice))
	}
	if unsetSlice[0].ID != "C" {
		t.Error("unsetSlice[0].ID should should be C current:", unsetSlice[0].ID)
	}

	replicaSlice, err := slice.GetNodesByFunc(IsReplica)
	if err != nil {
		t.Error("slice.GetNodesByFunc(IsPrimaryWithSlot) sould not return an error, current err:", err)
	}
	if len(replicaSlice) != 1 {
		t.Error("replicaSlice should have a size of 1, current:", len(replicaSlice))
	}
	if replicaSlice[0].ID != "B" {
		t.Error("replicaSlice[0].ID should should be B current:", replicaSlice[0].ID)
	}
}

func TestSearchNodeByID(t *testing.T) {
	var slice Nodes
	nodePrimary := &Node{ID: "A", Role: redisPrimaryRole, Slots: SlotSlice{0, 1, 4, 10}}
	slice = append(slice, nodePrimary)
	nodeReplica := &Node{ID: "B", Role: redisReplicaRole, Slots: SlotSlice{}}
	slice = append(slice, nodeReplica)
	nodeUnset := &Node{ID: "C", Role: redisPrimaryRole, Slots: SlotSlice{}}
	slice = append(slice, nodeUnset)

	// empty list
	_, err := Nodes{}.GetNodeByID("B")
	if err == nil {
		t.Errorf("With an empty list, GetNodeByID should return an error")
	}

	// empty list
	_, err = slice.GetNodeByID("D")
	if err == nil {
		t.Errorf("The Node D is not present in the list, GetNodeByID should return an error")
	}

	// not empty
	node, err := slice.GetNodeByID("B")
	if err != nil {
		t.Errorf("Unexpected error returned by GetNodeByID, current error:%v", err)
	}
	if node != nodeReplica {
		t.Errorf("Expected to find node %v, got %v", nodeReplica, node)
	}
}
