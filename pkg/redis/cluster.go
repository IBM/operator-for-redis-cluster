package redis

import (
	"sort"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// Cluster represents a Redis Cluster
type Cluster struct {
	Name           string
	Namespace      string
	NodeSelector   map[string]string
	Nodes          map[string]*Node
	KubeNodes      []corev1.Node
	Status         rapi.ClusterStatus
	NodesPlacement rapi.NodesPlacementInfo
	ActionsInfo    ClusterActionsInfo
}

// ClusterActionsInfo stores information about the current action on the Cluster
type ClusterActionsInfo struct {
	NbSlotsToMigrate int32
}

// NewCluster builds and returns new Cluster instance
func NewCluster(name, namespace string) *Cluster {
	c := &Cluster{
		Name:      name,
		Namespace: namespace,
		Nodes:     make(map[string]*Node),
	}

	return c
}

// GetZones gets all available zones from the list of k8s nodes
func (c *Cluster) GetZones() []string {
	set := make(map[string]struct{})
	var zones []string
	for _, node := range c.KubeNodes {
		zone, ok := node.Labels[corev1.LabelTopologyZone]
		if ok {
			set[zone] = struct{}{}
		} else {
			set[rapi.UnknownZone] = struct{}{}
		}
	}
	if len(set) == 0 {
		set[rapi.UnknownZone] = struct{}{}
	}
	for key := range set {
		zones = append(zones, key)
	}
	sort.Strings(zones)
	return zones
}

// GetZone gets the zone label from the specified k8s node
func (c *Cluster) GetZone(nodeName string) string {
	for _, node := range c.KubeNodes {
		if node.Name == nodeName {
			label, ok := node.Labels[corev1.LabelTopologyZone]
			if ok {
				return label
			} else {
				return rapi.UnknownZone
			}
		}
	}
	return rapi.UnknownZone
}

// AddNode used to add new Node in the cluster
// if node with the same ID is already present in the cluster
// the previous Node is replaced
func (c *Cluster) AddNode(node *Node) {
	if n, ok := c.Nodes[node.ID]; ok {
		n.Clear()
	}

	c.Nodes[node.ID] = node
}

// GetNodeByID returns a Cluster Node by its ID
// if not present in the cluster return an error
func (c *Cluster) GetNodeByID(id string) (*Node, error) {
	if n, ok := c.Nodes[id]; ok {
		return n, nil
	}
	return nil, nodeNotFoundError
}

// GetNodeByIP returns a Cluster Node by its ID
// if not present in the cluster return an error
func (c *Cluster) GetNodeByIP(ip string) (*Node, error) {
	findFunc := func(node *Node) bool {
		return node.IP == ip
	}

	return c.GetNodeByFunc(findFunc)
}

// GetNodeByPodName returns a Cluster Node by its Pod name
// if not present in the cluster return an error
func (c *Cluster) GetNodeByPodName(name string) (*Node, error) {
	findFunc := func(node *Node) bool {
		if node.Pod == nil {
			return false
		}
		if node.Pod.Name == name {
			return true
		}
		return false
	}

	return c.GetNodeByFunc(findFunc)
}

// GetNodeByFunc returns first node found by the FindNodeFunc
func (c *Cluster) GetNodeByFunc(f FindNodeFunc) (*Node, error) {
	for _, n := range c.Nodes {
		if f(n) {
			return n, nil
		}
	}
	return nil, nodeNotFoundError
}

// GetNodesByFunc returns first node found by the FindNodeFunc
func (c *Cluster) GetNodesByFunc(f FindNodeFunc) (Nodes, error) {
	nodes := Nodes{}
	for _, n := range c.Nodes {
		if f(n) {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return nodes, nodeNotFoundError
	}
	return nodes, nil
}

// FindNodeFunc function for finding a Node
// it is use as input for GetNodeByFunc and GetNodesByFunc
type FindNodeFunc func(node *Node) bool

// ToAPIClusterStatus convert the Cluster information to a api
func (c *Cluster) ToAPIClusterStatus() rapi.RedisClusterState {
	status := rapi.RedisClusterState{}
	status.Status = c.Status
	for _, node := range c.Nodes {
		status.Nodes = append(status.Nodes, node.ToAPINode())
	}
	return status
}
