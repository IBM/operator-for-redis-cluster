package admin

import (
	"context"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

// GetClusterInfoRetType structure to describe the return data of GetClusterInfo method
type GetClusterInfoRetType struct {
	Nodes redis.Nodes
	Err   error
}

// GetKeysInSlotRetType structure to describe the return data of GetKeysInSlot method
type GetKeysInSlotRetType struct {
	Keys []string
	Err  error
}

// CountKeysInSlotRetType structure to describe the return data of CountKeysInSlot method
type CountKeysInSlotRetType struct {
	NbKeys int64
	Err    error
}

// ClusterInfosRetType structure to describe the return data of GetClusterInfosRet method
type ClusterInfosRetType struct {
	ClusterInfos *redis.ClusterInfos
	Err          error
}

// Admin representation of a fake redis admin, where all
// return error for fake admin commands are configurable per addr, and return nil by default
type Admin struct {
	// HashMaxSlots Max slot value
	HashMaxSlots redis.Slot
	// AddrError map from address to error returned from the function
	AddrError map[string]error
	// GetClusterInfosRet returned value for GetClusterInfos function
	GetClusterInfosRet ClusterInfosRetType
	// GetClusterInfosSelectedRet returned value for GetClusterInfos function
	GetClusterInfosSelectedRet ClusterInfosRetType
	// GetKeysInSlotRet map of returned data for GetKeysInSlot function
	GetKeysInSlotRet map[string]GetKeysInSlotRetType
	// CountKeysInSlotRet map of returned data for CountKeysInSlot function
	CountKeysInSlotRet map[string]CountKeysInSlotRetType
	// GetKeysRet map of returned data for GetKeys function
	GetKeysRet map[string]GetKeysInSlotRetType
	cnx        *Connections
}

// NewFakeAdmin returns new AdminInterface for fake admin
func NewFakeAdmin() *Admin {
	return &Admin{
		HashMaxSlots:               16383,
		AddrError:                  make(map[string]error),
		GetClusterInfosRet:         ClusterInfosRetType{},
		GetClusterInfosSelectedRet: ClusterInfosRetType{},
		GetKeysInSlotRet:           make(map[string]GetKeysInSlotRetType),
		CountKeysInSlotRet:         make(map[string]CountKeysInSlotRetType),
		cnx:                        &Connections{},
	}
}

// Close closes all possible resources instantiated by the Admin
func (a *Admin) Close() {
}

// Connections returns a connection map
func (a *Admin) Connections() redis.AdminConnectionsInterface {
	return a.cnx
}

// GetHashMaxSlot gets the max slot value
func (a *Admin) GetHashMaxSlot() redis.Slot {
	return a.HashMaxSlots
}

// AttachNodeToCluster connects a node to the cluster
func (a *Admin) AttachNodeToCluster(ctx context.Context, addr string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// InitRedisCluster init a single node redis cluster
func (a *Admin) InitRedisCluster(ctx context.Context, addr string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// GetClusterInfos returns redis cluster info from all clients
func (a *Admin) GetClusterInfos(ctx context.Context) (*redis.ClusterInfos, error) {
	return a.GetClusterInfosRet.ClusterInfos, a.GetClusterInfosRet.Err
}

// GetClusterInfosSelected returns selected redis cluster info from all clients
func (a *Admin) GetClusterInfosSelected(ctx context.Context, addr []string) (*redis.ClusterInfos, error) {
	return a.GetClusterInfosSelectedRet.ClusterInfos, a.GetClusterInfosSelectedRet.Err
}

// StartFailover forces the failover of a specific redis primary node
func (a *Admin) StartFailover(ctx context.Context, addr string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// ForgetNode forces a redis cluster node to forget a specific node
func (a *Admin) ForgetNode(ctx context.Context, addr string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// ForgetNodeByAddr forces a redis cluster node to forget a specific node by address
func (a *Admin) ForgetNodeByAddr(ctx context.Context, addr string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// SetSlot uses SETSLOT command on a single slot
func (a *Admin) SetSlot(ctx context.Context, addr, action string, slot redis.Slot, node *redis.Node) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// SetSlots uses SETSLOT command on several slots
func (a *Admin) SetSlots(ctx context.Context, addr, action string, slots redis.SlotSlice, nodeID string, retryAttempt int) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// AddSlots uses ADDSLOT command on several slots
func (a *Admin) AddSlots(ctx context.Context, addr string, slots redis.SlotSlice) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// DelSlots deletes slots in a pipeline
func (a *Admin) DelSlots(ctx context.Context, addr string, slots redis.SlotSlice) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// GetKeysInSlot gets the keys in the given slot
func (a *Admin) GetKeysInSlot(ctx context.Context, addr string, node redis.Slot, batch string, limit bool) ([]string, error) {
	val, ok := a.GetKeysInSlotRet[addr]
	if !ok {
		val = GetKeysInSlotRetType{Keys: []string{}, Err: nil}
	}
	return val.Keys, val.Err
}

// CountKeysInSlot counts the keys in the given slot
func (a *Admin) CountKeysInSlot(ctx context.Context, addr string, node redis.Slot) (int64, error) {
	val, ok := a.CountKeysInSlotRet[addr]
	if !ok {
		val = CountKeysInSlotRetType{NbKeys: int64(0), Err: nil}
	}
	return val.NbKeys, val.Err
}

// GetKeys uses GETKEYSINSLOT command to get keys in a slot
// Number of keys returned specified by batch
func (a *Admin) GetKeys(ctx context.Context, addr string, slot redis.Slot, batch string) ([]string, error) {
	val, ok := a.GetKeysInSlotRet[addr]
	if !ok {
		val = GetKeysInSlotRetType{Keys: []string{}, Err: nil}
	}
	return val.Keys, val.Err
}

// DeleteKeys uses DEL command to delete multiple keys
func (a *Admin) DeleteKeys(ctx context.Context, addr string, keys []string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// MigrateKeys migrates keys from slots to other slots
func (a *Admin) MigrateKeys(ctx context.Context, source *redis.Node, dest *redis.Node, slots redis.SlotSlice, conf *rapi.RedisClusterSpec, replace, scaling bool, primaries redis.Nodes) error {
	val, ok := a.AddrError[source.IPPort()]
	if !ok {
		val = nil
	}
	return val
}

// AttachReplicaToPrimary attaches a replica to a primary node
func (a *Admin) AttachReplicaToPrimary(ctx context.Context, replica *redis.Node, primary *redis.Node) error {
	val, ok := a.AddrError[primary.ID]
	if !ok {
		val = nil
	}
	return val
}

// DetachReplica detaches a replica from a primary
func (a *Admin) DetachReplica(ctx context.Context, replica *redis.Node) error {
	val, ok := a.AddrError[replica.IPPort()]
	if !ok {
		val = nil
	}
	return val
}

// FlushAndReset resets the cluster configuration of the node
func (a *Admin) FlushAndReset(ctx context.Context, addr string, mode string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// FlushAll flushes all keys in cluster
func (a *Admin) FlushAll(ctx context.Context, addr string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}

// RebuildConnectionMap rebuilds the connection map according to the given addresses
func (a *Admin) RebuildConnectionMap(ctx context.Context, addrs []string, options *redis.AdminOptions) {
}

// GetConfig gets the running redis server configuration matching the pattern
func (a *Admin) GetConfig(ctx context.Context, pattern string) (map[string]string, error) {
	return map[string]string{}, nil
}

// SetConfig sets the specified redis server configuration
func (a *Admin) SetConfig(ctx context.Context, addr string, config []string) error {
	val, ok := a.AddrError[addr]
	if !ok {
		val = nil
	}
	return val
}
