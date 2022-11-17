package redis

import (
	"context"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/operator-for-redis-cluster/pkg/config"
	corev1 "k8s.io/api/core/v1"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"

	"github.com/mediocregopher/radix/v4"

	"github.com/golang/glog"
)

// AdminInterface redis cluster admin interface
type AdminInterface interface {
	// Connections returns the connection map of all clients
	Connections() AdminConnectionsInterface
	// Close the admin connections
	Close()
	// InitRedisCluster configures the first node of a cluster
	InitRedisCluster(ctx context.Context, addr string) error
	// GetClusterInfos gets node info for all nodes
	GetClusterInfos(ctx context.Context) (*ClusterInfos, error)
	// GetClusterInfosSelected returns the node info for all selected nodes in the cluster
	GetClusterInfosSelected(ctx context.Context, addrs []string) (*ClusterInfos, error)
	// AttachNodeToCluster connects a Node to the cluster
	AttachNodeToCluster(ctx context.Context, addr string) error
	// AttachReplicaToPrimary attaches a replica to a primary node
	AttachReplicaToPrimary(ctx context.Context, replica *Node, primary *Node) error
	// DetachReplica detaches a replica from a primary
	DetachReplica(ctx context.Context, replica *Node) error
	// StartFailover executes the failover of a redis primary with the corresponding addr
	StartFailover(ctx context.Context, addr string) error
	// ForgetNode forces the cluster to forget a node
	ForgetNode(ctx context.Context, id string) error
	// ForgetNodeByAddr forces the cluster to forget the node with the specified address
	ForgetNodeByAddr(ctx context.Context, addr string) error
	// SetSlot sets a single slot
	SetSlot(ctx context.Context, addr, action string, slot Slot, node *Node) error
	// SetSlots sets multiple slots in a pipeline
	SetSlots(ctx context.Context, addr string, action string, slots SlotSlice, nodeID string, retryAttempt int) error
	// AddSlots adds slots in a pipeline
	AddSlots(ctx context.Context, addr string, slots SlotSlice) error
	// DelSlots deletes slots in a pipeline
	DelSlots(ctx context.Context, addr string, slots SlotSlice) error
	// GetKeysInSlot gets the keys in the given slot on the node
	GetKeysInSlot(ctx context.Context, addr string, slot Slot, batch string, limit bool) ([]string, error)
	// CountKeysInSlot counts the keys in a given slot on the node
	CountKeysInSlot(ctx context.Context, addr string, slot Slot) (int64, error)
	// GetKeys gets keys in a slot
	GetKeys(ctx context.Context, addr string, slot Slot, batch string) ([]string, error)
	// DeleteKeys deletes keys
	DeleteKeys(ctx context.Context, addr string, keys []string) error
	// MigrateKeys migrates keys from the source to destination node and returns number of slot migrated
	MigrateKeys(ctx context.Context, source *Node, dest *Node, slots SlotSlice, spec *rapi.RedisClusterSpec, replace, scaling bool, primaries Nodes) error
	// FlushAndReset flushes and resets the cluster configuration of the node
	FlushAndReset(ctx context.Context, addr string, mode string) error
	// FlushAll flushes all keys in the cluster
	FlushAll(ctx context.Context, addr string) error
	// GetHashMaxSlot gets the max slot value
	GetHashMaxSlot() Slot
	// RebuildConnectionMap rebuilds the connection map according to the given addresses
	RebuildConnectionMap(ctx context.Context, addrs []string, options *AdminOptions)
	// GetConfig gets the running redis server configuration matching the pattern
	GetConfig(ctx context.Context, pattern string) (map[string]string, error)
	// SetConfig sets the specified redis server configuration
	SetConfig(ctx context.Context, addr string, config []string) error
}

// AdminOptions optional options for redis admin
type AdminOptions struct {
	ConnectionTimeout  time.Duration
	ClientName         string
	RenameCommandsFile string
}

// Admin wraps redis cluster admin logic
type Admin struct {
	hashMaxSlots Slot
	cnx          AdminConnectionsInterface
}

// NewRedisAdmin builds and returns new Admin from the list of pods
func NewRedisAdmin(ctx context.Context, pods []corev1.Pod, cfg *config.Redis) (AdminInterface, error) {
	nodesAddrs := []string{}
	for _, pod := range pods {
		redisPort := DefaultRedisPort
		for _, container := range pod.Spec.Containers {
			if container.Name == "redis-node" {
				for _, port := range container.Ports {
					if port.Name == "redis" {
						redisPort = fmt.Sprintf("%d", port.ContainerPort)
						break
					}
				}
			}
		}
		nodesAddrs = append(nodesAddrs, net.JoinHostPort(pod.Status.PodIP, redisPort))
	}
	adminConfig := AdminOptions{
		ConnectionTimeout:  time.Duration(cfg.DialTimeout) * time.Millisecond,
		RenameCommandsFile: cfg.GetRenameCommandsFile(),
	}

	return NewAdmin(ctx, nodesAddrs, &adminConfig), nil
}

// NewAdmin returns new AdminInterface instance
// at the same time it connects to all Redis Nodes thanks to the address list
func NewAdmin(ctx context.Context, addrs []string, options *AdminOptions) AdminInterface {
	a := &Admin{
		hashMaxSlots: HashMaxSlots,
	}

	// perform initial connections
	a.cnx = NewAdminConnections(ctx, addrs, options)

	return a
}

// Connections returns the connection map of all clients
func (a *Admin) Connections() AdminConnectionsInterface {
	return a.cnx
}

// Close used to close all possible resources instantiated by the Admin
func (a *Admin) Close() {
	a.Connections().Reset()
}

// GetHashMaxSlot get the max slot value
func (a *Admin) GetHashMaxSlot() Slot {
	return a.hashMaxSlots
}

// AttachNodeToCluster command used to connect a Node to the cluster
func (a *Admin) AttachNodeToCluster(ctx context.Context, addr string) error {
	ip, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}

	all := a.Connections().GetAll()
	if len(all) == 0 {
		return fmt.Errorf("no connection for other redis-node found")
	}
	for cAddr, c := range a.Connections().GetAll() {
		if cAddr == addr {
			continue
		}
		var resp string
		cmdErr := c.DoCmd(ctx, &resp, "CLUSTER", "MEET", ip, port)
		if err = a.Connections().ValidateResp(ctx, &resp, cmdErr, addr, "unable to attach node to cluster"); err != nil {
			return err
		}
	}

	err = a.Connections().Add(ctx, addr)
	if err != nil {
		return err
	}

	glog.Infof("node %s attached properly", addr)
	return nil
}

// InitRedisCluster used to init a single node redis cluster
func (a *Admin) InitRedisCluster(ctx context.Context, addr string) error {
	return a.AddSlots(ctx, addr, BuildSlotSlice(0, a.GetHashMaxSlot()))
}

// GetClusterInfos return the Nodes infos for all nodes
func (a *Admin) GetClusterInfos(ctx context.Context) (*ClusterInfos, error) {
	infos := NewClusterInfos()
	clusterErr := NewClusterInfosError()

	for addr, c := range a.Connections().GetAll() {
		nodeinfos, err := a.getInfos(ctx, c, addr)
		if err != nil {
			infos.Status = ClusterInfoPartial
			clusterErr.partial = true
			clusterErr.errs[addr] = err
			continue
		}
		if nodeinfos.Node != nil && nodeinfos.Node.IPPort() == addr {
			infos.Infos[addr] = nodeinfos
		} else {
			glog.Warningf("bad node info retrieved from %s", addr)
		}
	}

	if len(clusterErr.errs) == 0 {
		clusterErr.inconsistent = !infos.ComputeStatus()
	}
	if infos.Status == ClusterInfoConsistent {
		return infos, nil
	}
	return infos, clusterErr
}

// GetClusterInfosSelected return the Nodes infos for all nodes selected in the cluster
func (a *Admin) GetClusterInfosSelected(ctx context.Context, addrs []string) (*ClusterInfos, error) {
	infos := NewClusterInfos()
	clusterErr := NewClusterInfosError()

	for addr, c := range a.Connections().GetSelected(addrs) {
		nodeinfos, err := a.getInfos(ctx, c, addr)
		if err != nil {
			infos.Status = ClusterInfoPartial
			clusterErr.partial = true
			clusterErr.errs[addr] = err
			continue
		}
		if nodeinfos.Node != nil && nodeinfos.Node.IPPort() == addr {
			infos.Infos[addr] = nodeinfos
		} else {
			glog.Warningf("bad node info retrieved from %s", addr)
		}
	}

	if len(clusterErr.errs) == 0 {
		clusterErr.inconsistent = !infos.ComputeStatus()
	}
	if infos.Status == ClusterInfoConsistent {
		return infos, nil
	}
	return infos, clusterErr
}

// StartFailover used to force the failover of a specific redis primary node
func (a *Admin) StartFailover(ctx context.Context, addr string) error {
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	var me *NodeInfos
	me, err = a.getInfos(ctx, c, addr)
	if err != nil {
		return err
	}

	if me.Node.Role != redisPrimaryRole {
		// if not a Primary dont failover
		return nil
	}

	replicas, err := selectMyReplicas(me.Node, me.Friends)
	if err != nil {
		return fmt.Errorf("unable to found associated replicas, err:%s", err)
	}

	if len(replicas) == 0 {
		return fmt.Errorf("primary id:%s dont have associated replica", me.Node.ID)
	}

	if glog.V(3) {
		for _, replica := range replicas {
			glog.Info("- replica: ", replica.ID)
		}
	}

	failoverTriggered := false
	for _, aReplica := range replicas {
		var replicaClient ClientInterface
		if replicaClient, err = a.Connections().Get(ctx, aReplica.IPPort()); err != nil {
			glog.Errorf("unable to get connection for ip %s: %v", aReplica.IPPort(), err)
			continue
		}
		var resp string
		cmdErr := replicaClient.DoCmd(ctx, &resp, "CLUSTER", "FAILOVER")
		if err = a.Connections().ValidateResp(ctx, &resp, cmdErr, aReplica.IPPort(), "unable to execute CLUSTER FAILOVER"); err != nil {
			continue
		}
		failoverTriggered = true
		break
	}

	if !failoverTriggered {
		return fmt.Errorf("unable to trigger failover for node '%s'", me.Node.IPPort())
	}

	for {
		me, err = a.getInfos(ctx, c, addr)
		if err != nil {
			return err
		}

		if me.Node.TotalSlots() == 0 {
			glog.Info("failover completed")
			break
		}

		glog.Info("waiting failover to be complete...")
		time.Sleep(time.Second) // TODO: implement back-off like logic
		// we should wait until all slots have been moved to the new primary
		// this is the only way to know that we can stop this primary with no impact on the cluster
	}

	return nil
}

// ForgetNode used to force other redis cluster node to forget a specific node
func (a *Admin) ForgetNode(ctx context.Context, id string) error {
	infos, _ := a.GetClusterInfos(ctx)
	for nodeAddr, nodeinfos := range infos.Infos {
		if nodeinfos.Node.ID == id {
			a.Connections().Remove(nodeAddr)
			continue
		}
		c, err := a.Connections().Get(ctx, nodeAddr)
		if err != nil {
			glog.Errorf("node %s cannot forget node %s: %v", nodeAddr, id, err)
			continue
		}

		if IsReplica(nodeinfos.Node) && nodeinfos.Node.PrimaryReferent == id {
			if err = a.DetachReplica(ctx, nodeinfos.Node); err != nil {
				glog.Errorf("unable to detach replica %q of primary %q", nodeinfos.Node.ID, id)
				continue
			}
			glog.V(2).Infof("detach replica id: %s of primary: %s", nodeinfos.Node.ID, id)
		}
		var resp string
		err = c.DoCmd(ctx, &resp, "CLUSTER", "FORGET", id)
		_ = a.Connections().ValidateResp(ctx, &resp, err, nodeAddr, "unable to execute CLUSTER FORGET")
		glog.V(6).Infof("node %s forgot node %s:%s", nodeinfos.Node.ID, id, resp)
	}

	glog.Infof("forget node %s complete", id)
	return nil
}

// ForgetNodeByAddr used to force other redis cluster node to forget a specific node
func (a *Admin) ForgetNodeByAddr(ctx context.Context, addr string) error {
	infos, _ := a.GetClusterInfos(ctx)
	var me *Node
	myInfo, ok := infos.Infos[addr]
	if !ok {
		// get its id from a random node that still knows it
		for _, nodeInfos := range infos.Infos {
			for _, node := range nodeInfos.Friends {
				if node.IPPort() == addr {
					me = node
					break
				}
			}
			if me != nil {
				break
			}
		}
	} else {
		me = myInfo.Node
	}

	if me == nil {
		return fmt.Errorf("cannot forget node %s, not found in infos", addr)
	}

	return a.ForgetNode(ctx, me.ID)
}

func (a *Admin) SetSlot(ctx context.Context, addr, action string, slot Slot, node *Node) error {
	var resp string
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	cmdErr := c.DoCmd(ctx, &resp, "CLUSTER", "SETSLOT", slot.String(), action, node.ID)
	if err = a.Connections().ValidateResp(ctx, &resp, cmdErr, node.IPPort(), fmt.Sprintf("unable to execute CLUSTER SETSLOT %s %s %s ", slot, action, addr)); err != nil {
		return err
	}
	return nil
}

// SetSlots used to set SETSLOT command on several slots
func (a *Admin) SetSlots(ctx context.Context, addr, action string, slots SlotSlice, nodeID string, retryAttempt int) error {
	if len(slots) == 0 {
		return nil
	}
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	for _, slot := range slots {
		if nodeID == "" {
			c.PipeAppend(radix.Cmd(nil, "CLUSTER", "SETSLOT", slot.String(), action))
		} else {
			c.PipeAppend(radix.Cmd(nil, "CLUSTER", "SETSLOT", slot.String(), action, nodeID))
		}
	}
	if err = c.DoPipe(ctx); err != nil {
		c.PipeReset()
		if retryAttempt < defaultRetryAttempts {
			glog.Warningf("error %v occurred on node %s during CLUSTER SETSLOT %s; attempt: %d", err, addr, action, retryAttempt)
			return a.SetSlots(ctx, addr, action, slots, nodeID, retryAttempt+1)
		}
		return err
	}
	c.PipeReset()
	return nil
}

// AddSlots uses the ADDSLOTS command to add several slots
func (a *Admin) AddSlots(ctx context.Context, addr string, slots SlotSlice) error {
	if len(slots) == 0 {
		return nil
	}
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	var resp string
	args := []string{"ADDSLOTS"}
	args = append(args, slots.ConvertToStrings()...)
	err = c.DoCmd(ctx, &resp, "CLUSTER", args...)
	return a.Connections().ValidateResp(ctx, &resp, err, addr, "unable to execute ADDSLOTS")
}

// DelSlots uses the DELSLOTS command to delete several slots
func (a *Admin) DelSlots(ctx context.Context, addr string, slots SlotSlice) error {
	if len(slots) == 0 {
		return nil
	}
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	var resp string
	args := []string{"DELSLOTS"}
	args = append(args, slots.ConvertToStrings()...)
	err = c.DoCmd(ctx, &resp, "CLUSTER", args...)
	return a.Connections().ValidateResp(ctx, &resp, err, addr, "unable to execute DELSLOTS")
}

// GetKeysInSlot exec the redis command to get the keys in the given slot on the node we are connected to
// Batch is the number of keys fetch per batch, Limit can be use to limit to one batch
func (a *Admin) GetKeysInSlot(ctx context.Context, addr string, slot Slot, batch string, limit bool) ([]string, error) {
	keyCount := 0
	var allKeys []string
	for {
		keys, err := a.GetKeys(ctx, addr, slot, batch)
		if err != nil {
			return allKeys, err
		}
		allKeys = append(allKeys, keys...)

		keyCount += len(keys)
		if limit || len(keys) == 0 {
			break
		}
	}
	return allKeys, nil
}

// CountKeysInSlot exec the redis command to count the number of keys in the given slot on a node
func (a *Admin) CountKeysInSlot(ctx context.Context, addr string, slot Slot) (int64, error) {
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return 0, err
	}

	var resp int64
	cmdErr := c.DoCmd(ctx, &resp, "CLUSTER", "COUNTKEYSINSLOT", slot.String())
	if err := a.Connections().ValidateResp(ctx, &resp, cmdErr, addr, "unable to execute COUNTKEYSINSLOT"); err != nil {
		return 0, err
	}
	return resp, nil
}

// GetKeys uses the GETKEYSINSLOT command to get the number of keys specified by keyBatch
func (a *Admin) GetKeys(ctx context.Context, addr string, slot Slot, batch string) ([]string, error) {
	var keys []string
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return keys, err
	}
	cmdErr := c.DoCmd(ctx, &keys, "CLUSTER", "GETKEYSINSLOT", slot.String(), batch)
	if err = a.Connections().ValidateResp(ctx, &keys, cmdErr, addr, "unable to execute GETKEYSINSLOT"); err != nil {
		return keys, err
	}
	return keys, nil
}

// DeleteKeys uses the DEL command to get delete multiple keys
func (a *Admin) DeleteKeys(ctx context.Context, addr string, keys []string) error {
	var resp string
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	cmdErr := c.DoCmd(ctx, &resp, "DEL", keys...)
	if err = a.Connections().ValidateResp(ctx, &resp, cmdErr, addr, "unable to execute DEL keys"); err != nil {
		return err
	}
	return nil
}

func (a *Admin) migrateSlot(ctx context.Context, source *Node, dest *Node, slot Slot, batch, timeout string, replace bool) error {
	c, err := a.Connections().Get(ctx, source.IPPort())
	if err != nil {
		return err
	}
	for {
		keys, err := a.GetKeys(ctx, source.IPPort(), slot, batch)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			break
		}
		var args []string
		if replace {
			args = append([]string{dest.IP, dest.Port, "", "0", timeout, "REPLACE", "KEYS"}, keys...)
		} else {
			args = append([]string{dest.IP, dest.Port, "", "0", timeout, "KEYS"}, keys...)
		}
		var resp string
		cmdErr := c.DoCmdWithRetries(ctx, &resp, "MIGRATE", args...)
		if err = a.Connections().ValidateResp(ctx, &resp, cmdErr, source.IPPort(), "unable to run command MIGRATE"); err != nil {
			return err
		}
	}

	return nil
}

// MigrateKeys from the source node to the destination node. If replace is true, replace key on busy error.
// Timeout is in milliseconds
func (a *Admin) MigrateKeys(ctx context.Context, source *Node, dest *Node, slots SlotSlice, spec *rapi.RedisClusterSpec, replace, scaling bool, primaries Nodes) error {
	glog.V(2).Infof("batch migration started for %d slots from %s to %s - scaling: %v, primaries: %v, spec: %v", len(slots), source.IPPort(), dest.IPPort(), scaling, primaries, spec)
	if len(slots) == 0 {
		return nil
	}

	var timeoutStr, keyBatchSize string
	var slotBatchSize int
	if scaling {
		timeoutStr = strconv.Itoa(int(*spec.Scaling.IdleTimeoutMillis))
		keyBatchSize = strconv.Itoa(int(*spec.Scaling.KeyBatchSize))
		slotBatchSize = int(*spec.Scaling.SlotBatchSize)
	} else {
		timeoutStr = strconv.Itoa(int(*spec.RollingUpdate.IdleTimeoutMillis))
		keyBatchSize = strconv.Itoa(int(*spec.RollingUpdate.KeyBatchSize))
		slotBatchSize = int(*spec.RollingUpdate.SlotBatchSize)
	}
	slotsLenFloat := float64(len(slots))
	start := time.Now()

	if err := a.setSlotState(ctx, source, dest, slots); err != nil {
		return err
	}

	glog.V(6).Info("3) Migrate keys")
	for i := 0; i < len(slots); i = i + slotBatchSize {
		wg := sync.WaitGroup{}
		batchStart := time.Now()
		endIndex := int(math.Min(float64(i+slotBatchSize), slotsLenFloat))
		slotStart := time.Now()
		slotSlice := slots[i:endIndex]
		for _, slot := range slotSlice {
			wg.Add(1)
			slot := slot
			go func() {
				defer wg.Done()
				if scaling || *spec.RollingUpdate.KeyMigration {
					if err := a.migrateSlot(ctx, source, dest, slot, keyBatchSize, timeoutStr, replace); err != nil {
						glog.Error(err)
					}
				}
			}()
		}
		wg.Wait()
		if !scaling {
			delay := time.Duration(spec.RollingUpdate.WarmingDelayMillis)*time.Millisecond - time.Since(slotStart)
			if delay > 0 {
				time.Sleep(delay)
			}
		}
		glog.V(6).Infof("batch migration of slots %d-%d from %s to %s completed in %s", slots[i], slots[endIndex-1], source.IPPort(), dest.IPPort(), time.Since(batchStart))
	}
	a.setMigrationSlots(ctx, source, dest, slots)
	source.Slots = RemoveSlots(source.Slots, slots)
	a.setPrimarySlots(ctx, source, dest, slots, primaries)
	glog.V(2).Infof("batch migration of %d slots from %s to %s completed in %s", len(slots), source.IPPort(), dest.IPPort(), time.Since(start))
	return nil
}

func (a *Admin) setSlotState(ctx context.Context, src *Node, dest *Node, slots SlotSlice) error {
	glog.V(6).Info("1) Send SETSLOT IMPORTING command target:", dest.IPPort(), " source-node:", src.IPPort(), " total:", len(slots), " : ", slots)
	err := a.SetSlots(ctx, dest.IPPort(), "IMPORTING", slots, src.ID, 0)
	if err != nil {
		glog.Warningf("error during SETSLOT IMPORTING: %v", err)
		return err
	}
	glog.V(6).Info("2) Send SETSLOT MIGRATION command target:", src.IPPort(), " destination-node:", dest.IPPort(), " total:", len(slots), " : ", slots)
	err = a.SetSlots(ctx, src.IPPort(), "MIGRATING", slots, dest.ID, 0)
	if err != nil {
		glog.Warningf("error during SETSLOT MIGRATING: %v", err)
		return err
	}
	return nil
}

func (a *Admin) setMigrationSlots(ctx context.Context, src *Node, dest *Node, slots SlotSlice) {
	if err := a.SetSlots(ctx, dest.IPPort(), "NODE", slots, dest.ID, 0); err != nil {
		if glog.V(4) {
			glog.Warningf("warning during SETSLOT NODE on %s: %v", dest.IPPort(), err)
		}
	}
	if err := a.SetSlots(ctx, src.IPPort(), "NODE", slots, dest.ID, 0); err != nil {
		if glog.V(4) {
			glog.Warningf("warning during SETSLOT NODE on %s: %v", src.IPPort(), err)
		}
	}
}

func (a *Admin) setPrimarySlots(ctx context.Context, src *Node, dest *Node, slots SlotSlice, primaries Nodes) {
	for _, primary := range primaries {
		if primary.IPPort() == dest.IPPort() || primary.IPPort() == src.IPPort() {
			// we already did these two
			continue
		}
		if primary.TotalSlots() == 0 {
			// ignore primaries that no longer have slots
			// some primaries had their slots completely removed in the previous iteration
			continue
		}
		glog.V(6).Info("4) Send SETSLOT NODE command to primary: ", primary.IPPort(), " new owner: ", dest.IPPort(), " total: ", len(slots), " : ", slots)
		err := a.SetSlots(ctx, primary.IPPort(), "NODE", slots, dest.ID, 0)
		if err != nil {
			if glog.V(4) {
				glog.Warningf("warning during SETSLOT NODE on %s: %v", primary.IPPort(), err)
			}
		}
	}
}

// AttachReplicaToPrimary attach a replica to a primary node
func (a *Admin) AttachReplicaToPrimary(ctx context.Context, replica *Node, primary *Node) error {
	c, err := a.Connections().Get(ctx, replica.IPPort())
	if err != nil {
		return err
	}
	var resp string
	cmdErr := c.DoCmd(ctx, &resp, "CLUSTER", "REPLICATE", primary.ID)
	if err = a.Connections().ValidateResp(ctx, &resp, cmdErr, replica.IPPort(), "unable to execute REPLICATE"); err != nil {
		return err
	}

	replica.SetPrimaryReferent(primary.ID)
	return replica.SetRole(redisReplicaRole)
}

// DetachReplica use to detach a replica from a primary
func (a *Admin) DetachReplica(ctx context.Context, replica *Node) error {
	c, err := a.Connections().Get(ctx, replica.IPPort())
	if err != nil {
		glog.Errorf("unable to get the connection for replica ID:%s, addr:%s , err:%v", replica.ID, replica.IPPort(), err)
		return err
	}
	var resp string
	cmdErr := c.DoCmd(ctx, &resp, "CLUSTER", "RESET", ResetSoft)
	if err = a.Connections().ValidateResp(ctx, &resp, cmdErr, replica.IPPort(), "cannot attach node to cluster"); err != nil {
		return err
	}

	if err = a.AttachNodeToCluster(ctx, replica.IPPort()); err != nil {
		glog.Errorf("[DetachReplica] unable to attach replica with id: %s addr:%s", replica.ID, replica.IPPort())
		return err
	}

	replica.SetPrimaryReferent("")
	return replica.SetRole(redisPrimaryRole)
}

// FlushAndReset flush the cluster and reset the cluster configuration of the node. Commands are piped, to ensure no items arrived between flush and reset
func (a *Admin) FlushAndReset(ctx context.Context, addr string, mode string) error {
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	c.PipeAppend(radix.Cmd(nil, "FLUSHALL"))
	c.PipeAppend(radix.Cmd(nil, "CLUSTER", "RESET", mode))
	if err = c.DoPipe(ctx); err != nil {
		return fmt.Errorf("error %v occurred on node %s during CLUSTER RESET", err, addr)
	}
	return nil
}

// FlushAll flush all keys in cluster
func (a *Admin) FlushAll(ctx context.Context, addr string) error {
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	if err = c.DoCmd(ctx, nil, "FLUSHALL"); err != nil {
		return fmt.Errorf("error %v occured on node %s during FLUSHALL", err, addr)
	}
	return nil
}

func selectMyReplicas(me *Node, nodes Nodes) (Nodes, error) {
	return nodes.GetNodesByFunc(func(n *Node) bool {
		return n.PrimaryReferent == me.ID
	})
}

func (a *Admin) getInfos(ctx context.Context, c ClientInterface, addr string) (*NodeInfos, error) {
	var resp string
	cmdErr := c.DoCmd(ctx, &resp, "CLUSTER", "NODES")
	if err := a.Connections().ValidateResp(ctx, &resp, cmdErr, addr, "unable to retrieve node info"); err != nil {
		return nil, err
	}
	nodeInfos := DecodeNodeInfos(&resp, addr)

	if glog.V(3) {
		// Retrieve server info for debugging
		cmdErr = c.DoCmd(ctx, &resp, "INFO", "SERVER")
		if err := a.Connections().ValidateResp(ctx, &resp, cmdErr, addr, "unable to retrieve node info"); err != nil {
			return nil, err
		}

		var serverStartTime time.Time
		serverStartTime, err := DecodeNodeStartTime(&resp)

		if err != nil {
			return nil, err
		}

		nodeInfos.Node.ServerStartTime = serverStartTime
	}

	return nodeInfos, nil
}

// RebuildConnectionMap rebuild the connection map according to the given addresses
func (a *Admin) RebuildConnectionMap(ctx context.Context, addrs []string, options *AdminOptions) {
	a.cnx.Reset()
	a.cnx = NewAdminConnections(ctx, addrs, options)
}

// GetConfig gets the running redis server configuration matching the pattern
func (a *Admin) GetConfig(ctx context.Context, pattern string) (map[string]string, error) {
	cfg := make(map[string]string)
	c, err := a.Connections().GetRandom()
	if err != nil {
		return cfg, err
	}
	var resp []string
	if err = c.DoCmd(ctx, &resp, "CONFIG", "GET", pattern); err != nil {
		return nil, fmt.Errorf("unable to execute CONFIG GET: %v", err)
	}
	for i := 0; i < len(resp)-1; i += 2 {
		cfg[resp[i]] = resp[i+1]
	}
	return cfg, nil
}

// SetConfig sets the specified redis server configuration
func (a *Admin) SetConfig(ctx context.Context, addr string, config []string) error {
	var resp string
	c, err := a.Connections().Get(ctx, addr)
	if err != nil {
		return err
	}
	args := append([]string{"SET"}, config...)
	cmdErr := c.DoCmd(ctx, &resp, "CONFIG", args...)
	if err = a.Connections().ValidateResp(ctx, &resp, cmdErr, addr, "unable to execute CONFIG SET"); err != nil {
		return err
	}
	return nil
}
