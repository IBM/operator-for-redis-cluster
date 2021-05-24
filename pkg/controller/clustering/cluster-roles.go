package clustering

import (
	"context"

	v1 "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
	"github.com/golang/glog"
)

// ClassifyNodesByRole use to classify the Nodes by roles
func ClassifyNodesByRole(nodes redis.Nodes) (masters, slaves, mastersWithNoSlots redis.Nodes) {
	masters = redis.Nodes{}
	slaves = redis.Nodes{}
	mastersWithNoSlots = redis.Nodes{}

	for _, node := range nodes {
		if redis.IsMasterWithSlot(node) {
			masters = append(masters, node)
		} else if redis.IsSlave(node) {
			slaves = append(slaves, node)
		} else if redis.IsMasterWithNoSlot(node) {
			mastersWithNoSlots = append(mastersWithNoSlots, node)
		}
	}
	return masters, slaves, mastersWithNoSlots
}

// DispatchSlave aims to dispatch the available redis to slave of the current masters
func DispatchSlave(ctx context.Context, cluster *redis.Cluster, nodes redis.Nodes, replicationLevel int32, admin redis.AdminInterface) error {

	currentMasterNodes, currentSlaveNodes, futureSlaveNodes := ClassifyNodesByRole(nodes)
	glog.Infof("current masters: %v, current slaves: %v, future slaves: %v", currentMasterNodes, currentSlaveNodes, futureSlaveNodes)
	masterToSlaves, err := PlaceSlaves(cluster, currentMasterNodes, currentSlaveNodes, futureSlaveNodes, replicationLevel)
	if err != nil {
		return err
	}
	cluster.NodesPlacement = v1.NodesPlacementInfoOptimal
	if len(masterToSlaves) > 0 {
		return nil
	}
	return AttachSlavesToMaster(ctx, cluster, admin, masterToSlaves)
}

// AttachSlavesToMaster used to attach slaves to there masters
func AttachSlavesToMaster(ctx context.Context, cluster *redis.Cluster, admin redis.AdminInterface, masterToSlaves map[string]redis.Nodes) error {
	var globalErr error
	for masterID, slaves := range masterToSlaves {
		masterNode, err := cluster.GetNodeByID(masterID)
		if err != nil {
			glog.Errorf("[AttachSlavesToMaster] unable fo found the Cluster.Node with redis ID:%s", masterID)
			continue
		}
		for _, slave := range slaves {
			glog.V(2).Infof("[AttachSlavesToMaster] Attaching node %s to master %s", slave.ID, masterID)

			err := admin.AttachSlaveToMaster(ctx, slave, masterNode)
			if err != nil {
				glog.Errorf("Error while attaching node %s to master %s: %v", slave.ID, masterID, err)
				globalErr = err
			}
		}
	}
	return globalErr
}

// DispatchSlavesToNewMasters use to dispatch available Nodes as slave to Master in the case of rolling update
func DispatchSlavesToNewMasters(ctx context.Context, newMasterNodesSlice, oldSlaveNodesSlice, newSlaveNodesSlice redis.Nodes, replicationLevel int32, admin redis.AdminInterface) error {
	glog.V(3).Info("DispatchSlavesToNewMasters start")
	var err error
	slavesByMaster := make(map[string]redis.Nodes)
	masterByID := make(map[string]*redis.Node)

	for _, node := range newMasterNodesSlice {
		slavesByMaster[node.ID] = redis.Nodes{}
		masterByID[node.ID] = node
	}

	for _, slave := range oldSlaveNodesSlice {
		for _, master := range newMasterNodesSlice {
			if slave.MasterReferent == master.ID {
				//The master of this slave is among the new master nodes
				slavesByMaster[slave.MasterReferent] = append(slavesByMaster[slave.MasterReferent], slave)
				break
			}
		}
	}
	for _, slave := range newSlaveNodesSlice {
		selectedMaster := ""
		minSlaveNumber := int32(200) // max slave replication level
		for id, nodes := range slavesByMaster {
			len := int32(len(nodes))
			if len == replicationLevel {
				continue
			}
			if len < minSlaveNumber {
				selectedMaster = id
				minSlaveNumber = len
			}
		}
		if selectedMaster != "" {
			glog.V(2).Infof("Attaching node %s to master %s", slave.ID, selectedMaster)
			if err2 := admin.AttachSlaveToMaster(ctx, slave, masterByID[selectedMaster]); err2 != nil {
				glog.Errorf("Error while attaching node %s to master %s: %v", slave.ID, selectedMaster, err)
				break
			}
			slavesByMaster[selectedMaster] = append(slavesByMaster[selectedMaster], slave)
		} else {
			glog.V(2).Infof("No master found to attach for new slave : %s", slave.ID)
		}
	}
	return err
}

// DispatchSlaveByMaster use to dispatch available Nodes as slave to Master
func DispatchSlaveByMaster(ctx context.Context, futurMasterNodes, currentSlaveNodes, futurSlaveNodes redis.Nodes, replicationLevel int32, admin redis.AdminInterface) error {

	var err error

	glog.Infof("Attaching %d slaves per master, with %d masters, %d slaves, %d unassigned", replicationLevel, len(futurMasterNodes), len(currentSlaveNodes), len(futurSlaveNodes))

	slavesByMaster := make(map[string]redis.Nodes)
	masterByID := make(map[string]*redis.Node)

	for _, node := range futurMasterNodes {
		slavesByMaster[node.ID] = redis.Nodes{}
		masterByID[node.ID] = node
	}

	for _, node := range currentSlaveNodes {
		slavesByMaster[node.MasterReferent] = append(slavesByMaster[node.MasterReferent], node)
	}

	for id, slaves := range slavesByMaster {
		// detach slaves that are linked to a node without slots
		if _, err = futurSlaveNodes.GetNodeByID(id); err == nil {
			glog.Infof("Loosing master role: following slaves previously attached to '%s', will be reassigned: %s", id, slaves)
			futurSlaveNodes = append(futurSlaveNodes, slaves...)
			delete(slavesByMaster, id)
			continue
		}
		// if too many slaves on a master, make them available
		len := int32(len(slaves))
		if len > replicationLevel {
			glog.Infof("Too many slaves: following slaves previously attached to '%s', will be reassigned: %s", id, slaves[:len-replicationLevel])
			futurSlaveNodes = append(futurSlaveNodes, slaves[:len-replicationLevel]...)
			slavesByMaster[id] = slaves[len-replicationLevel:]
		}
	}

	for _, slave := range futurSlaveNodes {
		selectedMaster := ""
		minLevel := int32(200) // max slave replication level
		for id, nodes := range slavesByMaster {
			len := int32(len(nodes))
			if len == replicationLevel {
				continue
			}
			if len < minLevel {
				selectedMaster = id
				minLevel = len
			}
		}
		if selectedMaster != "" {
			glog.V(2).Infof("Attaching node %s to master %s", slave.ID, selectedMaster)
			err = admin.AttachSlaveToMaster(ctx, slave, masterByID[selectedMaster])
			if err != nil {
				glog.Errorf("Error while attaching node %s to master %s: %v", slave.ID, selectedMaster, err)
				break
			}
			slavesByMaster[selectedMaster] = append(slavesByMaster[selectedMaster], slave)
		}
	}
	return err
}
