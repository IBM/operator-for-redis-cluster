package clustering

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/golang/glog"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

type migrationInfo struct {
	From *redis.Node
	To   *redis.Node
}

type mapSlotByMigInfo map[migrationInfo]redis.SlotSlice

// SelectPrimaries used to select redis nodes with primary roles
func SelectPrimaries(cluster *redis.Cluster, currentPrimaries, candidatePrimaries redis.Nodes, nbPrimary int32) (redis.Nodes, error) {
	newPrimaryNodes, bestEffort, err := PlacePrimaries(cluster, currentPrimaries, candidatePrimaries, nbPrimary)
	glog.V(2).Infof("total primaries: %d - target %d - selected: %d", len(currentPrimaries)+len(candidatePrimaries), nbPrimary, len(newPrimaryNodes))
	if err != nil {
		return redis.Nodes{}, fmt.Errorf("insufficient primaries, total primaries: %d - target: %d - err: %v", len(currentPrimaries)+len(candidatePrimaries), nbPrimary, err)
	}

	newPrimaryNodes = newPrimaryNodes.SortByFunc(func(a, b *redis.Node) bool { return a.ID < b.ID })

	cluster.Status = rapi.ClusterStatusCalculatingRebalancing
	if bestEffort {
		cluster.NodesPlacement = rapi.NodesPlacementInfoBestEffort
	} else {
		cluster.NodesPlacement = rapi.NodesPlacementInfoOptimal
	}

	return newPrimaryNodes, nil
}

// DispatchSlotsToNewPrimaries used to dispatch slots to the new primary nodes
func DispatchSlotsToNewPrimaries(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, newPrimaryNodes, currentPrimaryNodes, allPrimaryNodes redis.Nodes) error {
	// calculate the migration slot information (which slots go where)
	migrationSlotInfo, info := feedMigInfo(newPrimaryNodes, currentPrimaryNodes, allPrimaryNodes, int(admin.GetHashMaxSlot()+1))
	rCluster.ActionsInfo = info
	rCluster.Status = rapi.ClusterStatusRebalancing
	for nodesInfo, slots := range migrationSlotInfo {
		// there is a need for real error handling here, we must ensure we don't keep a slot in abnormal state
		if nodesInfo.From == nil {
			if glog.V(4) {
				glog.Warning("Adding slots that have probably been lost during scale down, destination: ", nodesInfo.To.ID, " total:", len(slots), " : ", slots)
			}
			err := admin.AddSlots(ctx, nodesInfo.To.IPPort(), slots)
			if err != nil {
				glog.Error("error during ADDSLOTS: ", err)
				return err
			}
		} else {
			if err := setSlotState(ctx, admin, nodesInfo, slots); err != nil {
				return err
			}

			glog.V(6).Info("3) Migrate keys")
			if err := admin.MigrateKeys(ctx, nodesInfo.From, nodesInfo.To, slots, &cluster.Spec.Migration, true); err != nil {
				glog.Error("error during key migration: ", err)
			}

			// We need to call SETSLOT on the node owning the slot first. In the case of a manager crash,
			// the owner may think it is now owning the slot, creating a cluster view discrepancy
			setMigrationSlots(ctx, admin, nodesInfo, slots)

			// update bom
			nodesInfo.From.Slots = redis.RemoveSlots(nodesInfo.From.Slots, slots)

			// now tell all other nodes
			setPrimarySlots(ctx, admin, nodesInfo, slots, allPrimaryNodes)
		}
		// update bom
		nodesInfo.To.Slots = redis.AddSlots(nodesInfo.To.Slots, slots)
	}
	return nil
}

// DispatchEmptySlotsToPrimaries used to dispatch empty slots to the new primary nodes
func DispatchEmptySlotsToPrimaries(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, rCluster *redis.Cluster, newPrimaryNodes, currentPrimaryNodes, allPrimaryNodes redis.Nodes) error {
	// calculate the migration slot information (which slots go where)
	migrationSlotInfo, info := feedMigInfo(newPrimaryNodes, currentPrimaryNodes, allPrimaryNodes, int(admin.GetHashMaxSlot()+1))
	rCluster.ActionsInfo = info
	rCluster.Status = rapi.ClusterStatusRebalancing
	for nodesInfo, slots := range migrationSlotInfo {
		glog.Infof("node info: %v", nodesInfo)
		if nodesInfo.From == nil {
			if glog.V(4) {
				glog.Warning("Adding slots that have probably been lost during scale down, destination: ", nodesInfo.To.ID, " total:", len(slots), " : ", slots)
			}
			err := admin.AddSlots(ctx, nodesInfo.To.IPPort(), slots)
			if err != nil {
				glog.Error("error during ADDSLOTS: ", err)
				return err
			}
		} else {
			if err := setSlotState(ctx, admin, nodesInfo, slots); err != nil {
				return err
			}

			glog.V(6).Info("3) Migrate slots with no keys")
			if err := admin.MigrateEmptySlots(ctx, nodesInfo.From, nodesInfo.To, slots, &cluster.Spec.Migration); err != nil {
				glog.Error("error during key migration: ", err)
			}

			// update bom
			nodesInfo.From.Slots = redis.RemoveSlots(nodesInfo.From.Slots, slots)

			// now tell all other nodes
			setPrimarySlots(ctx, admin, nodesInfo, slots, allPrimaryNodes)
		}
		// update bom
		nodesInfo.To.Slots = redis.AddSlots(nodesInfo.To.Slots, slots)
	}
	return nil
}

func feedMigInfo(newPrimaryNodes, oldPrimaryNodes, allPrimaryNodes redis.Nodes, nbSlots int) (mapOut mapSlotByMigInfo, info redis.ClusterActionsInfo) {
	mapOut = make(mapSlotByMigInfo)
	mapSlotToUpdate := buildSlotsByNode(newPrimaryNodes, oldPrimaryNodes, allPrimaryNodes, nbSlots)

	for id, slots := range mapSlotToUpdate {
		for _, s := range slots {
			found := false
			for _, oldNode := range oldPrimaryNodes {
				if oldNode.ID == id {
					if redis.Contains(oldNode.Slots, s) {
						found = true
						break
					}
					continue
				}
				if redis.Contains(oldNode.Slots, s) {
					newNode, err := newPrimaryNodes.GetNodeByID(id)
					if err != nil {
						glog.Errorf("unable to find node with id:%s", id)
						continue
					}
					mapOut[migrationInfo{From: oldNode, To: newNode}] = append(mapOut[migrationInfo{From: oldNode, To: newNode}], s)
					found = true
					// increment slots counter
					info.NbSlotsToMigrate++
					break
				}
			}
			if !found {
				// new slots added (not from an existing primary). Correspond to lost slots during important scale down
				newNode, err := newPrimaryNodes.GetNodeByID(id)
				if err != nil {
					glog.Errorf("unable to find node with id:%s", id)
					continue
				}
				mapOut[migrationInfo{From: nil, To: newNode}] = append(mapOut[migrationInfo{From: nil, To: newNode}], s)

				// increment slots counter
				info.NbSlotsToMigrate++
			}
		}
	}
	return mapOut, info
}

func setSlotState(ctx context.Context, admin redis.AdminInterface, nodesInfo migrationInfo, slots redis.SlotSlice) error {
	glog.V(6).Info("1) Send SETSLOT IMPORTING command target:", nodesInfo.To.ID, " source-node:", nodesInfo.From.ID, " total:", len(slots), " : ", slots)
	err := admin.SetSlots(ctx, nodesInfo.To.IPPort(), "IMPORTING", slots, nodesInfo.From.ID)
	if err != nil {
		glog.Error("error during SETSLOT IMPORTING: ", err)
		return err
	}
	glog.V(6).Info("2) Send SETSLOT MIGRATING command target:", nodesInfo.From.ID, " destination-node:", nodesInfo.To.ID, " total:", len(slots), " : ", slots)
	err = admin.SetSlots(ctx, nodesInfo.From.IPPort(), "MIGRATING", slots, nodesInfo.To.ID)
	if err != nil {
		glog.Error("error during SETSLOT MIGRATING: ", err)
		return err
	}
	return nil
}

func setMigrationSlots(ctx context.Context, admin redis.AdminInterface, nodesInfo migrationInfo, slots redis.SlotSlice) {
	err := admin.SetSlots(ctx, nodesInfo.To.IPPort(), "NODE", slots, nodesInfo.To.ID)
	if err != nil {
		if glog.V(4) {
			glog.Warningf("warning during SETSLOT NODE on %s: %v", nodesInfo.To.IPPort(), err)
		}
	}
	err = admin.SetSlots(ctx, nodesInfo.From.IPPort(), "NODE", slots, nodesInfo.To.ID)
	if err != nil {
		if glog.V(4) {
			glog.Warningf("warning during SETSLOT NODE on %s: %v", nodesInfo.From.IPPort(), err)
		}
	}
}

func setPrimarySlots(ctx context.Context, admin redis.AdminInterface, nodesInfo migrationInfo, slots redis.SlotSlice, primaries redis.Nodes) {
	for _, primary := range primaries {
		if primary.IPPort() == nodesInfo.To.IPPort() || primary.IPPort() == nodesInfo.From.IPPort() {
			// we already did these two
			continue
		}
		if primary.TotalSlots() == 0 {
			// ignore primaries that no longer have slots
			// some primaries had their slots completely removed in the previous iteration
			continue
		}
		glog.V(6).Info("4) Send SETSLOT NODE command to primary: ", primary.ID, " new owner: ", nodesInfo.To.ID, " total: ", len(slots), " : ", slots)
		err := admin.SetSlots(ctx, primary.IPPort(), "NODE", slots, nodesInfo.To.ID)
		if err != nil {
			if glog.V(4) {
				glog.Warningf("warning during SETSLOT NODE on %s: %v", primary.IPPort(), err)
			}
		}
	}
}

// buildSlotsByNode get all slots that have to be migrated with retrieveSlotToMigrateFrom and retrieveSlotToMigrateFromRemovedNodes
// and assign those slots to node that need them
func buildSlotsByNode(newPrimaryNodes, oldPrimaryNodes, allPrimaryNodes redis.Nodes, nbSlots int) map[string]redis.SlotSlice {
	var nbNode = len(newPrimaryNodes)
	if nbNode == 0 {
		return make(map[string]redis.SlotSlice)
	}
	nbSlotByNode := int(math.Ceil(float64(nbSlots) / float64(nbNode)))
	slotToMigrateByNode := retrieveSlotToMigrateFrom(oldPrimaryNodes, nbSlotByNode)
	slotToMigrateByNodeFromDeleted := retrieveSlotToMigrateFromRemovedNodes(newPrimaryNodes, oldPrimaryNodes)
	for id, slots := range slotToMigrateByNodeFromDeleted {
		slotToMigrateByNode[id] = slots
	}

	slotToMigrateByNode[""] = retrieveLostSlots(oldPrimaryNodes, nbSlots)
	if len(slotToMigrateByNode[""]) != 0 {
		glog.Errorf("several slots have been lost: %v", slotToMigrateByNode[""])
	}
	slotToAddByNode := buildSlotByNodeFromAvailableSlots(newPrimaryNodes, nbSlotByNode, slotToMigrateByNode)

	total := 0
	for _, node := range allPrimaryNodes {
		currentSlots := 0
		removedSlots := 0
		addedSlots := 0
		expectedSlots := 0
		if slots, ok := slotToMigrateByNode[node.ID]; ok {
			removedSlots = len(slots)
		}
		if slots, ok := slotToAddByNode[node.ID]; ok {
			addedSlots = len(slots)
		}
		currentSlots += len(node.Slots)
		total += currentSlots - removedSlots + addedSlots
		searchByAddrFunc := func(n *redis.Node) bool {
			return n.IPPort() == node.IPPort()
		}
		if _, err := newPrimaryNodes.GetNodesByFunc(searchByAddrFunc); err == nil {
			expectedSlots = nbSlotByNode
		}
		glog.Infof("node %s will have %d + %d - %d = %d slots; expected: %d[+/-%d]", node.ID, currentSlots, addedSlots, removedSlots, currentSlots+addedSlots-removedSlots, expectedSlots, len(newPrimaryNodes))
	}
	glog.Infof("total slots: %d - expected: %d", total, nbSlots)

	return slotToAddByNode
}

// retrieveSlotToMigrateFrom list the number of slots that need to be migrated to reach nbSlotByNode per nodes
func retrieveSlotToMigrateFrom(oldPrimaryNodes redis.Nodes, nbSlotByNode int) map[string]redis.SlotSlice {
	slotToMigrateByNode := make(map[string]redis.SlotSlice)
	for _, node := range oldPrimaryNodes {
		glog.V(6).Info("--- oldPrimaryNode:", node.ID)
		if node.TotalSlots() >= nbSlotByNode {
			if len(node.Slots[nbSlotByNode:]) > 0 {
				slotToMigrateByNode[node.ID] = append(slotToMigrateByNode[node.ID], node.Slots[nbSlotByNode:]...)
			}
			glog.V(6).Infof("--- migrating from %s, %d slots", node.ID, len(slotToMigrateByNode[node.ID]))
		}
	}
	return slotToMigrateByNode
}

// retrieveSlotToMigrateFromRemovedNodes given the list of node that will be primaries with slots, and the list of nodes that were primaries with slots
// return the list of slots from previous nodes that will be moved, because this node will no longer hold slots
func retrieveSlotToMigrateFromRemovedNodes(newPrimaryNodes, oldPrimaryNodes redis.Nodes) map[string]redis.SlotSlice {
	slotToMigrateByNode := make(map[string]redis.SlotSlice)
	var removedNodes redis.Nodes
	for _, oldNode := range oldPrimaryNodes {
		glog.V(6).Info("--- oldPrimaryNode:", oldNode.ID)
		isPresent := false
		for _, newNode := range newPrimaryNodes {
			if oldNode.ID == newNode.ID {
				isPresent = true
				break
			}
		}
		if !isPresent {
			removedNodes = append(removedNodes, oldNode)
		}
	}

	for _, node := range removedNodes {
		slotToMigrateByNode[node.ID] = node.Slots
		glog.V(6).Infof("--- removedNode %s: migrating %d slots", node.ID, len(slotToMigrateByNode[node.ID]))
	}
	return slotToMigrateByNode
}

// retrieveLostSlots retrieve the list of slots that are not attributed to a node
func retrieveLostSlots(oldPrimaryNodes redis.Nodes, nbSlots int) redis.SlotSlice {
	var currentFullRange redis.SlotSlice
	for _, node := range oldPrimaryNodes {
		// TODO a lot of perf improvement can be done here with better algorithm to add slot ranges
		currentFullRange = append(currentFullRange, node.Slots...)
	}
	sort.Sort(currentFullRange)
	lostSlots := redis.SlotSlice{}
	// building []slot of slots that are missing from currentFullRange to reach [0, nbSlots]
	last := redis.Slot(0)
	if len(currentFullRange) == 0 || currentFullRange[0] != 0 {
		lostSlots = append(lostSlots, 0)
	}
	for _, slot := range currentFullRange {
		if slot > last+1 {
			for i := last + 1; i < slot; i++ {
				lostSlots = append(lostSlots, i)
			}
		}
		last = slot
	}
	for i := last + 1; i < redis.Slot(nbSlots); i++ {
		lostSlots = append(lostSlots, i)
	}

	return lostSlots
}

func buildSlotByNodeFromAvailableSlots(newPrimaryNodes redis.Nodes, nbSlotByNode int, slotToMigrateByNode map[string]redis.SlotSlice) map[string]redis.SlotSlice {
	slotToAddByNode := make(map[string]redis.SlotSlice)
	var nbNode = len(newPrimaryNodes)
	if nbNode == 0 {
		return slotToAddByNode
	}
	var slotOfNode = make(map[int]redis.SlotSlice)
	for i, node := range newPrimaryNodes {
		slotOfNode[i] = node.Slots
	}
	idNode := 0
	for _, slotsFrom := range slotToMigrateByNode {
		slotIndex := 0
		for slotIndex < len(slotsFrom) {
			missingSlots := nbSlotByNode - len(slotOfNode[idNode])
			if missingSlots > 0 {
				// Node has missing slots, add the slot to current node and increment index
				slotOfNode[idNode] = append(slotOfNode[idNode], slotsFrom[slotIndex])
				slotToAddByNode[newPrimaryNodes[idNode].ID] = append(slotToAddByNode[newPrimaryNodes[idNode].ID], slotsFrom[slotIndex])
				slotIndex++
			} else {
				// Node does not have any missing slots, go to the next node
				idNode++
				if idNode > (nbNode - 1) {
					// All nodes have been filled, go to previous node and overfill it
					idNode--
					slotOfNode[idNode] = append(slotOfNode[idNode], slotsFrom[slotIndex])
					slotToAddByNode[newPrimaryNodes[idNode].ID] = append(slotToAddByNode[newPrimaryNodes[idNode].ID], slotsFrom[slotIndex])
					slotIndex++
				}
			}
		}
	}

	return slotToAddByNode
}
