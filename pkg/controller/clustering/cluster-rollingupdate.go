package clustering

import (
	"fmt"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

// SelectMastersToReplace used to replace currentMasters with new redis nodes
func SelectMastersToReplace(oldMasters, newMasters, newNodesNoRole redis.Nodes, nbMaster, nbMasterToReplace int32) (selectedMasters redis.Nodes, newSelectedMasters redis.Nodes, err error) {
	newSelectedMasters = redis.Nodes{}
	if len(newMasters) == int(nbMaster) {
		return newMasters, newSelectedMasters, nil
	}

	selectedMasters = append(selectedMasters, newMasters...)
	nbMasterReplaced := int32(0)
	for _, newNode := range newNodesNoRole {
		if nbMasterReplaced == nbMasterToReplace {
			break
		}
		nbMasterReplaced++
		selectedMasters = append(selectedMasters, newNode)
		newSelectedMasters = append(newSelectedMasters, newNode)
	}
	nbRemainingOldMaster := int(nbMaster) - len(selectedMasters)
	if nbRemainingOldMaster > len(oldMasters) {
		nbRemainingOldMaster = len(oldMasters)
	}
	currentOldMasterSorted := oldMasters.SortNodes()
	selectedMasters = append(selectedMasters, currentOldMasterSorted[:nbRemainingOldMaster]...)
	if nbMasterReplaced != nbMasterToReplace {
		return selectedMasters, newSelectedMasters, fmt.Errorf("insufficient number of nodes for master replacement, wanted:%d, current:%d", nbMasterToReplace, nbMasterReplaced)
	}

	if len(selectedMasters) != int(nbMaster) {
		return selectedMasters, newSelectedMasters, fmt.Errorf("insufficient number of masters, wanted:%d, current:%d", len(selectedMasters), nbMaster)
	}

	return selectedMasters, newSelectedMasters, err
}
