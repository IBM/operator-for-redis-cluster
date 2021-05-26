package clustering

import (
	"fmt"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

// SelectPrimariesToReplace used to replace currentPrimaries with new redis nodes
func SelectPrimariesToReplace(oldPrimaries, newPrimaries, newNodesNoRole redis.Nodes, nbPrimary, nbPrimaryToReplace int32) (selectedPrimaries redis.Nodes, newSelectedPrimaries redis.Nodes, err error) {
	newSelectedPrimaries = redis.Nodes{}
	if len(newPrimaries) == int(nbPrimary) {
		return newPrimaries, newSelectedPrimaries, nil
	}

	selectedPrimaries = append(selectedPrimaries, newPrimaries...)
	nbPrimaryReplaced := int32(0)
	for _, newNode := range newNodesNoRole {
		if nbPrimaryReplaced == nbPrimaryToReplace {
			break
		}
		nbPrimaryReplaced++
		selectedPrimaries = append(selectedPrimaries, newNode)
		newSelectedPrimaries = append(newSelectedPrimaries, newNode)
	}
	nbRemainingOldPrimary := int(nbPrimary) - len(selectedPrimaries)
	if nbRemainingOldPrimary > len(oldPrimaries) {
		nbRemainingOldPrimary = len(oldPrimaries)
	}
	currentOldPrimarySorted := oldPrimaries.SortNodes()
	selectedPrimaries = append(selectedPrimaries, currentOldPrimarySorted[:nbRemainingOldPrimary]...)
	if nbPrimaryReplaced != nbPrimaryToReplace {
		return selectedPrimaries, newSelectedPrimaries, fmt.Errorf("insufficient number of nodes for primary replacement, wanted:%d, current:%d", nbPrimaryToReplace, nbPrimaryReplaced)
	}

	if len(selectedPrimaries) != int(nbPrimary) {
		return selectedPrimaries, newSelectedPrimaries, fmt.Errorf("insufficient number of primaries, wanted:%d, current:%d", len(selectedPrimaries), nbPrimary)
	}

	return selectedPrimaries, newSelectedPrimaries, err
}
