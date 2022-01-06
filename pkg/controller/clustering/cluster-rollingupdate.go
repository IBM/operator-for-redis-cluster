package clustering

import (
	"fmt"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

// SelectPrimariesToReplace used to replace currentPrimaries with new redis nodes
func SelectPrimariesToReplace(oldPrimaries, newPrimaries, newNodesNoSlots redis.Nodes, nbPrimaries, nbPrimariesToReplace int32) (redis.Nodes, redis.Nodes, error) {
	if len(newPrimaries) == int(nbPrimaries) {
		return newPrimaries, redis.Nodes{}, nil
	}
	selectedPrimaries, newSelectedPrimaries := selectPrimaries(oldPrimaries, newPrimaries, newNodesNoSlots, nbPrimaries, nbPrimariesToReplace)
	if len(newSelectedPrimaries) < int(nbPrimariesToReplace) {
		return selectedPrimaries, newSelectedPrimaries, fmt.Errorf("insufficient number of nodes for primary replacement, wanted:%d, current:%d", nbPrimariesToReplace, len(newSelectedPrimaries))
	}
	if len(selectedPrimaries) != int(nbPrimaries) {
		return selectedPrimaries, newSelectedPrimaries, fmt.Errorf("insufficient number of primaries, wanted:%d, current:%d", len(selectedPrimaries), nbPrimaries)
	}
	return selectedPrimaries, newSelectedPrimaries, nil
}

func selectPrimaries(oldPrimaries, newPrimaries, newNodesNoSlots redis.Nodes, nbPrimaries, nbPrimariesToReplace int32) (redis.Nodes, redis.Nodes) {
	selectedPrimaries := redis.Nodes{}
	newSelectedPrimaries := redis.Nodes{}

	selectedPrimaries = append(selectedPrimaries, newPrimaries...)
	for _, newNode := range newNodesNoSlots {
		if len(newSelectedPrimaries) == int(nbPrimariesToReplace) {
			break
		}
		selectedPrimaries = append(selectedPrimaries, newNode)
		newSelectedPrimaries = append(newSelectedPrimaries, newNode)
	}
	oldPrimaries = oldPrimaries.SortNodes()
	nbRemainingOldPrimary := int(nbPrimaries) - len(selectedPrimaries)
	if nbRemainingOldPrimary > len(oldPrimaries) {
		nbRemainingOldPrimary = len(oldPrimaries)
	}
	if nbRemainingOldPrimary >= 0 {
		selectedPrimaries = append(selectedPrimaries, oldPrimaries[:nbRemainingOldPrimary]...)
	}

	return selectedPrimaries, newSelectedPrimaries
}
