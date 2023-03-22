package sanitycheck

import (
	"context"
	"sort"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	"github.com/golang/glog"
)

// Check if too many masters are scheduled in the same zone
func needMasterRebalancing(cluster *rapi.RedisCluster) bool {
	needRebalancing := false
	zoneToPrimariesCount := make(map[string]int)
	zoneToNodesCount := make(map[string]int)
	primariesCount := 0
	for _, node := range cluster.Status.Cluster.Nodes {
		if node.Role == rapi.RedisClusterNodeRolePrimary {
			zoneToPrimariesCount[node.Zone]++
			primariesCount++
		}
		zoneToNodesCount[node.Zone]++
	}

	// Check if there are zero zones
	if len(zoneToNodesCount) <= 0 {
		return false
	}

	// Calculate max number of primaries allowed in a zone
	maxPrimariesPerZone := float64(primariesCount)/float64(len(zoneToNodesCount)) + 1
	glog.V(6).Info("[SanityChecks][FixZoneBalance] Max primaries allowed per zone: ", maxPrimariesPerZone, ", Number of zones: ", len(zoneToNodesCount))

	// Check if any zones have more primaries scheduled than maxPrimariesPerZone
	for _, count := range zoneToPrimariesCount {
		if float64(count) > maxPrimariesPerZone {
			needRebalancing = true
		}
	}

	return needRebalancing
}

// Balance master pods over zones
func balanceMasterPods(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster) error {

	zoneToPrimaries := make(map[string][]rapi.RedisClusterNode)
	zoneToReplicas := make(map[string][]rapi.RedisClusterNode)
	primaryToReplicas := make(map[string][]rapi.RedisClusterNode)

	// Populate the maps
	for _, node := range cluster.Status.Cluster.Nodes {

		if node.Role == rapi.RedisClusterNodeRolePrimary {
			zoneToPrimaries[node.Zone] = append(zoneToPrimaries[node.Zone], node)
		}
		if node.Role == rapi.RedisClusterNodeRoleReplica {
			primaryToReplicas[node.PrimaryRef] = append(primaryToReplicas[node.PrimaryRef], node)
			zoneToReplicas[node.Zone] = append(zoneToReplicas[node.Zone], node)
		}
	}
	eligibleZones := make([]string, 0, len(zoneToPrimaries))
	for k := range zoneToPrimaries {
		eligibleZones = append(eligibleZones, k)
	}
	// Sort eligibleZones in decending order of number of primaries
	sort.Slice(eligibleZones, func(i, j int) bool {
		return len(zoneToPrimaries[eligibleZones[i]]) > len(zoneToPrimaries[eligibleZones[j]])
	})

	// Starting from the zone with the highest number of primaries, check for eligible replica that can take over
	for _, eligibleZone := range eligibleZones {
		for _, primary := range zoneToPrimaries[eligibleZone] {
			// check if any the primary has a replica that is in a zone with fewer primaries
			for _, replica := range primaryToReplicas[primary.ID] {
				if len(zoneToPrimaries[replica.Zone]) < (len(zoneToPrimaries[primary.Zone]) - 1) {
					// Start failover in the eligible replica
					return admin.StartReplicaFailover(ctx, replica.IP+":"+replica.Port)
				}
			}
		}

	}

	glog.V(3).Info("[SanityChecks][FixZoneBalance] No eligible replicas found to take over primaries")
	return nil

}

func FixZoneBalance(ctx context.Context, admin redis.AdminInterface, cluster *rapi.RedisCluster, infos *redis.ClusterInfos, dryRun bool) (bool, error) {
	// Do not run the fix if cluster status is not OK
	if cluster.Status.Cluster.Status != rapi.ClusterStatusOK {
		glog.V(3).Infof("[SanityChecks][FixZoneBalance] Cluster needs to be stable. Current status: %s", cluster.Status.Cluster.Status)
		return false, nil
	}

	if needMasterRebalancing(cluster) {
		glog.V(3).Info("[SanityChecks][FixZoneBalance] Need master rebalancing")
		if !dryRun {
			err := balanceMasterPods(ctx, admin, cluster)
			return true, err
		}
		return true, nil
	}

	glog.V(3).Info("[SanityChecks][FixZoneBalance] No zone imbalance detected")
	return false, nil
}
