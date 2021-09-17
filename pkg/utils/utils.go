package utils

import (
	"context"
	"errors"
	"math"
	"sort"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
)

// IsPodReady check if pod is in ready condition, return the error message otherwise
func IsPodReady(pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, errors.New("no pod")
	}

	// get ready condition
	var readycondition corev1.PodCondition
	found := false
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			readycondition = cond
			found = true
			break
		}
	}

	if !found {
		return false, errors.New("couldn't find ready condition")
	}

	if readycondition.Status != corev1.ConditionTrue {
		return false, errors.New(readycondition.Message)
	}

	return true, nil
}

func GetKubeNodes(ctx context.Context, kubeClient client.Client, nodeSelector map[string]string) ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	err := kubeClient.List(ctx, nodeList, client.MatchingLabels(nodeSelector))
	if err != nil {
		return nil, err
	}
	return nodeList.Items, nil
}

func ListPods(ctx context.Context, kubeClient client.Client, opts []client.ListOption) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	err := kubeClient.List(ctx, podList, opts...)
	if err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func GetZonesFromKubeNodes(nodes []corev1.Node) []string {
	set := make(map[string]struct{})
	var zones []string
	for _, node := range nodes {
		zone, ok := node.Labels[corev1.LabelTopologyZone]
		if ok {
			set[zone] = struct{}{}
		} else {
			set[rapi.UnknownZone] = struct{}{}
		}
	}
	for key := range set {
		zones = append(zones, key)
	}
	sort.Strings(zones)
	return zones
}

func GetZoneSkew(zoneToNodes map[string][]string) int {
	if len(zoneToNodes) == 0 {
		return 0
	}
	largestZoneSize := 0
	smallestZoneSize := math.MaxInt32
	for _, nodes := range zoneToNodes {
		if len(nodes) > largestZoneSize {
			largestZoneSize = len(nodes)
		}
		if len(nodes) < smallestZoneSize {
			smallestZoneSize = len(nodes)
		}
	}
	return largestZoneSize - smallestZoneSize
}

func GetZone(nodeName string, kubeNodes []corev1.Node) string {
	for _, node := range kubeNodes {
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

func GetZoneSkewByRole(zoneToPrimaries map[string][]string, zoneToReplicas map[string][]string) (int, int, bool) {
	primarySkew := GetZoneSkew(zoneToPrimaries)
	replicaSkew := GetZoneSkew(zoneToReplicas)
	return primarySkew, replicaSkew, primarySkew <= 2 && replicaSkew <= 2
}

func ZoneToRole(nodes []rapi.RedisClusterNode) (map[string][]string, map[string][]string) {
	zoneToPrimaries := make(map[string][]string)
	zoneToReplicas := make(map[string][]string)
	for _, node := range nodes {
		if node.Role == rapi.RedisClusterNodeRolePrimary {
			zoneToPrimaries[node.Zone] = append(zoneToPrimaries[node.Zone], node.ID)
		}
		if node.Role == rapi.RedisClusterNodeRoleReplica && node.PrimaryRef != "" {
			zoneToReplicas[node.Zone] = append(zoneToReplicas[node.Zone], node.ID)
		}
	}
	return zoneToPrimaries, zoneToReplicas
}

func GetNbPodsToCreate(cluster *rapi.RedisCluster) int32 {
	nbMigrationPods := 1 + *cluster.Spec.ReplicationFactor
	nbRequiredPods := *cluster.Spec.NumberOfPrimaries * nbMigrationPods
	return nbRequiredPods + nbMigrationPods - cluster.Status.Cluster.NumberOfPods
}
