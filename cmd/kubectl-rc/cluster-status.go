package main

import (
	"context"
	"os"
	"sort"
	"sync"

	"github.com/golang/glog"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/olekukonko/tablewriter"
	kapiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type ClusterStatus struct {
	cluster            *rapi.RedisCluster
	podNameToInfo      map[string]*PodInfo
	primaryIDToPodName map[string]string
	primaryToReplicas  map[string][]string
	pods               []kapiv1.Pod
	statusUnknownPods  []*PodInfo
}

func NewClusterStatus(client kclient.Client, cluster *rapi.RedisCluster, restConfig *rest.Config) (*ClusterStatus, error) {
	cs := ClusterStatus{
		cluster:            cluster,
		podNameToInfo:      map[string]*PodInfo{},
		primaryIDToPodName: map[string]string{},
		primaryToReplicas:  map[string][]string{},
		pods:               []kapiv1.Pod{},
		statusUnknownPods:  []*PodInfo{},
	}

	if err := cs.getRedisPods(client); err != nil {
		return nil, err
	}

	cs.populatePrimaryIDToPodNameMap()
	cs.populatePrimaryToReplicasMap()
	cs.populatePodNameToInfoMap()
	cs.populatePodInfoStats(restConfig, cs.podNameToInfo)
	cs.populateStatusUnknownPods(restConfig)
	return &cs, nil
}

// NOTE: this is operating on a podNameToInfo that is passed in (as opposed to the one in the struct)
// this is because the same logic is used to retrieve pod info stats for unknown/new pods
func (cs *ClusterStatus) populatePodInfoStats(restConfig *rest.Config, podInfoMap map[string]*PodInfo) {
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		glog.Fatalf("unable to get clientset: %v", err)
	}

	scheme := apiruntime.NewScheme()
	if err := kapiv1.AddToScheme(scheme); err != nil {
		glog.Fatalf("error adding to scheme: %v", err)
	}
	parameterCodec := apiruntime.NewParameterCodec(scheme)

	wg := sync.WaitGroup{}
	for _, pod := range podInfoMap {
		wg.Add(1)
		pod := pod
		go func() {
			defer wg.Done()
			cs.addToPodInfo(restConfig, clientset, pod.pod, parameterCodec, podInfoMap)
		}()
	}

	wg.Wait()
}

func (cs *ClusterStatus) addToPodInfo(restConfig *rest.Config, clientset *kubernetes.Clientset, pod *kapiv1.Pod, parameterCodec apiruntime.ParameterCodec, podInfoMap map[string]*PodInfo) {
	podInfo, ok := podInfoMap[pod.Name]
	if !ok {
		glog.Infof("couldn't find match in podNameToInfo for pod %s", pod.Name)
		return
	}
	stdout, err := execCommandOnPod(restConfig, clientset, pod, parameterCodec, noCommandOverride)
	if err != nil {
		glog.Infof("failed to exec command on pod %s", pod.Name)
	} else {
		lines := parseCommandOutput(stdout, pod)
		podInfo.populateMemoryStats(lines)
		podInfo.populateKeyStats(lines)
	}
}

func (cs *ClusterStatus) populateStatusUnknownPods(restConfig *rest.Config) {
	unknownPodInfoMap := map[string]*PodInfo{}
	for _, pod := range cs.pods {
		if _, ok := cs.podNameToInfo[pod.Name]; !ok {
			podInfo := NewPodInfoFromClusterStatus(&pod)
			cs.statusUnknownPods = append(cs.statusUnknownPods, podInfo)
			unknownPodInfoMap[pod.Name] = podInfo
		}
	}
	cs.populatePodInfoStats(restConfig, unknownPodInfoMap)
}

func (cs *ClusterStatus) populatePodNameToInfoMap() {
	for _, node := range cs.cluster.Status.Cluster.Nodes {
		for _, pod := range cs.pods {
			if pod.Name == node.PodName {
				cs.podNameToInfo[pod.Name] = NewPodInfo(&pod, node)
				break
			}
		}
	}
}

func (cs *ClusterStatus) populatePrimaryIDToPodNameMap() {
	for _, node := range cs.cluster.Status.Cluster.Nodes {
		if node.Role == rapi.RedisClusterNodeRolePrimary {
			cs.primaryIDToPodName[node.ID] = node.PodName
		}
	}
}

func (cs *ClusterStatus) populatePrimaryToReplicasMap() {
	for _, node := range cs.cluster.Status.Cluster.Nodes {
		if node.Role == rapi.RedisClusterNodeRoleReplica && node.PrimaryRef != "" {
			if primaryPodName, ok := cs.primaryIDToPodName[node.PrimaryRef]; ok {
				replicas := cs.primaryToReplicas[primaryPodName]
				cs.primaryToReplicas[primaryPodName] = append(replicas, node.PodName)
			}
		} else if node.Role == rapi.RedisClusterNodeRolePrimary {
			if _, ok := cs.primaryToReplicas[node.PodName]; !ok {
				cs.primaryToReplicas[node.PodName] = []string{}
			}
		}
	}
}

func (cs *ClusterStatus) getRedisPods(client kclient.Client) error {
	labelSet := map[string]string{
		rapi.ClusterNameLabelKey: cs.cluster.Name,
	}
	podList := &kapiv1.PodList{}
	err := client.List(context.Background(), podList, kclient.InNamespace(cs.cluster.Namespace), kclient.MatchingLabels(labelSet))
	if err == nil {
		cs.pods = podList.Items
	}
	return err
}

func (cs *ClusterStatus) outputRedisClusterStatus() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"POD_NAME", "IP", "NODE", "ID", "ZONE", "USED MEMORY", "MAX MEMORY", "KEYS", "SLOTS"})
	table.SetBorders(tablewriter.Border{Left: false, Top: false, Right: false, Bottom: false})
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetRowLine(false)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderLine(false)
	table.SetAutoWrapText(false)

	var sortedPrimaries []string
	for primary := range cs.primaryToReplicas {
		sortedPrimaries = append(sortedPrimaries, primary)
	}
	sort.Strings(sortedPrimaries)

	var info [][]string
	var joiningPods []string // if a primary doesn't have replicas and RF > 0, assume it's joining

	for _, primary := range sortedPrimaries {
		podInfo, ok := cs.podNameToInfo[primary]
		if !ok {
			glog.Infof("unable to find %s in podNameToInfo", primary)
			continue
		}
		replicas := cs.primaryToReplicas[primary]
		sort.Strings(replicas)
		if len(replicas) == 0 && cs.cluster.Status.Cluster.MinReplicationFactor > 0 {
			joiningPods = append(joiningPods, primary)
		} else {
			info = append(info, []string{podInfo.populateRoleChar() + podInfo.name, podInfo.ip, podInfo.node, podInfo.id, podInfo.zone, podInfo.usedMemory, podInfo.maxMemory, podInfo.keys, podInfo.slots})
			for _, replica := range replicas {
				replicaPodInfo := cs.podNameToInfo[replica]
				info = append(info, []string{"| " + replicaPodInfo.name, replicaPodInfo.ip, replicaPodInfo.node, replicaPodInfo.id, replicaPodInfo.zone, replicaPodInfo.usedMemory, replicaPodInfo.maxMemory, replicaPodInfo.keys, replicaPodInfo.slots})
			}
		}
	}

	sort.Strings(joiningPods)
	for _, pod := range joiningPods {
		podInfo := cs.podNameToInfo[pod]
		info = append(info, []string{"^ " + podInfo.name, podInfo.ip, podInfo.node, podInfo.id, podInfo.zone, podInfo.usedMemory, podInfo.maxMemory, podInfo.keys, podInfo.slots})
	}

	for _, pod := range cs.statusUnknownPods {
		info = append(info, []string{"? " + pod.name, pod.ip, pod.node, pod.id, pod.zone, pod.usedMemory, pod.maxMemory, pod.keys, pod.slots})
	}

	for _, v := range info {
		table.Append(v)
	}
	table.Append([]string{"", ""})
	table.Render() // Send output
}
