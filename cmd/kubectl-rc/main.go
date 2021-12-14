package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/IBM/operator-for-redis-cluster/pkg/utils"

	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/golang/glog"
	"github.com/olekukonko/tablewriter"

	kapiv1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
)

func main() {
	namespace := ""
	if val := os.Getenv("KUBECTL_PLUGINS_CURRENT_NAMESPACE"); val != "" {
		namespace = val
	}

	clusterName := ""
	if val := os.Getenv("KUBECTL_PLUGINS_LOCAL_FLAG_RC"); val != "" {
		clusterName = val
	}

	kubeconfigFilePath := getKubeConfigDefaultPath(getHomePath())
	if len(kubeconfigFilePath) == 0 {
		glog.Fatal("error initializing config, the KUBECONFIG environment variable must be defined")
	}

	config, err := configFromPath(kubeconfigFilePath)
	if err != nil {
		glog.Fatalf("error obtaining kubectl config: %v", err)
	}

	restConfig, err := config.ClientConfig()
	if err != nil {
		glog.Fatal(err)
	}

	scheme := apiruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rapi.AddToScheme(scheme))
	client, err := kclient.New(restConfig, kclient.Options{Scheme: scheme})
	if err != nil {
		glog.Fatalf("unable to init kubernetes client from kubeconfig: %v", err)
	}

	rcs := &rapi.RedisClusterList{}
	if clusterName == "" {
		if err = client.List(context.Background(), rcs, kclient.InNamespace(namespace)); err != nil {
			glog.Fatalf("unable to list redisclusters: %v", err)
		}
	} else {
		cluster := &rapi.RedisCluster{}
		namespacedName := types.NamespacedName{Namespace: namespace, Name: clusterName}
		if err = client.Get(context.Background(), namespacedName, cluster); err != nil && !apierrors.IsNotFound(err) {
			glog.Fatalf("unable to get rediscluster %s: %v", clusterName, err)
		}
		rcs.Items = append(rcs.Items, *cluster)
	}

	var wg = sync.WaitGroup{}
	clusterStatuses := buildRedisClusterStatuses(rcs, client, restConfig, &wg)
	var data [][]string
	for _, cluster := range rcs.Items {
		data = append(data, []string{cluster.Name, cluster.Namespace, buildPodStatus(&cluster), buildClusterStatus(&cluster), string(cluster.Status.Cluster.Status), buildPrimaryStatus(&cluster), buildReplicationStatus(&cluster), buildZoneSkew(&cluster)})
	}
	wg.Wait()
	for _, cs := range clusterStatuses {
		cs.outputRedisClusterStatus()
	}
	outputRedisClusterState(data)

	os.Exit(0)
}

func buildRedisClusterStatuses(rcs *rapi.RedisClusterList, client kclient.Client, restConfig *rest.Config, wg *sync.WaitGroup) []*ClusterStatus {
	// done this way to preserve the ordering of `rcs.Items`
	clusterStatuses := make([]*ClusterStatus, len(rcs.Items))
	var lock = sync.Mutex{}
	for i, cluster := range rcs.Items {
		wg.Add(1)
		cluster := cluster
		index := i
		go func() {
			defer wg.Done()
			cs, err := NewClusterStatus(client, &cluster, restConfig)
			if err != nil {
				glog.Fatalf("cannot build redis cluster status for %s/%s: %v", cluster.Namespace, cluster.Name, err)
			}
			lock.Lock()
			clusterStatuses[index] = cs
			lock.Unlock()
		}()
	}
	return clusterStatuses
}

func outputRedisClusterState(data [][]string) {
	if len(data) == 0 {
		fmt.Println("No resources found.")
		os.Exit(0)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Namespace", "Pods", "Ops Status", "Redis Status", "Nb Primary", "Replication", "Zone Skew"})
	table.SetBorders(tablewriter.Border{Left: false, Top: false, Right: false, Bottom: false})
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetRowLine(false)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderLine(false)

	for _, v := range data {
		table.Append(v)
	}
	table.Render() // Send output
}

func hasStatus(cluster *rapi.RedisCluster, conditionType rapi.RedisClusterConditionType, status kapiv1.ConditionStatus) bool {
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == conditionType && cond.Status == status {
			return true
		}
	}
	return false
}

func buildClusterStatus(cluster *rapi.RedisCluster) string {
	var status []string

	if hasStatus(cluster, rapi.RedisClusterOK, kapiv1.ConditionFalse) {
		status = append(status, "KO")
	} else if hasStatus(cluster, rapi.RedisClusterOK, kapiv1.ConditionTrue) {
		status = append(status, string(rapi.RedisClusterOK))
	}

	if hasStatus(cluster, rapi.RedisClusterRollingUpdate, kapiv1.ConditionTrue) {
		status = append(status, string(rapi.RedisClusterRollingUpdate))
	}
	if hasStatus(cluster, rapi.RedisClusterScaling, kapiv1.ConditionTrue) {
		status = append(status, string(rapi.RedisClusterScaling))
	}
	if hasStatus(cluster, rapi.RedisClusterRebalancing, kapiv1.ConditionTrue) {
		status = append(status, string(rapi.RedisClusterRebalancing))
	}

	return strings.Join(status, "-")
}

func buildPodStatus(cluster *rapi.RedisCluster) string {
	numberOfPrimaries := *cluster.Spec.NumberOfPrimaries
	replicationFactor := *cluster.Spec.ReplicationFactor
	podWanted := numberOfPrimaries * (1 + replicationFactor)

	numPods := cluster.Status.Cluster.NumberOfPods
	numPodsReady := cluster.Status.Cluster.NumberOfPodsReady

	return fmt.Sprintf("%d/%d/%d", numPodsReady, numPods, podWanted)
}

func buildPrimaryStatus(cluster *rapi.RedisCluster) string {
	return fmt.Sprintf("%d/%d", cluster.Status.Cluster.NumberOfPrimaries, *cluster.Spec.NumberOfPrimaries)
}

func buildReplicationStatus(cluster *rapi.RedisCluster) string {
	spec := *cluster.Spec.ReplicationFactor
	return fmt.Sprintf("%d-%d/%d", cluster.Status.Cluster.MinReplicationFactor, cluster.Status.Cluster.MaxReplicationFactor, spec)
}

func buildZoneSkew(cluster *rapi.RedisCluster) string {
	balanced := "BALANCED"
	primarySkew, replicaSkew, ok := utils.GetZoneSkewByRole(utils.ZoneToRole(cluster.Status.Cluster.Nodes))
	if !ok {
		balanced = "UNBALANCED"
	}
	return fmt.Sprintf("%d/%d/%s", primarySkew, replicaSkew, balanced)
}

func configFromPath(path string) (clientcmd.ClientConfig, error) {
	flag.Parse()
	rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: path}
	credentials, err := rules.Load()
	if err != nil {
		return nil, fmt.Errorf("the provided credentials %q could not be loaded: %v", path, err)
	}

	overrides := &clientcmd.ConfigOverrides{
		Context: clientcmdapi.Context{
			Namespace: os.Getenv("KUBECTL_PLUGINS_GLOBAL_FLAG_NAMESPACE"),
		},
	}

	kcontext := os.Getenv("KUBECTL_PLUGINS_GLOBAL_FLAG_CONTEXT")
	if len(kcontext) > 0 {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		return clientcmd.NewNonInteractiveClientConfig(*credentials, kcontext, overrides, rules), nil
	}
	return clientcmd.NewDefaultClientConfig(*credentials, overrides), nil
}

func getHomePath() string {
	home := os.Getenv("HOME")
	if runtime.GOOS == "windows" {
		home = os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
	}

	return home
}

func getKubeConfigDefaultPath(home string) string {
	kubeconfig := filepath.Join(home, ".kube", "config")

	kubeconfigEnv := os.Getenv("KUBECONFIG")
	if len(kubeconfigEnv) > 0 {
		kubeconfig = kubeconfigEnv
	}

	configFile := os.Getenv("KUBECTL_PLUGINS_GLOBAL_FLAG_CONFIG")
	kubeConfigFile := os.Getenv("KUBECTL_PLUGINS_GLOBAL_FLAG_KUBECONFIG")
	if len(configFile) > 0 {
		kubeconfig = configFile
	} else if len(kubeConfigFile) > 0 {
		kubeconfig = kubeConfigFile
	}

	return kubeconfig
}
