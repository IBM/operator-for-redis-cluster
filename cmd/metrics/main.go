package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	metricsPath = "/metrics"
)

var (
	namespace   = os.Getenv("NAMESPACE")
	clusterName = os.Getenv("CLUSTER_NAME")
	serverPort  = os.Getenv("SERVER_PORT")

	nodePerZone = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_per_zone",
		Help: "Number of Redis nodes per zone",
	}, []string{"name"})

	availableZones = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "available_zones",
		Help: "Zones available to Redis cluster",
	}, []string{"name"})

	zoneSkew = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "zone_skew",
		Help: "Zone skew of Redis nodes",
	}, []string{"name"})
)

func recordMetrics(cluster *rapi.RedisCluster, k8sNodes []v1.Node) {
	zones := utils.GetZones(k8sNodes)
	zoneToNodeCount := map[string]int{}
	for _, zone := range zones {
		zoneToNodeCount[zone] = 0
		availableZones.With(prometheus.Labels{"name": zone}).Set(float64(0))
	}
	for _, node := range cluster.Status.Cluster.Nodes {
		zoneToNodeCount[node.Zone] += 1
	}
	for zone, count := range zoneToNodeCount {
		nodePerZone.With(prometheus.Labels{"name": zone}).Set(float64(count))
	}
	zoneToPrimaries, zoneToReplicas := utils.ZoneToRole(cluster.Status.Cluster.Nodes)
	primarySkew, replicaSkew, _ := utils.GetZoneSkewByRole(zoneToPrimaries, zoneToReplicas)
	zoneSkew.With(prometheus.Labels{"name": "primaries"}).Set(float64(primarySkew))
	zoneSkew.With(prometheus.Labels{"name": "replicas"}).Set(float64(replicaSkew))
}

func watchRedisCluster(client kclient.Client, rClient *rest.RESTClient, cluster *rapi.RedisCluster) chan struct{} {
	options := func(options *metav1.ListOptions) {
		options.LabelSelector = labels.FormatLabels(cluster.Labels)
	}
	watchlist := cache.NewFilteredListWatchFromClient(
		rClient,
		rapi.ResourcePlural,
		namespace,
		options,
	)

	_, controller := cache.NewInformer(
		watchlist,
		&rapi.RedisCluster{},
		0*time.Second,
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj interface{}, newObj interface{}) {
				if newCluster, ok := newObj.(*rapi.RedisCluster); ok {
					k8sNodes, err := utils.GetKubeNodes(context.Background(), client, cluster.Spec.PodTemplate.Spec.NodeSelector)
					if err != nil {
						log.Fatalf("unable to get kube nodes: %v", err)
					}
					recordMetrics(newCluster, k8sNodes)
				}
			},
		},
	)
	stop := make(chan struct{})
	go controller.Run(stop)
	return stop
}

func main() {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("unable to get rest config: %v", err)
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rapi.AddToScheme(scheme))
	client, err := kclient.New(restConfig, kclient.Options{Scheme: scheme})
	if err != nil {
		log.Fatalf("unable to create k8s client: %v", err)
	}
	cluster := &rapi.RedisCluster{}
	namespacedName := types.NamespacedName{Namespace: namespace, Name: clusterName}
	if err = client.Get(context.Background(), namespacedName, cluster); err != nil && !apierrors.IsNotFound(err) {
		log.Fatalf("unable to get redis cluster %s/%s: %v", namespace, clusterName, err)
	}
	rClient, err := rapi.NewClient(restConfig)
	if err != nil {
		log.Fatalf("unable to create k8s client: %v", err)
	}
	watchCh := watchRedisCluster(client, rClient, cluster)
	defer close(watchCh)
	http.Handle(metricsPath, promhttp.Handler())
	log.Printf("Serving requests on port %s\n", serverPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", serverPort), nil))
}
