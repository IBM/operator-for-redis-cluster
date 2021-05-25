package operator

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/google/uuid"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/golang/glog"
	"github.com/heptiolabs/healthcheck"

	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	rclient "github.com/TheWeatherCompany/icm-redis-operator/pkg/client"
	redisinformers "github.com/TheWeatherCompany/icm-redis-operator/pkg/client/informers/externalversions"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/garbagecollector"
)

const (
	LeaderElectionLock = "redis-operator-leader-election-lock"
)

// RedisOperator contains all info to run the redis operator
type RedisOperator struct {
	LeaseID              string // LeaseID used for leader election lock
	Client               *clientset.Clientset
	kubeInformerFactory  kubeinformers.SharedInformerFactory
	redisInformerFactory redisinformers.SharedInformerFactory
	controller           *controller.Controller
	GC                   garbagecollector.Interface
	health               healthcheck.Handler // Kubernetes Probes handler
	httpServer           *http.Server
	config               *Config
}

// NewRedisOperator builds and returns new RedisOperator instance
func NewRedisOperator(cfg *Config) *RedisOperator {
	kubeConfig, err := initKubeConfig(cfg)
	if err != nil {
		glog.Fatalf("Unable to init redis cluster controller: %v", err)
	}

	kubeClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		glog.Fatalf("Unable to initialize kubeClient:%v", err)
	}

	redisClient, err := rclient.NewClient(kubeConfig)
	if err != nil {
		glog.Fatalf("Unable to init redis.clientset from kubeconfig:%v", err)
	}

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	redisInformerFactory := redisinformers.NewSharedInformerFactory(redisClient, time.Second*30)

	op := &RedisOperator{
		LeaseID:              uuid.New().String(),
		Client:               kubeClient,
		kubeInformerFactory:  kubeInformerFactory,
		redisInformerFactory: redisInformerFactory,
		controller:           controller.NewController(controller.NewConfig(1, cfg.Redis), kubeClient, redisClient, kubeInformerFactory, redisInformerFactory),
		GC:                   garbagecollector.NewGarbageCollector(redisClient, kubeClient, redisInformerFactory),
		health:               healthcheck.NewHandler(),
		config:               cfg,
	}

	op.configureHealthChecks()
	op.httpServer = &http.Server{Addr: cfg.ListenAddr, Handler: op.health}

	return op
}

// Run executes all of the necessary components to start processing requests
func (op *RedisOperator) Run(stop <-chan struct{}) error {
	var err error
	if op.controller != nil {
		op.redisInformerFactory.Start(stop)
		op.kubeInformerFactory.Start(stop)
		err = op.controller.Run(stop)
		op.runGC(stop)
	}
	return err
}

func (op *RedisOperator) runGC(stop <-chan struct{}) {
	go func() {
		if !cache.WaitForCacheSync(stop, op.GC.InformerSync()) {
			glog.Errorf("Timed out waiting for caches to sync")
		}
		wait.Until(func() {
			err := op.GC.CollectRedisClusterGarbage()
			if err != nil {
				glog.Errorf("collecting rediscluster pods and services: %v", err)
			}
		}, garbagecollector.Interval, stop)
		// run garbage collection before stopping the process
		op.GC.CollectRedisClusterGarbage()
	}()
}

func initKubeConfig(c *Config) (*rest.Config, error) {
	if len(c.KubeConfigFile) > 0 {
		return clientcmd.BuildConfigFromFlags(c.Master, c.KubeConfigFile) // out of cluster config
	}
	return rest.InClusterConfig()
}

func (op *RedisOperator) isLeader() bool {
	// If leader election is disabled, the operator must be the leader
	if !op.config.LeaderElectionEnabled {
		return true
	}
	lease, err := op.Client.CoordinationV1().Leases(op.config.Namespace).Get(context.Background(), LeaderElectionLock, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("Error getting lease %s: %v", LeaderElectionLock, err)
	}
	return *lease.Spec.HolderIdentity == op.LeaseID
}

func (op *RedisOperator) configureHealthChecks() {
	op.health.AddReadinessCheck("RedisCluster_cache_sync", func() error {
		if !op.isLeader() || op.controller.RedisClusterSynced() {
			return nil
		}
		return fmt.Errorf("RedisCluster cache not sync")
	})
	op.health.AddReadinessCheck("Pod_cache_sync", func() error {
		if !op.isLeader() || op.controller.PodSynced() {
			return nil
		}
		return fmt.Errorf("Pod cache not sync")
	})
	op.health.AddReadinessCheck("Service_cache_sync", func() error {
		if !op.isLeader() || op.controller.ServiceSynced() {
			return nil
		}
		return fmt.Errorf("Service cache not sync")
	})
	op.health.AddReadinessCheck("PodDisruptionBudget_cache_sync", func() error {
		if !op.isLeader() || op.controller.PodDiscruptionBudgetSynced() {
			return nil
		}
		return fmt.Errorf("PodDiscruptionBudget cache not sync")
	})
}

func (op *RedisOperator) RunHttpServer(stop <-chan struct{}) error {
	go func() {
		glog.Infof("Operator server listening on http://%s\n", op.httpServer.Addr)
		if err := op.httpServer.ListenAndServe(); err != nil {
			glog.Error("Http server error: ", err)
		}
	}()

	<-stop
	glog.Info("Shutting down the http server...")
	return op.httpServer.Shutdown(context.Background())
}

func (op *RedisOperator) RunMetricsServer(stop <-chan struct{}) error {
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.ReconcileErrors)
	registry.MustRegister(metrics.ReconcileTotal)
	registry.MustRegister(metrics.ReconcileTime)

	metricsServer := &http.Server{
		Addr:    op.config.MetricsAddr,
		Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}
	go func() {
		glog.Infof("Metrics server listening on http://%s\n", metricsServer.Addr)
		if err := metricsServer.ListenAndServe(); err != nil {
			glog.Errorf("Can't start metrics server: %v", err)
		}
	}()

	<-stop
	glog.Info("Shutting down the metrics server...")
	return op.httpServer.Shutdown(context.Background())
}
