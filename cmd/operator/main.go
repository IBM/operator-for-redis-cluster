package main

import (
	"flag"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller"
	"github.com/IBM/operator-for-redis-cluster/pkg/garbagecollector"
	"github.com/IBM/operator-for-redis-cluster/pkg/operator"
	"github.com/golang/glog"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	leaderElectionID = "redis-operator-leader-election-lock"
)

func main() {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rapi.AddToScheme(scheme))

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	config := operator.NewRedisOperatorConfig()
	config.AddFlags(pflag.CommandLine)
	if err := config.ParseEnvironment(); err != nil {
		glog.Fatalf("Failed to parse environment variables: %v", err)
	}
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		glog.Fatalf("Failed to parse args: %v", err)
	}

	restConfig := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                 scheme,
		MetricsBindAddress:     config.MetricsAddr,
		HealthProbeBindAddress: config.HealthCheckAddr,
		LeaderElection:         config.LeaderElectionEnabled,
		LeaderElectionID:       leaderElectionID,
	})
	if err != nil {
		glog.Fatalf("unable to start manager: %v", err)
	}

	ctrlCfg := controller.NewConfig(1, config.Redis)
	redisClusterCtrl := controller.NewController(ctrlCfg, mgr, mgr.GetClient(), mgr.GetEventRecorderFor("rediscluster-controller"))
	if err = controller.SetupRedisClusterController(mgr, redisClusterCtrl); err != nil {
		glog.Fatalf("unable to set up rediscluster controller: %v", err)
	}

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		glog.Fatalf("unable to set up health check: %v", err)
	}
	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		glog.Fatalf("unable to set up ready check: %v", err)
	}

	ctx := ctrl.SetupSignalHandler()

	gc := garbagecollector.NewGarbageCollector(mgr.GetClient())
	go gc.Run(ctx.Done())

	glog.Info("starting manager")
	if err = mgr.Start(ctx); err != nil {
		glog.Fatalf("problem running manager: %v", err)
	}
}
