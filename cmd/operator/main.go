package main

import (
	"context"
	goflag "flag"
	"os"
	. "runtime"
	"time"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/operator"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/signal"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/utils"
	"github.com/golang/glog"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 3 * time.Second
)

func main() {
	utils.BuildInfos()
	GOMAXPROCS(NumCPU())

	config := operator.NewRedisOperatorConfig()
	config.AddFlags(pflag.CommandLine)
	if err := config.ParseEnvironment(); err != nil {
		glog.Fatalf("Failed to parse environment variables: %v", err)
	}
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()
	goflag.CommandLine.Parse([]string{})

	op := operator.NewRedisOperator(config)

	ctx, cancel := context.WithCancel(context.Background())
	go signal.HandleSignal(cancel)

	glog.Infof("Leader election enabled: %v", config.LeaderElectionEnabled)
	go op.RunHttpServer(ctx.Done())
	go op.RunMetricsServer(ctx.Done())
	if config.LeaderElectionEnabled {
		runLeaderElection(ctx, op, config)
	} else {
		run(ctx, op)
	}
	os.Exit(0)
}

func leaderElectionConfig(op *operator.RedisOperator, cfg *operator.Config) leaderelection.LeaderElectionConfig {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      operator.LeaderElectionLock,
			Namespace: cfg.Namespace,
		},
		Client: op.Client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: op.LeaseID,
		},
	}
	return leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				run(ctx, op)
			},
			OnStoppedLeading: func() {
				glog.Infof("Redis operator leader with id %s lost", op.LeaseID)
				os.Exit(0)
			},
			OnNewLeader: func(id string) {
				if id == op.LeaseID {
					return
				}
				glog.Infof("Redis operator leader with id %s elected", id)
			},
		},
	}
}

func runLeaderElection(ctx context.Context, op *operator.RedisOperator, cfg *operator.Config) {
	lec := leaderElectionConfig(op, cfg)
	le, err := leaderelection.NewLeaderElector(lec)
	if err != nil {
		glog.Fatalf("Failed to create new leader elector: %v", err)
	}
	le.Run(ctx)
}

func run(ctx context.Context, op *operator.RedisOperator) {
	if err := op.Run(ctx.Done()); err != nil {
		glog.Errorf("Redis operator returned an error:%v", err)
		os.Exit(1)
	}
}
