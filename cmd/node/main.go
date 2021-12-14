package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	"github.com/IBM/operator-for-redis-cluster/pkg/redisnode"
	"github.com/IBM/operator-for-redis-cluster/pkg/utils"
)

func main() {
	utils.BuildInfos()
	config := redisnode.NewRedisNodeConfig()
	config.AddFlags(pflag.CommandLine)

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		glog.Errorf("goflag.CommandLine.Parse failed: %v", err)
		os.Exit(1)
	}

	rn := redisnode.NewRedisNode(config)

	if err := run(rn); err != nil {
		glog.Errorf("redis-node returned an error:%v", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func run(rn *redisnode.RedisNode) error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func(cancelFunc context.CancelFunc) {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT)
		sig := <-sigc
		glog.Infof("signal received: %s, stop the process", sig.String())
		cancelFunc()
	}(cancelFunc)

	return rn.Run(ctx.Done())
}
