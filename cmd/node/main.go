package main

import (
	"context"
	goflag "flag"
	"os"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redisnode"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/signal"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/utils"
)

func main() {
	utils.BuildInfos()
	config := redisnode.NewRedisNodeConfig()
	config.AddFlags(pflag.CommandLine)

	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()
	err := goflag.CommandLine.Parse([]string{})
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
	go signal.HandleSignal(cancelFunc)

	return rn.Run(ctx.Done())
}
