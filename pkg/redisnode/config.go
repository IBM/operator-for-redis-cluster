package redisnode

import (
	"time"

	"github.com/IBM/operator-for-redis-cluster/pkg/config"
	"github.com/spf13/pflag"
)

const (
	// RedisStartWaitDefault default stop duration (sec)
	RedisStartWaitDefault = 10 * time.Second
	// RedisStartDelayDefault default start delay duration (sec)
	RedisStartDelayDefault = 10 * time.Second
	// HTTPServerAddrDefault default http server address
	HTTPServerAddrDefault = "0.0.0.0:8080"
)

// Config contains configuration for redis-operator
type Config struct {
	KubeConfigFile  string
	KubeAPIServer   string
	Redis           config.Redis
	Cluster         config.Cluster
	RedisStartWait  time.Duration
	RedisStartDelay time.Duration
	HTTPServerAddr  string
}

// NewRedisNodeConfig builds and returns a redis-operator Config
func NewRedisNodeConfig() *Config {

	return &Config{}
}

// AddFlags add cobra flags to populate Config
func (c *Config) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&c.KubeConfigFile, "kubeconfig", c.KubeConfigFile, "location of kubeconfig file for access to kubernetes service")
	fs.StringVar(&c.KubeAPIServer, "kube-api-server", c.KubeAPIServer, "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	fs.DurationVar(&c.RedisStartWait, "t", RedisStartWaitDefault, "max time waiting for redis to start")
	fs.DurationVar(&c.RedisStartDelay, "d", RedisStartDelayDefault, "delay before that the redis-server is started")
	fs.StringVar(&c.HTTPServerAddr, "http-addr", HTTPServerAddrDefault, "the http server listen address")

	c.Redis.AddFlags(fs)
	c.Cluster.AddFlags(fs)
}
