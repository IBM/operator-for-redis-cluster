package operator

import (
	"github.com/IBM/operator-for-redis-cluster/pkg/config"
	"github.com/caarlos0/env/v6"
	"github.com/spf13/pflag"
)

// Config contains configuration for redis-operator
type Config struct {
	Namespace             string `env:"NAMESPACE" envDefault:"default"`
	LeaderElectionEnabled bool   `env:"LEADERELECTION_ENABLED" envDefault:"true"`
	KubeConfigFile        string
	KubeAPIServer         string
	HealthCheckAddr       string
	MetricsAddr           string
	Redis                 config.Redis
}

// NewRedisOperatorConfig builds and returns a redis-operator Config
func NewRedisOperatorConfig() *Config {
	return &Config{}
}

// ParseEnvironment parses Config for environment variables
func (c *Config) ParseEnvironment() error {
	if err := env.Parse(c); err != nil {
		return err
	}
	return nil
}

// AddFlags add cobra flags to populate Config
func (c *Config) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&c.KubeConfigFile, "kubeconfig", c.KubeConfigFile, "Location of kubeconfig file for access to kubernetes primary service")
	fs.StringVar(&c.KubeAPIServer, "kube-api-server", c.KubeAPIServer, "Address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	fs.StringVar(&c.HealthCheckAddr, "health-check-addr", "0.0.0.0:8086", "Listen address of the http server which serves kubernetes probes")
	fs.StringVar(&c.MetricsAddr, "metricsAddr", "0.0.0.0:2112", "Listen address of the metrics server which serves controller metrics")
	c.Redis.AddFlags(fs)
}
