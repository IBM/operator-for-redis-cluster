package operator

import (
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/config"
	"github.com/caarlos0/env/v6"
	"github.com/spf13/pflag"
)

// Config contains configuration for redis-operator
type Config struct {
	Namespace             string `env:"NAMESPACE" envDefault:"default"`
	LeaderElectionEnabled bool   `env:"LEADERELECTION_ENABLED" envDefault:"true"`
	KubeConfigFile        string
	Master                string
	ListenAddr            string
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
	fs.StringVar(&c.KubeConfigFile, "kubeconfig", c.KubeConfigFile, "Location of kubeconfig file for access to kubernetes master service")
	fs.StringVar(&c.Master, "master", c.Master, "Address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	fs.StringVar(&c.ListenAddr, "addr", "0.0.0.0:8086", "Listen address of the http server which serves kubernetes probes and prometheus endpoints")
	c.Redis.AddFlags(fs)
}
