package e2e

import (
	goflag "flag"
	"github.com/golang/glog"
	"os"
	"testing"

	"github.com/TheWeatherCompany/icm-redis-operator/test/e2e/framework"
	"github.com/spf13/pflag"
)

func TestE2E(t *testing.T) {
	RunE2ETests(t)
}

func TestMain(m *testing.M) {

	pflag.StringVar(&framework.FrameworkContext.KubeConfigPath, "kubeconfig", "", "Path to kubeconfig")
	pflag.StringVar(&framework.FrameworkContext.ImageTag, "image-tag", "local", "image tag")
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()
	err := goflag.CommandLine.Parse([]string{})
	if err != nil {
		glog.Errorf("failed to parse args: %v", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}
