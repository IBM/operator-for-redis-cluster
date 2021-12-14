package framework

import (
	"fmt"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Framework stores necessary info to run e2e
type Framework struct {
	KubeConfig *rest.Config
}

type frameworkContextType struct {
	KubeConfigPath string
	ImageTag       string
}

// FrameworkContext stores the framework context
var FrameworkContext frameworkContextType

// NewFramework creates and initializes the Framework struct
func NewFramework() (*Framework, error) {
	Logf("KubeconfigPath-> %q", FrameworkContext.KubeConfigPath)
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", FrameworkContext.KubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("cannot retrieve kubeConfig:%v", err)
	}
	return &Framework{
		KubeConfig: kubeConfig,
	}, nil
}

func (f *Framework) kubeClient() (kclient.Client, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rapi.AddToScheme(scheme))
	return kclient.New(f.KubeConfig, kclient.Options{Scheme: scheme})
}
