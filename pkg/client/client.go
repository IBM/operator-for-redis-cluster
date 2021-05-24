package client

import (
	"context"
	"reflect"
	"time"

	"github.com/gogo/protobuf/proto"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"

	"github.com/golang/glog"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis"
	v1 "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/client/clientset/versioned"
)

// DefineRedisClusterResource defines a RedisClusterResource as a k8s CR
func DefineRedisClusterResource(clientset apiextensionsclient.Interface) (*apiextensionsv1.CustomResourceDefinition, error) {
	redisClusterResourceName := v1.ResourcePlural + "." + redis.GroupName
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: redisClusterResourceName,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: redis.GroupName,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    v1.SchemeGroupVersion.Version,
				Served:  true,
				Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						LabelSelectorPath:  proto.String(".status.cluster.labelSelectorPath"),
						SpecReplicasPath:   ".spec.numberOfMaster",
						StatusReplicasPath: ".status.cluster.numberOfMastersReady",
					},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					XPreserveUnknownFields: proto.Bool(true),
					Type:                   "object",
				}},
			}},
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:     v1.ResourcePlural,
				Singular:   v1.ResourceSingular,
				Kind:       reflect.TypeOf(v1.RedisCluster{}).Name(),
				ShortNames: []string{"rdc"},
			},
		},
	}
	_, err := clientset.ApiextensionsV1().CustomResourceDefinitions().Create(context.Background(), crd, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	// wait for CRD being established
	err = wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		crd, err = clientset.ApiextensionsV1().CustomResourceDefinitions().Get(context.Background(), redisClusterResourceName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1.Established:
				if cond.Status == apiextensionsv1.ConditionTrue {
					return true, err
				}
			case apiextensionsv1.NamesAccepted:
				if cond.Status == apiextensionsv1.ConditionFalse {
					glog.Errorf("Name conflict: %v\n", cond.Reason)
				}
			}
		}
		return false, err
	})
	if err != nil {
		deleteErr := clientset.ApiextensionsV1().CustomResourceDefinitions().Delete(context.Background(), redisClusterResourceName, metav1.DeleteOptions{})
		if deleteErr != nil {
			return nil, errors.NewAggregate([]error{err, deleteErr})
		}
		return nil, err
	}

	return crd, nil
}

// NewClient builds and initializes a Client and a Scheme for RedisCluster CR
func NewClient(cfg *rest.Config) (versioned.Interface, error) {
	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		return nil, err
	}

	config := *cfg
	config.GroupVersion = &v1.SchemeGroupVersion
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.WithoutConversionCodecFactory{CodecFactory: serializer.NewCodecFactory(scheme)}

	cs, err := versioned.NewForConfig(&config)
	if err != nil {
		return nil, err
	}
	return cs, nil
}
