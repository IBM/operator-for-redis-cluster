package controller

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/golang/glog"
	"k8s.io/client-go/tools/record"

	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	policyv1listers "k8s.io/client-go/listers/policy/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1"
	rclient "github.com/TheWeatherCompany/icm-redis-operator/pkg/client/clientset/versioned"
	rinformers "github.com/TheWeatherCompany/icm-redis-operator/pkg/client/informers/externalversions"
	rlisters "github.com/TheWeatherCompany/icm-redis-operator/pkg/client/listers/redis/v1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/pod"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/sanitycheck"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/metrics"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

// Controller contains all controller fields
type Controller struct {
	kubeClient  clientset.Interface
	redisClient rclient.Interface

	redisClusterLister rlisters.RedisClusterLister
	RedisClusterSynced cache.InformerSynced

	podLister corev1listers.PodLister
	PodSynced cache.InformerSynced

	serviceLister corev1listers.ServiceLister
	ServiceSynced cache.InformerSynced

	podDisruptionBudgetLister  policyv1listers.PodDisruptionBudgetLister
	PodDiscruptionBudgetSynced cache.InformerSynced

	podControl                 pod.RedisClusterControlInteface
	serviceControl             ServicesControlInterface
	podDisruptionBudgetControl PodDisruptionBudgetsControlInterface

	updateHandler func(*rapi.RedisCluster) (*rapi.RedisCluster, error) // callback to update RedisCluster. Added as member for testing

	queue workqueue.RateLimitingInterface // RedisClusters to be synced

	recorder record.EventRecorder

	config *Config
}

// NewController builds and return new controller instance
func NewController(cfg *Config, kubeClient clientset.Interface, redisClient rclient.Interface, kubeInformer kubeinformers.SharedInformerFactory, rInformer rinformers.SharedInformerFactory) *Controller {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	serviceInformer := kubeInformer.Core().V1().Services()
	podInformer := kubeInformer.Core().V1().Pods()
	redisInformer := rInformer.Redisoperator().V1().RedisClusters()
	podDisruptionBudgetInformer := kubeInformer.Policy().V1beta1().PodDisruptionBudgets()

	controller := &Controller{
		kubeClient:                 kubeClient,
		redisClient:                redisClient,
		redisClusterLister:         redisInformer.Lister(),
		RedisClusterSynced:         redisInformer.Informer().HasSynced,
		podLister:                  podInformer.Lister(),
		PodSynced:                  podInformer.Informer().HasSynced,
		serviceLister:              serviceInformer.Lister(),
		ServiceSynced:              serviceInformer.Informer().HasSynced,
		podDisruptionBudgetLister:  podDisruptionBudgetInformer.Lister(),
		PodDiscruptionBudgetSynced: podDisruptionBudgetInformer.Informer().HasSynced,

		queue:    workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "rediscluster"),
		recorder: eventBroadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "rediscluster-controller"}),

		config: cfg,
	}

	redisInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    controller.onAddRedisCluster,
			UpdateFunc: controller.onUpdateRedisCluster,
			DeleteFunc: controller.onDeleteRedisCluster,
		},
	)

	podInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    controller.onAddPod,
			UpdateFunc: controller.onUpdatePod,
			DeleteFunc: controller.onDeletePod,
		},
	)

	controller.updateHandler = controller.updateRedisCluster
	controller.podControl = pod.NewRedisClusterControl(controller.podLister, controller.kubeClient, controller.recorder)
	controller.serviceControl = NewServicesControl(controller.kubeClient, controller.recorder)
	controller.podDisruptionBudgetControl = NewPodDisruptionBudgetsControl(controller.kubeClient, controller.recorder)
	return controller
}

// Run executes the Controller
func (c *Controller) Run(stop <-chan struct{}) error {
	glog.Infof("Starting RedisCluster controller")

	if !cache.WaitForCacheSync(stop, c.PodSynced, c.RedisClusterSynced, c.ServiceSynced) {
		return fmt.Errorf("Timed out waiting for caches to sync")
	}

	for i := 0; i < c.config.NbWorker; i++ {
		go wait.Until(c.runWorker, time.Second, stop)
	}

	<-stop
	return nil
}

func (c *Controller) runWorker() {
	ctx := context.Background()
	for c.processNextItem(ctx) {
	}
}

func (c *Controller) processNextItem(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	needRequeue, err := c.sync(ctx, key.(string))
	if err == nil {
		c.queue.Forget(key)
	} else {
		utilruntime.HandleError(fmt.Errorf("Error syncing rediscluster: %v", err))
		c.queue.AddRateLimited(key)
		return true
	}

	if needRequeue {
		glog.V(4).Info("processNextItem: Requeue key:", key)
		c.queue.AddRateLimited(key)
	}

	return true
}

func (c *Controller) sync(ctx context.Context, key string) (bool, error) {
	var forceRequeue bool
	var err error
	glog.V(2).Infof("sync() key:%s", key)
	startTime := metav1.Now()
	defer func() {
		if err != nil {
			metrics.ReconcileErrors.WithLabelValues(metrics.RedisOperatorController).Inc()
			metrics.ReconcileTotal.WithLabelValues(metrics.RedisOperatorController, metrics.LabelError).Inc()
		} else if forceRequeue {
			metrics.ReconcileTotal.WithLabelValues(metrics.RedisOperatorController, metrics.LabelRequeue).Inc()
		} else {
			metrics.ReconcileTotal.WithLabelValues(metrics.RedisOperatorController, metrics.LabelSuccess).Inc()
		}
		reconcileTime := time.Since(startTime.Time)
		metrics.ReconcileTime.WithLabelValues(metrics.RedisOperatorController).Observe(reconcileTime.Seconds())

		glog.V(2).Infof("Finished syncing RedisCluster %q (%v)", key, reconcileTime)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return false, err
	}
	glog.V(6).Infof("Syncing %s/%s", namespace, name)
	sharedRedisCluster, err := c.redisClusterLister.RedisClusters(namespace).Get(name)
	if err != nil {
		glog.Errorf("unable to get RedisCluster %s/%s: %v. Maybe deleted", namespace, name, err)
		return false, nil
	}

	if !rapi.IsRedisClusterDefaulted(sharedRedisCluster) {
		defaultedRedisCluster := rapi.DefaultRedisCluster(sharedRedisCluster)
		if _, err = c.updateHandler(defaultedRedisCluster); err != nil {
			glog.Errorf("RedisCluster %s/%s updated error:, err", namespace, name)
			return false, fmt.Errorf("unable to default RedisCluster %s/%s: %v", namespace, name, err)
		}
		glog.V(6).Infof("RedisCluster-Operator.sync Defaulted %s/%s", namespace, name)
		return false, nil
	}

	// TODO add validation

	// TODO: add test the case of graceful deletion
	if sharedRedisCluster.DeletionTimestamp != nil {
		return false, nil
	}

	rediscluster := sharedRedisCluster.DeepCopy()

	// Init status.StartTime
	if rediscluster.Status.StartTime == nil {
		rediscluster.Status.StartTime = &startTime
		if _, err := c.updateHandler(rediscluster); err != nil {
			glog.Errorf("RedisCluster %s/%s: unable init startTime: %v", namespace, name, err)
			return false, nil
		}
		glog.V(4).Infof("RedisCluster %s/%s: startTime updated", namespace, name)
		return false, nil
	}

	forceRequeue, err = c.syncCluster(ctx, rediscluster)
	return forceRequeue, err
}

func (c *Controller) getRedisClusterService(redisCluster *rapi.RedisCluster) (*apiv1.Service, error) {
	serviceName := getServiceName(redisCluster)
	labels, err := pod.GetLabelsSet(redisCluster)
	if err != nil {
		return nil, fmt.Errorf("couldn't get cluster label, err: %v ", err)
	}

	svcList, err := c.serviceLister.Services(redisCluster.Namespace).List(labels.AsSelector())
	if err != nil {
		return nil, fmt.Errorf("couldn't list service with label:%s, err:%v ", labels.String(), err)
	}
	var svc *apiv1.Service
	for i, s := range svcList {
		if s.Name == serviceName {
			svc = svcList[i]
		}
	}
	return svc, nil
}

func (c *Controller) getRedisClusterPodDisruptionBudget(redisCluster *rapi.RedisCluster) (*policyv1.PodDisruptionBudget, error) {
	podDisruptionBudgetName := redisCluster.Name
	labels, err := pod.GetLabelsSet(redisCluster)
	if err != nil {
		return nil, fmt.Errorf("couldn't get cluster label, err: %v ", err)
	}

	pdbList, err := c.podDisruptionBudgetLister.PodDisruptionBudgets(redisCluster.Namespace).List(labels.AsSelector())
	if err != nil {
		return nil, fmt.Errorf("couldn't list PodDisruptionBudget with label:%s, err:%v ", labels.String(), err)
	}
	var pdb *policyv1.PodDisruptionBudget
	for i, p := range pdbList {
		if p.Name == podDisruptionBudgetName {
			pdb = pdbList[i]
		}
	}
	return pdb, nil
}

func (c *Controller) syncCluster(ctx context.Context, rediscluster *rapi.RedisCluster) (forceRequeue bool, err error) {
	glog.V(6).Info("syncCluster START")
	defer glog.V(6).Info("syncCluster STOP")
	forceRequeue = false
	redisClusterService, err := c.getRedisClusterService(rediscluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.sync unable to retrieves service associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
		return forceRequeue, err
	}
	if redisClusterService == nil {
		if _, err = c.serviceControl.CreateRedisClusterService(rediscluster); err != nil {
			glog.Errorf("RedisCluster-Operator.sync unable to create service associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
			return forceRequeue, err
		}
	}

	redisClusterPodDisruptionBudget, err := c.getRedisClusterPodDisruptionBudget(rediscluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.sync unable to retrieves podDisruptionBudget associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
		return forceRequeue, err
	}
	if redisClusterPodDisruptionBudget == nil {
		if _, err = c.podDisruptionBudgetControl.CreateRedisClusterPodDisruptionBudget(rediscluster); err != nil {
			glog.Errorf("RedisCluster-Operator.sync unable to create podDisruptionBudget associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
			return forceRequeue, err
		}
	}
	redisClusterPods, err := c.podControl.GetRedisClusterPods(rediscluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.sync unable to retrieves pod associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
		return forceRequeue, err
	}

	pods, lostPods := filterLostNodes(redisClusterPods)
	if len(lostPods) != 0 {
		for _, p := range lostPods {
			err := c.podControl.DeletePodNow(rediscluster, p.Name)
			glog.Errorf("Lost node with pod %s. Deleting... %v", p.Name, err)
		}
		redisClusterPods = pods
	}

	// RedisAdmin is used access the Redis process in the different pods.
	admin, err := NewRedisAdmin(ctx, redisClusterPods, &c.config.redis)
	if err != nil {
		return forceRequeue, fmt.Errorf("unable to create the redis.Admin, err:%v", err)
	}
	defer admin.Close()

	clusterInfos, errGetInfos := admin.GetClusterInfos(ctx)
	if errGetInfos != nil {
		glog.Errorf("Error when getting cluster infos to rebuild bom : %v", errGetInfos)
		if clusterInfos.Status == redis.ClusterInfoPartial {
			return false, fmt.Errorf("partial cluster info")
		}
	}

	// From the Redis cluster nodes connections, build the cluster status
	clusterStatus, err := c.buildClusterStatus(clusterInfos, redisClusterPods, rediscluster)
	if err != nil {
		glog.Errorf("unable to build the RedisClusterStatus, err:%v", err)
		return forceRequeue, fmt.Errorf("unable to build clusterStatus, err:%v", err)
	}

	updated, err := c.updateClusterStatus(rediscluster, clusterStatus)
	rediscluster.Status.Cluster.Nodes = clusterStatus.Nodes
	if err != nil {
		return forceRequeue, err
	}
	if updated {
		// If the cluster status changes requeue the key. We want to apply the RedisCluster operation on a
		// stable cluster already stored in the API server.
		glog.V(3).Infof("cluster updated %s-%s", rediscluster.Namespace, rediscluster.Name)
		return true, nil
	}

	allPodsReady := true
	if (clusterStatus.NumberOfPods - clusterStatus.NumberOfRedisNodesRunning) != 0 {
		glog.V(3).Infof("Not all redis nodes are running, numberOfPods: %d, numberOfRedisNodesRunning: %d", clusterStatus.NumberOfPods, clusterStatus.NumberOfRedisNodesRunning)
		allPodsReady = false
	}

	// Now check if the Operator need to execute some operation the redis cluster. if yes run the clusterAction(...) method.
	needSanitize, err := c.checkSanityCheck(ctx, rediscluster, admin, clusterInfos)
	if err != nil {
		glog.Errorf("checkSanityCheck, error happened in dryrun mode, err:%v", err)
		return false, err
	}

	if allPodsReady && needClusterOperation(rediscluster) || needSanitize {
		forceRequeue = false
		requeue, err := c.clusterAction(ctx, admin, rediscluster, clusterInfos)
		if err != nil {
			glog.Errorf("error during action on cluster: %s-%s, err: %v", rediscluster.Namespace, rediscluster.Name, err)
		}
		forceRequeue = requeue
		_, err = c.updateRedisCluster(rediscluster)
		return forceRequeue, err
	}

	if setRebalancingCondition(&rediscluster.Status, false) ||
		setRollingUpdateCondition(&rediscluster.Status, false) ||
		setScalingCondition(&rediscluster.Status, false) ||
		setClusterStatusCondition(&rediscluster.Status, true) {
		_, err = c.updateHandler(rediscluster)
		return forceRequeue, err
	}

	return forceRequeue, nil
}

func (c *Controller) checkSanityCheck(ctx context.Context, cluster *rapi.RedisCluster, admin redis.AdminInterface, infos *redis.ClusterInfos) (bool, error) {
	return sanitycheck.RunSanityChecks(ctx, admin, &c.config.redis, c.podControl, cluster, infos, true)
}

func (c *Controller) updateClusterStatus(cluster *rapi.RedisCluster, newStatus *rapi.RedisClusterState) (bool, error) {
	if compareStatus(&cluster.Status.Cluster, newStatus) {
		glog.V(3).Infof("Status changed for cluster: %s-%s", cluster.Namespace, cluster.Name)
		// the status have been update, needs to update the RedisCluster
		cluster.Status.Cluster = *newStatus
		_, err := c.updateRedisCluster(cluster)
		return true, err
	}
	return false, nil
}

func (c *Controller) buildClusterStatus(clusterInfos *redis.ClusterInfos, pods []*apiv1.Pod, cluster *rapi.RedisCluster) (*rapi.RedisClusterState, error) {
	clusterStatus := getRedisClusterStatus(clusterInfos, pods)

	podLabels, err := pod.GetLabelsSet(cluster)
	if err != nil {
		glog.Errorf("Unable to get labelset. err: %v", err)
	}
	clusterStatus.LabelSelectorPath = podLabels.String()

	min, max := getReplicationFactors(clusterStatus.NumberOfSlavesPerMaster)
	clusterStatus.MinReplicationFactor = int32(min)
	clusterStatus.MaxReplicationFactor = int32(max)

	glog.V(3).Infof("Build Bom, current node list: %s ", clusterStatus.String())

	return clusterStatus, nil
}

func getReplicationFactors(numberOfSlavesPerMaster map[string]int) (int, int) {
	minReplicationFactor := math.MaxInt32
	maxReplicationFactor := 0
	for _, i := range numberOfSlavesPerMaster {
		if i > maxReplicationFactor {
			maxReplicationFactor = i
		}
		if i < minReplicationFactor {
			minReplicationFactor = i
		}
	}
	if len(numberOfSlavesPerMaster) == 0 {
		minReplicationFactor = 0
	}
	return minReplicationFactor, maxReplicationFactor
}

func getRedisClusterStatus(clusterInfos *redis.ClusterInfos, pods []*apiv1.Pod) *rapi.RedisClusterState {
	clusterStatus := &rapi.RedisClusterState{}
	clusterStatus.NumberOfPodsReady = 0
	clusterStatus.NumberOfRedisNodesRunning = 0
	clusterStatus.MaxReplicationFactor = 0
	clusterStatus.MinReplicationFactor = 0
	clusterStatus.NumberOfPods = int32(len(pods))
	clusterStatus.NumberOfSlavesPerMaster = map[string]int{}

	numberOfPodsReady := int32(0)
	numberOfRedisNodesRunning := int32(0)
	numberOfMasters := int32(0)
	numberOfMastersReady := int32(0)
	numberOfSlavesPerMaster := map[string]int{}

	for _, p := range pods {
		podReady := false
		if podReady, _ = IsPodReady(p); !podReady {
			continue
		}
		numberOfPodsReady++
		// find corresponding Redis node
		redisNodes, err := clusterInfos.GetNodes().GetNodesByFunc(func(node *redis.Node) bool {
			return node.IP == p.Status.PodIP
		})
		if err != nil {
			glog.Errorf("unable to retrieve the associated Redis Node with the pod: %s, ip:%s, err:%v", p.Name, p.Status.PodIP, err)
			continue
		}
		newNode := rapi.RedisClusterNode{
			PodName: p.Name,
			IP:      p.Status.PodIP,
			Pod:     p,
			Slots:   []string{},
		}
		// only one redis node with a role per pod
		if len(redisNodes) == 1 {
			redisNode := redisNodes[0]
			if redis.IsMasterWithSlot(redisNode) {
				if _, ok := numberOfSlavesPerMaster[redisNode.ID]; !ok {
					numberOfSlavesPerMaster[redisNode.ID] = 0
				}
				numberOfMasters++
				if podReady {
					numberOfMastersReady++
				}
			}

			newNode.ID = redisNode.ID
			newNode.Role = redisNode.GetRole()
			newNode.Port = redisNode.Port
			if redis.IsSlave(redisNode) && redisNode.MasterReferent != "" {
				numberOfSlavesPerMaster[redisNode.MasterReferent] = numberOfSlavesPerMaster[redisNode.MasterReferent] + 1
				newNode.MasterRef = redisNode.MasterReferent
			}
			if len(redisNode.Slots) > 0 {
				slots := redis.SlotRangesFromSlots(redisNode.Slots)
				for _, slot := range slots {
					newNode.Slots = append(newNode.Slots, slot.String())
				}
			}
			numberOfRedisNodesRunning++
		}
		clusterStatus.Nodes = append(clusterStatus.Nodes, newNode)
	}

	clusterStatus.NumberOfRedisNodesRunning = numberOfRedisNodesRunning
	clusterStatus.NumberOfMasters = numberOfMasters
	clusterStatus.NumberOfMastersReady = numberOfMastersReady
	clusterStatus.NumberOfPodsReady = numberOfPodsReady
	clusterStatus.NumberOfSlavesPerMaster = numberOfSlavesPerMaster
	clusterStatus.Status = rapi.ClusterStatusOK

	return clusterStatus
}

// enqueue adds key in the controller queue
func (c *Controller) enqueue(rediscluster *rapi.RedisCluster) {
	key, err := cache.MetaNamespaceKeyFunc(rediscluster)
	if err != nil {
		glog.Errorf("RedisCluster-Controller:enqueue: couldn't get key for RedisCluster %s/%s: %v", rediscluster.Namespace, rediscluster.Name, err)
		return
	}
	c.queue.Add(key)
}

func (c *Controller) updateRedisCluster(rediscluster *rapi.RedisCluster) (*rapi.RedisCluster, error) {
	rc, err := c.redisClient.RedisoperatorV1().RedisClusters(rediscluster.Namespace).Update(rediscluster)
	if err != nil {
		glog.Errorf("updateRedisCluster cluster: [%v] error: %v", *rediscluster, err)
		return rc, err
	}

	glog.V(6).Infof("RedisCluster %s/%s updated", rediscluster.Namespace, rediscluster.Name)
	return rc, nil
}

func (c *Controller) onAddRedisCluster(obj interface{}) {
	rediscluster, ok := obj.(*rapi.RedisCluster)
	if !ok {
		glog.Errorf("adding RedisCluster, expected RedisCluster object. Got: %+v", obj)
		return
	}
	glog.V(6).Infof("onAddRedisCluster %s/%s", rediscluster.Namespace, rediscluster.Name)
	if !reflect.DeepEqual(rediscluster.Status, rapi.RedisClusterStatus{}) {
		glog.Errorf("rediscluster %s/%s created with non empty status. Going to be removed", rediscluster.Namespace, rediscluster.Name)

		if _, err := cache.MetaNamespaceKeyFunc(rediscluster); err != nil {
			glog.Errorf("couldn't get key for RedisCluster (to be deleted) %s/%s: %v", rediscluster.Namespace, rediscluster.Name, err)
			return
		}
		// TODO: how to remove a rediscluster created with an invalid or even with a valid status. What in case of error for this delete?
		if err := c.deleteRedisCluster(rediscluster.Namespace, rediscluster.Name); err != nil {
			glog.Errorf("unable to delete non empty status RedisCluster %s/%s: %v. No retry will be performed.", rediscluster.Namespace, rediscluster.Name, err)
		}

		return
	}

	c.enqueue(rediscluster)
}

func (c *Controller) onDeleteRedisCluster(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		glog.Errorf("Unable to get key for %#v: %v", obj, err)
		return
	}
	rediscluster, ok := obj.(*rapi.RedisCluster)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			glog.Errorf("unknown object from RedisCluster delete event: %#v", obj)
			return
		}
		rediscluster, ok = tombstone.Obj.(*rapi.RedisCluster)
		if !ok {
			glog.Errorf("Tombstone contained object that is not an RedisCluster: %#v", obj)
			return
		}
	}
	glog.V(6).Infof("onDeleteRedisCluster %s/%s", rediscluster.Namespace, rediscluster.Name)

	c.queue.Add(key)
}

func (c *Controller) onUpdateRedisCluster(oldObj, newObj interface{}) {
	rediscluster, ok := newObj.(*rapi.RedisCluster)
	if !ok {
		glog.Errorf("Expected RedisCluster object. Got: %+v", newObj)
		return
	}
	glog.V(6).Infof("onUpdateRedisCluster %s/%s", rediscluster.Namespace, rediscluster.Name)
	c.enqueue(rediscluster)
}

func (c *Controller) onAddPod(obj interface{}) {
	pod, ok := obj.(*apiv1.Pod)
	if !ok {
		glog.Errorf("adding Pod, expected Pod object. Got: %+v", obj)
		return
	}
	if _, ok := pod.GetObjectMeta().GetLabels()[rapi.ClusterNameLabelKey]; !ok {
		return
	}
	redisCluster, err := c.getRedisClusterFromPod(pod)
	if err != nil {
		glog.Errorf("unable to retrieve the associated rediscluster for pod %s/%s:%v", pod.Namespace, pod.Name, err)
		return
	}
	if redisCluster == nil {
		glog.Errorf("empty redisCluster. Unable to retrieve the associated rediscluster for the pod  %s/%s", pod.Namespace, pod.Name)
		return
	}

	c.enqueue(redisCluster)
}

func (c *Controller) onDeletePod(obj interface{}) {
	pod, ok := obj.(*apiv1.Pod)
	if _, ok := pod.GetObjectMeta().GetLabels()[rapi.ClusterNameLabelKey]; !ok {
		return
	}
	glog.V(6).Infof("onDeletePod old=%v", pod.Name)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			glog.Errorf("Couldn't get object from tombstone %+v", obj)
			return
		}
		pod, ok = tombstone.Obj.(*apiv1.Pod)
		if !ok {
			glog.Errorf("Tombstone contained object that is not a pod %+v", obj)
			return
		}
	}

	redisCluster, err := c.getRedisClusterFromPod(pod)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.onDeletePod: %v", err)
		return
	}
	if redisCluster == nil {
		glog.Errorf("empty redisCluster . RedisCluster-Operator.onDeletePod")
		return
	}

	c.enqueue(redisCluster)
}

func (c *Controller) onUpdatePod(oldObj, newObj interface{}) {
	oldPod := oldObj.(*apiv1.Pod)
	newPod := newObj.(*apiv1.Pod)
	if oldPod.ResourceVersion == newPod.ResourceVersion { // Since periodic resync will send update events for all known Pods.
		return
	}
	if _, ok := newPod.GetObjectMeta().GetLabels()[rapi.ClusterNameLabelKey]; !ok {
		return
	}
	glog.V(6).Infof("onUpdatePod old=%v, cur=%v ", oldPod.Name, newPod.Name)
	redisCluster, err := c.getRedisClusterFromPod(newPod)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.onUpdateJob cannot get redisclusters for Pod %s/%s: %v", newPod.Namespace, newPod.Name, err)
		return
	}
	if redisCluster == nil {
		glog.Errorf("empty redisCluster .onUpdateJob cannot get redisclusters for Pod %s/%s", newPod.Namespace, newPod.Name)
		return
	}

	c.enqueue(redisCluster)

	// TODO: in case of relabelling ?
	// TODO: in case of labelSelector relabelling?
}

func (c *Controller) deleteRedisCluster(namespace, name string) error {
	return nil
}

func (c *Controller) getRedisClusterFromPod(pod *apiv1.Pod) (*rapi.RedisCluster, error) {
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no rediscluster found for pod. Pod %s/%s has no labels", pod.Namespace, pod.Name)
	}

	clusterName, ok := pod.Labels[rapi.ClusterNameLabelKey]
	if !ok {
		return nil, fmt.Errorf("no rediscluster name found for pod. Pod %s/%s has no labels %s", pod.Namespace, pod.Name, rapi.ClusterNameLabelKey)
	}
	return c.redisClusterLister.RedisClusters(pod.Namespace).Get(clusterName)
}
