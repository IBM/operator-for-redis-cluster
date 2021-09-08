package controller

import (
	"context"
	"fmt"
	"math"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	policyv1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	rapi "github.com/TheWeatherCompany/icm-redis-operator/api/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/pod"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/controller/sanitycheck"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

// Controller contains all controller fields
type Controller struct {
	mgr    manager.Manager
	client kclient.Client

	podControl                 pod.RedisClusterControlInterface
	serviceControl             ServicesControlInterface
	podDisruptionBudgetControl PodDisruptionBudgetsControlInterface

	updateHandler func(*rapi.RedisCluster) (*rapi.RedisCluster, error) // callback to update RedisCluster. Added as member for testing

	recorder record.EventRecorder

	config *Config
}

// NewController builds and return new controller instance
func NewController(cfg *Config, mgr manager.Manager, kubeClient kclient.Client, recorder record.EventRecorder) *Controller {
	controller := &Controller{
		mgr:                        mgr,
		client:                     kubeClient,
		recorder:                   recorder,
		config:                     cfg,
		podControl:                 pod.NewRedisClusterControl(kubeClient, recorder),
		serviceControl:             NewServicesControl(kubeClient, recorder),
		podDisruptionBudgetControl: NewPodDisruptionBudgetsControl(kubeClient, recorder),
	}

	controller.updateHandler = controller.updateRedisCluster
	return controller
}

func SetupRedisClusterController(mgr ctrl.Manager, redisClusterController *Controller) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("rediscluster").
		For(&rapi.RedisCluster{}).
		Owns(&v1.Pod{}).
		Owns(&v1.Service{}).
		Owns(&policy.PodDisruptionBudget{}).
		//WithEventFilter(predicate.NewRedisClusterPredicate()). //uncomment to see kubernetes events in the logs, e.g. ConfigMap updates
		Complete(redisClusterController)
}

func (c *Controller) Reconcile(ctx context.Context, namespacedName ctrl.Request) (ctrl.Result, error) {
	var err error
	glog.V(2).Infof("Reconcile() key:%s", namespacedName)
	startTime := metav1.Now()
	defer func() {
		reconcileTime := time.Since(startTime.Time)
		glog.V(2).Infof("Finished reconciling RedisCluster %q (%v)", namespacedName, reconcileTime)
	}()

	glog.V(6).Infof("Reconciling %s", namespacedName)
	sharedRedisCluster := &rapi.RedisCluster{}
	err = c.client.Get(ctx, namespacedName.NamespacedName, sharedRedisCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			glog.Infof("RedisCluster %s not found. Maybe deleted.", namespacedName)
			return ctrl.Result{}, nil
		}
		glog.Errorf("unable to get RedisCluster %s: %v", namespacedName, err)
		return ctrl.Result{}, err
	}

	if !rapi.IsRedisClusterDefaulted(sharedRedisCluster) {
		defaultedRedisCluster := rapi.DefaultRedisCluster(sharedRedisCluster)
		if _, err = c.updateHandler(defaultedRedisCluster); err != nil {
			glog.Errorf("RedisCluster %s update error:, err", namespacedName)
			return ctrl.Result{}, fmt.Errorf("unable to default RedisCluster %s: %v", namespacedName, err)
		}
		glog.V(6).Infof("RedisCluster-Operator.Reconcile Defaulted %s", namespacedName)
		return ctrl.Result{}, nil
	}

	// TODO add validation

	// TODO: add test the case of graceful deletion
	if sharedRedisCluster.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	rediscluster := sharedRedisCluster.DeepCopy()

	// Init status.StartTime
	if rediscluster.Status.StartTime == nil {
		rediscluster.Status.StartTime = &startTime
		if _, err := c.updateHandler(rediscluster); err != nil {
			glog.Errorf("RedisCluster %s: unable init startTime: %v", namespacedName, err)
			return ctrl.Result{}, err
		}
		glog.V(4).Infof("RedisCluster %s: startTime updated", namespacedName)
		return ctrl.Result{}, nil
	}

	result, err := c.syncCluster(ctx, rediscluster)
	return result, err
}

func (c *Controller) getRedisCluster(ctx context.Context, namespace, name string) (*rapi.RedisCluster, error) {
	newCluster := &rapi.RedisCluster{}
	namespacedName := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	err := c.client.Get(ctx, namespacedName, newCluster)
	if err != nil {
		return nil, err
	}
	return newCluster, err
}

func (c *Controller) getRedisClusterService(cluster *rapi.RedisCluster) (*v1.Service, error) {
	serviceName := getServiceName(cluster)
	labels, err := pod.GetLabelsSet(cluster)
	if err != nil {
		return nil, fmt.Errorf("couldn't get cluster label, err: %v ", err)
	}

	svcList := &v1.ServiceList{}
	err = c.client.List(context.Background(), svcList, kclient.InNamespace(cluster.Namespace), kclient.MatchingLabelsSelector{Selector: labels.AsSelector()})
	if err != nil {
		return nil, fmt.Errorf("couldn't list service with label:%s, err:%v ", labels.String(), err)
	}
	var svc *v1.Service
	for i, s := range svcList.Items {
		if s.Name == serviceName {
			svc = &svcList.Items[i]
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

	pdbList := &policyv1.PodDisruptionBudgetList{}
	err = c.client.List(context.Background(), pdbList, kclient.InNamespace(redisCluster.Namespace), kclient.MatchingLabelsSelector{Selector: labels.AsSelector()})
	if err != nil {
		return nil, fmt.Errorf("couldn't list PodDisruptionBudget with label:%s, err:%v ", labels.String(), err)
	}
	var pdb *policyv1.PodDisruptionBudget
	for i, p := range pdbList.Items {
		if p.Name == podDisruptionBudgetName {
			pdb = &pdbList.Items[i]
		}
	}
	return pdb, nil
}

func (c *Controller) syncCluster(ctx context.Context, rediscluster *rapi.RedisCluster) (ctrl.Result, error) {
	glog.V(6).Info("syncCluster START")
	defer glog.V(6).Info("syncCluster STOP")
	result := ctrl.Result{}
	redisClusterService, err := c.getRedisClusterService(rediscluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.Reconcile unable to retrieves service associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
		return result, err
	}
	if redisClusterService == nil {
		if _, err = c.serviceControl.CreateRedisClusterService(rediscluster); err != nil {
			glog.Errorf("RedisCluster-Operator.Reconcile unable to create service associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
			return result, err
		}
	}

	redisClusterPodDisruptionBudget, err := c.getRedisClusterPodDisruptionBudget(rediscluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.Reconcile unable to retrieves podDisruptionBudget associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
		return result, err
	}
	if redisClusterPodDisruptionBudget == nil {
		if _, err = c.podDisruptionBudgetControl.CreateRedisClusterPodDisruptionBudget(rediscluster); err != nil {
			glog.Errorf("RedisCluster-Operator.Reconcile unable to create podDisruptionBudget associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
			return result, err
		}
	}
	redisPods, err := c.podControl.GetRedisClusterPods(rediscluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.Reconcile unable to retrieves pod associated to the RedisCluster: %s/%s", rediscluster.Namespace, rediscluster.Name)
		return result, err
	}

	pods, lostPods := filterLostNodes(redisPods)
	if len(lostPods) != 0 {
		for _, p := range lostPods {
			err = c.podControl.DeletePodNow(rediscluster, p.Name)
			glog.Errorf("Lost node with pod %s. Deleting... %v", p.Name, err)
		}
		redisPods = pods
	}

	// RedisAdmin is used access the Redis process in the different pods.
	admin, err := NewRedisAdmin(ctx, redisPods, &c.config.redis)
	if err != nil {
		return result, fmt.Errorf("unable to create the redis.Admin, err:%v", err)
	}
	defer admin.Close()

	clusterInfos, errGetInfos := admin.GetClusterInfos(ctx)
	if errGetInfos != nil {
		glog.Errorf("Error when getting cluster infos to rebuild bom : %v", errGetInfos)
		if clusterInfos.Status == redis.ClusterInfoPartial {
			return result, fmt.Errorf("partial cluster info")
		}
	}

	// From the Redis cluster nodes connections, build the cluster status
	clusterStatus, err := c.buildClusterStatus(rediscluster, clusterInfos, redisPods)
	if err != nil {
		glog.Errorf("unable to build the RedisClusterStatus, err:%v", err)
		return result, fmt.Errorf("unable to build clusterStatus, err:%v", err)
	}

	updated, err := c.updateClusterStatus(ctx, rediscluster.Namespace, rediscluster.Name, clusterStatus)
	rediscluster.Status.Cluster.Nodes = clusterStatus.Nodes
	if err != nil {
		return result, err
	}
	if updated {
		// If the cluster status changes requeue the key. We want to apply the RedisCluster operation on a
		// stable cluster already stored in the API server.
		glog.V(3).Infof("cluster updated %s-%s", rediscluster.Namespace, rediscluster.Name)
		return ctrl.Result{Requeue: true}, nil
	}

	allPodsReady := true
	if (clusterStatus.NumberOfPods - clusterStatus.NumberOfRedisNodesRunning) != 0 {
		glog.V(3).Infof("Not all redis nodes are running, numberOfPods: %d, numberOfRedisNodesRunning: %d", clusterStatus.NumberOfPods, clusterStatus.NumberOfRedisNodesRunning)
		allPodsReady = false
	}

	// Now check if the operator needs to execute some operation on the redis cluster
	needSanitize, err := c.checkSanityCheck(ctx, rediscluster, admin, clusterInfos)
	if err != nil {
		glog.Errorf("checkSanityCheck, error happened in dryrun mode, err:%v", err)
		return result, err
	}

	if allPodsReady && needClusterOperation(rediscluster) || needSanitize {
		actionResult, err := c.clusterAction(ctx, admin, rediscluster, clusterInfos)
		if err != nil {
			glog.Errorf("error during action on cluster: %s-%s, err: %v", rediscluster.Namespace, rediscluster.Name, err)
		}
		_, err = c.updateHandler(rediscluster)
		return actionResult, err
	}

	if setRebalancingCondition(&rediscluster.Status, false) ||
		setRollingUpdateCondition(&rediscluster.Status, false) ||
		setScalingCondition(&rediscluster.Status, false) ||
		setClusterStatusCondition(&rediscluster.Status, true) {
		_, err = c.updateHandler(rediscluster)
		return result, err
	}

	return result, nil
}

func (c *Controller) checkSanityCheck(ctx context.Context, cluster *rapi.RedisCluster, admin redis.AdminInterface, infos *redis.ClusterInfos) (bool, error) {
	return sanitycheck.RunSanityChecks(ctx, admin, &c.config.redis, c.podControl, cluster, infos, true)
}

func (c *Controller) updateClusterStatus(ctx context.Context, namespace, name string, newStatus *rapi.RedisClusterState) (bool, error) {
	cluster, err := c.getRedisCluster(ctx, namespace, name)
	if err != nil {
		return false, err
	}
	if compareStatus(&cluster.Status.Cluster, newStatus) {
		glog.V(3).Infof("Status changed for cluster: %s-%s", cluster.Namespace, cluster.Name)
		// the status have been update, needs to update the RedisCluster
		cluster.Status.Cluster = *newStatus
		_, err := c.updateHandler(cluster)
		return true, err
	}
	return false, nil
}

func (c *Controller) buildClusterStatus(cluster *rapi.RedisCluster, clusterInfos *redis.ClusterInfos, pods []v1.Pod) (*rapi.RedisClusterState, error) {
	clusterStatus := getRedisClusterStatus(clusterInfos, pods)
	podLabels, err := pod.GetLabelsSet(cluster)
	if err != nil {
		glog.Errorf("Unable to get labelset. err: %v", err)
	}
	clusterStatus.LabelSelectorPath = podLabels.String()
	min, max := getReplicationFactors(clusterStatus.NumberOfReplicasPerPrimary)
	clusterStatus.MinReplicationFactor = int32(min)
	clusterStatus.MaxReplicationFactor = int32(max)
	clusterStatus.Status = cluster.Status.Cluster.Status
	if clusterStatus.NumberOfRedisNodesRunning == clusterStatus.NumberOfPrimariesReady*(1+clusterStatus.MinReplicationFactor) {
		clusterStatus.Status = rapi.ClusterStatusOK
	}
	glog.V(3).Infof("Build Bom, current node list: %s ", clusterStatus.String())
	return clusterStatus, nil
}

func getReplicationFactors(numberOfReplicasPerPrimary map[string]int) (int, int) {
	minReplicationFactor := math.MaxInt32
	maxReplicationFactor := 0
	for _, i := range numberOfReplicasPerPrimary {
		if i > maxReplicationFactor {
			maxReplicationFactor = i
		}
		if i < minReplicationFactor {
			minReplicationFactor = i
		}
	}
	if len(numberOfReplicasPerPrimary) == 0 {
		minReplicationFactor = 0
	}
	return minReplicationFactor, maxReplicationFactor
}

func getRedisClusterStatus(clusterInfos *redis.ClusterInfos, pods []v1.Pod) *rapi.RedisClusterState {
	clusterStatus := &rapi.RedisClusterState{}
	clusterStatus.NumberOfPodsReady = 0
	clusterStatus.NumberOfRedisNodesRunning = 0
	clusterStatus.NumberOfPods = int32(len(pods))
	clusterStatus.NumberOfReplicasPerPrimary = map[string]int{}

	numberOfPodsReady := int32(0)
	numberOfRedisNodesRunning := int32(0)
	numberOfPrimaries := int32(0)
	numberOfPrimariesReady := int32(0)
	numberOfReplicasPerPrimary := map[string]int{}

	for i, p := range pods {
		podReady := false
		if podReady, _ = IsPodReady(&p); !podReady {
			continue
		}
		numberOfPodsReady++
		// find corresponding Redis node
		redisNodes, err := clusterInfos.GetNodes().GetNodesByFunc(func(node *redis.Node) bool {
			return node.IP == p.Status.PodIP
		})
		if err != nil {
			glog.Warningf("unable to retrieve the redis node associated with pod: %s, ip:%s, err:%v", p.Name, p.Status.PodIP, err)
			continue
		}
		newNode := rapi.RedisClusterNode{
			PodName: p.Name,
			IP:      p.Status.PodIP,
			Pod:     &pods[i],
			Slots:   []string{},
		}
		// only one redis node with a role per pod
		if len(redisNodes) == 1 {
			redisNode := redisNodes[0]
			if redis.IsPrimaryWithSlot(redisNode) {
				if _, ok := numberOfReplicasPerPrimary[redisNode.ID]; !ok {
					numberOfReplicasPerPrimary[redisNode.ID] = 0
				}
				numberOfPrimaries++
				if podReady {
					numberOfPrimariesReady++
				}
			}

			newNode.ID = redisNode.ID
			newNode.Role = redisNode.GetRole()
			newNode.Port = redisNode.Port

			if redis.IsReplica(redisNode) && redisNode.PrimaryReferent != "" {
				numberOfReplicasPerPrimary[redisNode.PrimaryReferent] = numberOfReplicasPerPrimary[redisNode.PrimaryReferent] + 1
				newNode.PrimaryRef = redisNode.PrimaryReferent
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
	clusterStatus.NumberOfPrimaries = numberOfPrimaries
	clusterStatus.NumberOfPrimariesReady = numberOfPrimariesReady
	clusterStatus.NumberOfPodsReady = numberOfPodsReady
	clusterStatus.NumberOfReplicasPerPrimary = numberOfReplicasPerPrimary

	return clusterStatus
}

func (c *Controller) updateRedisCluster(rediscluster *rapi.RedisCluster) (*rapi.RedisCluster, error) {
	err := c.client.Update(context.Background(), rediscluster)
	if err != nil {
		glog.Errorf("updateRedisCluster cluster: %v, error: %v", *rediscluster, err)
		return rediscluster, err
	}

	glog.V(6).Infof("RedisCluster %s/%s updated", rediscluster.Namespace, rediscluster.Name)
	return rediscluster, nil
}
