package controller

import (
	"context"
	"fmt"
	"math"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/IBM/operator-for-redis-cluster/pkg/utils"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller/pod"
	"github.com/IBM/operator-for-redis-cluster/pkg/controller/sanitycheck"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

const (
	requeueDelay = time.Second * 5
)

// Controller contains all controller fields
type Controller struct {
	mgr    manager.Manager
	client kclient.Client

	podControl                 pod.RedisClusterControlInterface
	serviceControl             ServicesControlInterface
	podDisruptionBudgetControl PodDisruptionBudgetsControlInterface

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

	return controller
}

func SetupRedisClusterController(mgr ctrl.Manager, redisClusterController *Controller) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("rediscluster").
		For(&rapi.RedisCluster{}).
		Owns(&v1.Pod{}).
		Owns(&v1.Service{}).
		Owns(&v1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		//WithEventFilter(predicate.NewRedisClusterPredicate()). //uncomment to see kubernetes events in the logs, e.g. ConfigMap updates
		Complete(redisClusterController)
}

func (c *Controller) Reconcile(ctx context.Context, namespacedName ctrl.Request) (ctrl.Result, error) {
	var err error
	glog.V(2).Infof("Reconcile() key:%s", namespacedName)
	startTime := metav1.Now()
	defer func() {
		reconcileTime := time.Since(startTime.Time)
		glog.V(2).Infof("finished reconciling RedisCluster %q (%v)", namespacedName, reconcileTime)
	}()
	result := ctrl.Result{}
	sharedRedisCluster := &rapi.RedisCluster{}
	err = c.client.Get(ctx, namespacedName.NamespacedName, sharedRedisCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			glog.Infof("RedisCluster %s not found. Might be deleted.", namespacedName)
			return result, nil
		}
		glog.Errorf("unable to get RedisCluster %s: %v", namespacedName, err)
		return result, err
	}

	if !rapi.IsRedisClusterDefaulted(sharedRedisCluster) {
		defaultedRedisCluster := rapi.DefaultRedisCluster(sharedRedisCluster)
		if result.Requeue = c.updateRedisClusterSpec(defaultedRedisCluster); result.Requeue {
			return result, nil
		}
		glog.V(6).Infof("RedisCluster %s correctly defaulted", namespacedName)
	}

	if sharedRedisCluster.DeletionTimestamp != nil {
		return result, nil
	}

	redisCluster := sharedRedisCluster.DeepCopy()

	// init status.StartTime
	if redisCluster.Status.StartTime == nil {
		redisCluster.Status.StartTime = &startTime
		if result.Requeue = c.updateRedisClusterStatus(ctx, redisCluster); result.Requeue {
			return result, nil
		}
		glog.V(4).Infof("startTime updated for RedisCluster %s", namespacedName)
	}
	return c.syncCluster(ctx, redisCluster)
}

func (c *Controller) reconcileConfigMap(ctx context.Context, cluster *rapi.RedisCluster) (*v1.ConfigMap, error) {
	cm, err := c.getRedisClusterConfigMap(cluster)
	if err != nil {
		glog.Errorf("unable to get redis cluster config map: %v", err)
		return nil, err
	}
	if len(cm.OwnerReferences) == 0 {
		if err = controllerutil.SetControllerReference(cluster, cm, c.mgr.GetScheme()); err != nil {
			return nil, err
		}
		if err = c.client.Update(ctx, cm); err != nil {
			return nil, err
		}
	}
	return cm, nil
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

func (c *Controller) getRedisClusterConfigMap(redisCluster *rapi.RedisCluster) (*v1.ConfigMap, error) {
	configMap := &v1.ConfigMap{}
	namespacedName := types.NamespacedName{
		Name:      redisCluster.Name,
		Namespace: redisCluster.Namespace,
	}
	return configMap, c.client.Get(context.Background(), namespacedName, configMap)
}

func (c *Controller) syncCluster(ctx context.Context, redisCluster *rapi.RedisCluster) (ctrl.Result, error) {
	glog.V(6).Info("syncCluster START")
	defer glog.V(6).Info("syncCluster STOP")
	result := ctrl.Result{}

	redisClusterConfigMap, err := c.reconcileConfigMap(ctx, redisCluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.Reconcile unable to update config map associated with RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
		return result, err
	}

	redisClusterService, err := c.getRedisClusterService(redisCluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.Reconcile unable to retrieve service associated with RedisCluster: %s/%s", redisCluster.Namespace, redisCluster.Name)
		return result, err
	}
	if redisClusterService == nil {
		if _, err = c.serviceControl.CreateRedisClusterService(redisCluster); err != nil {
			glog.Errorf("RedisCluster-Operator.Reconcile unable to create service associated with RedisCluster: %s/%s", redisCluster.Namespace, redisCluster.Name)
			return result, err
		}
	}

	redisClusterPodDisruptionBudget, err := c.getRedisClusterPodDisruptionBudget(redisCluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.Reconcile unable to retrieve podDisruptionBudget associated with RedisCluster: %s/%s", redisCluster.Namespace, redisCluster.Name)
		return result, err
	}
	if redisClusterPodDisruptionBudget == nil {
		if _, err = c.podDisruptionBudgetControl.CreateRedisClusterPodDisruptionBudget(redisCluster); err != nil {
			glog.Errorf("RedisCluster-Operator.Reconcile unable to create podDisruptionBudget associated with RedisCluster: %s/%s", redisCluster.Namespace, redisCluster.Name)
			return result, err
		}
	}

	redisPods, err := c.podControl.GetRedisClusterPods(redisCluster)
	if err != nil {
		glog.Errorf("RedisCluster-Operator.Reconcile unable to retrieve pods associated with RedisCluster: %s/%s", redisCluster.Namespace, redisCluster.Name)
		return result, err
	}

	pods, lostPods := filterLostNodes(redisPods)
	if len(lostPods) != 0 {
		for _, p := range lostPods {
			err = c.podControl.DeletePodNow(redisCluster, p.Name)
			glog.Errorf("Lost node associated with pod %s: %v", p.Name, err)
		}
		redisPods = pods
	}

	admin, err := redis.NewRedisAdmin(ctx, redisPods, &c.config.redis)
	if err != nil {
		return result, fmt.Errorf("unable to create the redis.Admin, err:%v", err)
	}
	defer admin.Close()

	clusterInfos, errGetInfos := admin.GetClusterInfos(ctx)
	if errGetInfos != nil {
		glog.Errorf("error when getting cluster infos to rebuild bom : %v", errGetInfos)
		if clusterInfos.Status == redis.ClusterInfoPartial {
			return result, fmt.Errorf("partial cluster info")
		}
	}

	kubeNodes, err := utils.GetKubeNodes(ctx, c.client, redisCluster.Spec.PodTemplate.Spec.NodeSelector)
	if err != nil {
		glog.Errorf("error getting k8s nodes with label selector %q: %v", redisCluster.Spec.PodTemplate.Spec.NodeSelector, err)
		return result, err
	}

	// build the cluster status from the RedisCluster nodes,
	clusterState, err := c.buildClusterState(redisCluster, clusterInfos, redisPods, kubeNodes)
	if err != nil {
		return result, fmt.Errorf("unable to build RedisCluster status, err: %v", err)
	}
	redisCluster.Status.Cluster = *clusterState
	allPodsReady := true
	if clusterState.NumberOfPods-clusterState.NumberOfRedisNodesRunning != 0 {
		glog.V(3).Infof("not all redis nodes are running, numberOfPods: %d, numberOfRedisNodesRunning: %d", clusterState.NumberOfPods, clusterState.NumberOfRedisNodesRunning)
		allPodsReady = false
	}

	// check if the operator needs to execute some operation on the redis cluster
	needSanitize, err := c.checkSanity(ctx, redisCluster, admin, clusterInfos)
	if err != nil {
		glog.Errorf("checkSanity error occurred in dry run mode: %v", err)
		return result, err
	}

	if allPodsReady {
		configChanges, err := checkServerConfig(ctx, admin, redisClusterConfigMap)
		if err != nil {
			glog.Warningf("unable to get server config: %v", err)
		}
		if len(configChanges) > 0 {
			updateConfig(ctx, admin, configChanges)
			c.recorder.Event(redisCluster, v1.EventTypeNormal, "ConfigUpdate", "Server configuration updated")
		}
		if !checkZoneBalance(redisCluster) {
			glog.Warningf("Node zones are not balanced. Trigger a rolling update to force reschedule redis pods.")
			c.recorder.Event(redisCluster, v1.EventTypeWarning, "UnbalancedZones", "Zones are unbalanced")
		}
		if needClusterOperation(redisCluster) || needSanitize {
			result, err = c.clusterAction(ctx, admin, redisCluster, clusterInfos)
			if err != nil {
				return result, err
			}
			if c.updateClusterStatus(ctx, redisCluster) {
				result.Requeue = true
			}
			return result, nil
		}
	}

	setClusterStatusCondition(&redisCluster.Status, true)
	result.Requeue = c.updateClusterStatus(ctx, redisCluster)
	return result, nil
}

func (c *Controller) updateRedisClusterSpec(desiredCluster *rapi.RedisCluster) bool {
	ctx := context.Background()
	actualCluster, err := c.getRedisCluster(ctx, desiredCluster.Namespace, desiredCluster.Name)
	if err != nil {
		glog.Errorf("failed to get RedisCluster %s/%s: %v", desiredCluster.Namespace, desiredCluster.Name, err)
		return false
	}
	actualCluster.Spec = desiredCluster.Spec
	if err = c.client.Update(ctx, actualCluster); err != nil {
		if errors.IsConflict(err) {
			glog.V(6).Infof("conflict occurred when updating RedisCluster %s/%s", desiredCluster.Namespace, desiredCluster.Name)
			return true
		}
		glog.Errorf("failed to update RedisCluster %s/%s: %v", desiredCluster.Name, desiredCluster.Namespace, err)
		return true
	}

	glog.V(6).Infof("RedisCluster %s/%s updated", desiredCluster.Namespace, desiredCluster.Name)
	return false
}

func (c *Controller) updateRedisClusterStatus(ctx context.Context, redisCluster *rapi.RedisCluster) bool {
	if err := c.client.Status().Update(ctx, redisCluster); err != nil {
		if errors.IsConflict(err) {
			glog.V(6).Infof("conflict occurred when updating RedisCluster %s/%s", redisCluster.Namespace, redisCluster.Name)
			return true
		}
		glog.Errorf("failed to update RedisCluster %s/%s: %v", redisCluster.Namespace, redisCluster.Name, err)
		return true
	}

	glog.V(6).Infof("RedisCluster %s/%s updated", redisCluster.Namespace, redisCluster.Name)
	return false
}

func (c *Controller) updateClusterStatus(ctx context.Context, desiredCluster *rapi.RedisCluster) bool {
	actualCluster, err := c.getRedisCluster(ctx, desiredCluster.Namespace, desiredCluster.Name)
	if err != nil {
		glog.Errorf("failed to get RedisCluster %s/%s: %v", desiredCluster.Namespace, desiredCluster.Name, err)
		return true
	}
	if compareStatus(&actualCluster.Status, &desiredCluster.Status) {
		glog.V(3).Infof("status changed for RedisCluster %s/%s", actualCluster.Namespace, actualCluster.Name)
		// the status have been update, needs to update the RedisCluster
		actualCluster.Status = desiredCluster.Status
		return c.updateRedisClusterStatus(ctx, actualCluster)
	}
	return false
}

func (c *Controller) buildClusterState(cluster *rapi.RedisCluster, clusterInfos *redis.ClusterInfos, pods []v1.Pod, kubeNodes []v1.Node) (*rapi.RedisClusterState, error) {
	clusterState := getRedisClusterState(clusterInfos, pods, kubeNodes)
	podLabels, err := pod.GetLabelsSet(cluster)
	if err != nil {
		glog.Errorf("unable to get label set: %v", err)
	}
	clusterState.LabelSelectorPath = podLabels.String()
	min, max := getReplicationFactors(clusterState.NumberOfReplicasPerPrimary)
	clusterState.MinReplicationFactor = int32(min)
	clusterState.MaxReplicationFactor = int32(max)
	clusterState.Status = cluster.Status.Cluster.Status
	glog.V(3).Infof("current cluster state: %s ", clusterState.String())
	return clusterState, nil
}

func (c *Controller) checkSanity(ctx context.Context, cluster *rapi.RedisCluster, admin redis.AdminInterface, infos *redis.ClusterInfos) (bool, error) {
	return sanitycheck.RunSanityChecks(ctx, admin, &c.config.redis, c.podControl, cluster, infos, true)
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

func getRedisClusterState(clusterInfos *redis.ClusterInfos, pods []v1.Pod, kubeNodes []v1.Node) *rapi.RedisClusterState {
	clusterState := &rapi.RedisClusterState{}
	clusterState.NumberOfPodsReady = 0
	clusterState.NumberOfRedisNodesRunning = 0
	clusterState.NumberOfPods = int32(len(pods))
	clusterState.NumberOfReplicasPerPrimary = map[string]int{}

	numberOfPodsReady := int32(0)
	numberOfRedisNodesRunning := int32(0)
	numberOfPrimaries := int32(0)
	numberOfPrimariesReady := int32(0)
	numberOfReplicasPerPrimary := map[string]int{}

	for i, p := range pods {
		podReady, _ := utils.IsPodReady(&p)
		if !podReady {
			continue
		}
		numberOfPodsReady++
		// find corresponding Redis node
		redisNodes, err := clusterInfos.GetNodes().GetNodesByFunc(func(node *redis.Node) bool {
			return node.IP == p.Status.PodIP
		})
		if err != nil {
			glog.Warningf("unable to retrieve redis node associated with pod: %s, ip: %s, err: %v", p.Name, p.Status.PodIP, err)
			continue
		}
		newNode := rapi.RedisClusterNode{
			PodName: p.Name,
			IP:      p.Status.PodIP,
			Zone:    utils.GetZone(p.Spec.NodeName, kubeNodes),
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
		clusterState.Nodes = append(clusterState.Nodes, newNode)
	}

	clusterState.NumberOfRedisNodesRunning = numberOfRedisNodesRunning
	clusterState.NumberOfPrimaries = numberOfPrimaries
	clusterState.NumberOfPrimariesReady = numberOfPrimariesReady
	clusterState.NumberOfPodsReady = numberOfPodsReady
	clusterState.NumberOfReplicasPerPrimary = numberOfReplicasPerPrimary

	return clusterState
}
