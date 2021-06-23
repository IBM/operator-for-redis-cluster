package predicate

import (
	"github.com/golang/glog"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var _ predicate.Predicate = &redisClusterReconcilePredicate{}

type redisClusterReconcilePredicate struct{}

func NewRedisClusterPredicate() predicate.Predicate {
	return &redisClusterReconcilePredicate{}
}

func (p *redisClusterReconcilePredicate) Create(e event.CreateEvent) bool {
	glog.Infof("Create event for resource %T: %s/%s", e.Object, e.Object.GetNamespace(), e.Object.GetName())
	return true
}

func (p *redisClusterReconcilePredicate) Delete(e event.DeleteEvent) bool {
	glog.Infof("Delete event for resource %T: %s/%s", e.Object, e.Object.GetNamespace(), e.Object.GetName())
	return true
}

func (p *redisClusterReconcilePredicate) Update(e event.UpdateEvent) bool {
	glog.Infof("Update event for resource %T: %s/%s", e.ObjectNew, e.ObjectNew.GetNamespace(), e.ObjectNew.GetName())
	return true
}

func (p *redisClusterReconcilePredicate) Generic(e event.GenericEvent) bool {
	glog.Infof("Generic event for resource %T: %s/%s", e.Object, e.Object.GetNamespace(), e.Object.GetName())
	return true
}
