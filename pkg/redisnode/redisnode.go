package redisnode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/golang/glog"

	"github.com/heptiolabs/healthcheck"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mediocregopher/radix/v4"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	"github.com/IBM/operator-for-redis-cluster/pkg/utils"
)

// RedisNode contains all info to run the redis-node.
type RedisNode struct {
	config     *Config
	kubeClient clientset.Interface
	redisAdmin redis.AdminInterface
	admOptions redis.AdminOptions

	// Kubernetes Probes handler
	health healthcheck.Handler

	httpServer *http.Server
}

// NewRedisNode builds and returns new RedisNode instance
func NewRedisNode(cfg *Config) *RedisNode {
	kubeConfig, err := initKubeConfig(cfg)
	if err != nil {
		glog.Fatalf("Unable to init rediscluster controller: %v", err)
	}

	kubeClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		glog.Fatalf("Unable to initialize kubeClient:%v", err)
	}

	rn := &RedisNode{
		config:     cfg,
		kubeClient: kubeClient,
	}

	return rn
}

// Run executes the RedisNode
func (r *RedisNode) Run(stop <-chan struct{}) error {
	node, err := r.init()
	if err != nil {
		return err
	}

	go func() {
		err := r.runHttpServer(stop)
		if err != nil {
			glog.Errorf("Failed to run HTTP server: %v", err)
		}
	}()

	node, err = r.run(node)
	if err != nil {
		return err
	}

	glog.Info("Awaiting stop signal")
	<-stop
	glog.Info("Receive Stop Signal...")

	return r.handleStop(node)
}

func initKubeConfig(c *Config) (*rest.Config, error) {
	if len(c.KubeConfigFile) > 0 {
		return clientcmd.BuildConfigFromFlags(c.KubeAPIServer, c.KubeConfigFile) // out of cluster config
	}
	return rest.InClusterConfig()
}

func (r *RedisNode) init() (*Node, error) {
	// Too fast restart of redis-server can result in slots lost
	// This is due to a possible bug in Redis. Redis doesn't check that the node ID behind an IP is still the same after a disconnection/reconnection.
	// And so the the replica reconnects and syncs to an empty node.
	// Therefore, we need to wait for the possible failover to finish.
	// 2 * nodetimeout for failed state detection, voting, and safety
	time.Sleep(r.config.RedisStartDelay)
	ctx := context.Background()
	nodesAddr, err := getRedisNodesAddrs(r.kubeClient, r.config.Cluster.Namespace, r.config.Cluster.NodeService)
	if err != nil {
		glog.Warning(err)
	}

	if len(nodesAddr) > 0 {
		if glog.V(3) {
			for _, node := range nodesAddr {
				glog.Info("REDIS Node addresses:", node)
			}
		}
	} else {
		glog.Info("Redis Node list empty")
	}

	r.admOptions = redis.AdminOptions{
		ConnectionTimeout:  time.Duration(r.config.Redis.DialTimeout) * time.Millisecond,
		RenameCommandsFile: r.config.Redis.GetRenameCommandsFile(),
	}
	host, err := os.Hostname()
	if err != nil {
		r.admOptions.ClientName = host // will be pod name in kubernetes
	}

	r.redisAdmin = redis.NewAdmin(ctx, nodesAddr, &r.admOptions)

	me := NewNode(r.config, r.redisAdmin)
	if me == nil {
		glog.Fatal("Unable to get Node information")
	}
	defer me.Clear()

	// reconfigure redis config file with proper IP/port
	err = me.UpdateNodeConfigFile()
	if err != nil {
		glog.Fatal("Unable to update the configuration file, err:", err)
	}

	err = me.ClearDataFolder() // may be needed if container crashes and restart at the same place
	if err != nil {
		glog.Errorf("Unable to clear data folder, err: %v", err)
	}

	r.httpServer = &http.Server{Addr: r.config.HTTPServerAddr}
	if err := r.configureHealth(ctx); err != nil {
		glog.Errorf("unable to configure health checks, err:%v", err)
		return nil, err
	}

	return me, nil
}

func (r *RedisNode) run(me *Node) (*Node, error) {
	ctx := context.Background()
	// Start redis server and wait for it to be accessible
	chRedis := make(chan error)
	go WrapRedis(r.config, chRedis)
	starter := testAndWaitConnection(ctx, me.Addr, r.config.RedisStartWait)
	if starter != nil {
		glog.Error("Error while waiting for redis to start: ", starter)
		return nil, starter
	}

	configFunc := func() (bool, error) {
		// Initial redis server configuration
		nodes, initCluster := r.isClusterInitialization(me.Addr)

		if initCluster {
			glog.Infof("Initializing cluster with slots from 0 to %d", redis.HashMaxSlots)
			if err := me.InitRedisCluster(ctx, me.Addr); err != nil {
				glog.Error("Unable to init the cluster with this node, err:", err)
				return false, err
			}
		} else {
			glog.Infof("Attaching node to cluster")
			r.redisAdmin.RebuildConnectionMap(ctx, nodes, &r.admOptions)
			if err := me.AttachNodeToCluster(ctx, me.Addr); err != nil {
				glog.Error("Unable to attach a node to the cluster, err:", err)
				return false, nil
			}
		}
		return true, nil
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute)
	defer cancelFunc()
	err := wait.PollUntil(2*time.Second, configFunc, ctx.Done())
	if err != nil {
		glog.Errorf("Failed polling: %v", err)
		return nil, err
	}

	glog.Infof("RedisNode: Running properly")
	return me, nil
}

func (r *RedisNode) isClusterInitialization(currentIP string) ([]string, bool) {
	var initCluster = true
	nodesAddr, _ := getRedisNodesAddrs(r.kubeClient, r.config.Cluster.Namespace, r.config.Cluster.NodeService)
	if len(nodesAddr) > 0 {
		initCluster = false
		if glog.V(3) {
			for _, node := range nodesAddr {
				glog.Info("REDIS Node addresses:", node)
			}
		}
	} else {
		glog.Info("Redis Node list empty")
	}

	if len(nodesAddr) == 1 && nodesAddr[0] == net.JoinHostPort(currentIP, r.config.Redis.ServerPort) {
		// Init Primary cluster
		initCluster = true
	}

	return nodesAddr, initCluster
}

func (r *RedisNode) handleStop(me *Node) error {
	ctx := context.Background()
	nodesAddr, err := getRedisNodesAddrs(r.kubeClient, r.config.Cluster.Namespace, r.config.Cluster.NodeService)
	if err != nil {
		glog.Error("Unable to retrieve Redis Node, err:", err)
		return err
	}

	r.redisAdmin.Connections().ReplaceAll(ctx, nodesAddr)
	if err = me.StartFailover(ctx); err != nil {
		glog.Errorf("Failover node:%s  error:%s", me.Addr, err)
	}

	if err = me.ForgetNode(ctx); err != nil {
		glog.Errorf("Forget node:%s  error:%s", me.Addr, err)
	}

	return err
}

func (r *RedisNode) configureHealth(ctx context.Context) error {
	addr := net.JoinHostPort("127.0.0.1", r.config.Redis.ServerPort)
	health := healthcheck.NewHandler()
	health.AddReadinessCheck("Check redis-node readiness", func() error {
		if err := readinessCheck(ctx, addr); err != nil {
			glog.Errorf("readiness check failed, err:%v", err)
			return err
		}
		return nil
	})

	health.AddLivenessCheck("Check redis-node liveness", func() error {
		if err := livenessCheck(ctx, addr); err != nil {
			glog.Errorf("liveness check failed, err:%v", err)
			return err
		}
		return nil
	})

	r.health = health
	http.Handle("/", r.health)
	http.Handle("/metrics", promhttp.Handler())
	return nil
}

func readinessCheck(ctx context.Context, addr string) error {
	client, rediserr := redis.NewClient(ctx, addr, time.Second, map[string]string{}) // will fail if node not accessible or slot range not set
	if rediserr != nil {
		return fmt.Errorf("Readiness failed, err: %v", rediserr)
	}
	defer client.Close()

	var resp radix.ClusterTopo
	err := client.DoCmd(ctx, &resp, "CLUSTER", "SLOTS")
	if err != nil {
		return fmt.Errorf("Readiness failed, cluster slots response err: %v", err)
	}
	if len(resp) == 0 {
		return fmt.Errorf("Readiness failed, cluster slots response empty")
	}
	glog.V(6).Info("Readiness probe ok")
	return nil
}

func livenessCheck(ctx context.Context, addr string) error {
	client, rediserr := redis.NewClient(ctx, addr, time.Second, map[string]string{}) // will fail if node not accessible or slot range not set
	if rediserr != nil {
		return fmt.Errorf("Liveness failed, err: %v", rediserr)
	}
	defer client.Close()
	glog.V(6).Info("Liveness probe ok")
	return nil
}

func (r *RedisNode) runHttpServer(stop <-chan struct{}) error {

	go func() {
		glog.Infof("Listening on http://%s\n", r.httpServer.Addr)

		if err := r.httpServer.ListenAndServe(); err != nil {
			glog.Error("Http server error: ", err)
		}
	}()

	<-stop
	glog.Info("Shutting down the http server...")
	return r.httpServer.Shutdown(context.Background())
}

// WrapRedis start a redis server in a sub process
func WrapRedis(c *Config, ch chan error) {
	cmd := exec.Command(c.Redis.ServerBin, c.Redis.ConfigFileName)
	cmd.Stdout = utils.NewLogWriter(glog.Info)
	cmd.Stderr = utils.NewLogWriter(glog.Error)

	if err := cmd.Start(); err != nil {
		glog.Error("Error during redis-server start, err", err)
		ch <- err
	}

	if err := cmd.Wait(); err != nil {
		glog.Error("Error during redis-server execution, err", err)
		ch <- err
	}

	glog.Info("Redis-server stop properly")
	ch <- nil
}

func testAndWaitConnection(ctx context.Context, addr string, maxWait time.Duration) error {
	startTime := time.Now()
	waitTime := maxWait
	for {
		currentTime := time.Now()
		timeout := waitTime - startTime.Sub(currentTime)
		if timeout <= 0 {
			return errors.New("Timeout reached")
		}
		dialer := &radix.Dialer{
			NetDialer: &net.Dialer{
				Timeout: timeout,
			},
		}
		client, err := dialer.Dial(ctx, "tcp", addr)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		defer client.Close()
		var resp string
		if err := client.Do(ctx, radix.Cmd(&resp, "PING")); err != nil {
			client.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		} else if resp != "PONG" {
			client.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return nil
	}
}

func getRedisNodesAddrs(kubeClient clientset.Interface, namespace, service string) ([]string, error) {
	addrs := []string{}
	eps, err := kubeClient.CoreV1().Endpoints(namespace).Get(context.Background(), service, metav1.GetOptions{})
	if err != nil {
		return addrs, err
	}

	for _, subset := range eps.Subsets {
		for _, host := range subset.Addresses {
			for _, port := range subset.Ports {
				addrs = append(addrs, net.JoinHostPort(host.IP, strconv.Itoa(int(port.Port))))
			}
		}
	}

	return addrs, nil
}
