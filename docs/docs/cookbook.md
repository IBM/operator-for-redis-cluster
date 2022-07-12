---
title: Cookbook
slug: /cookbook
---

# Cookbook

## Installation

Operator for Redis Cluster is written in [Go](https://golang.org/).

### Required Dependencies

* `make`
* [Go 1.17+](https://golang.org/doc/install)
* [Docker](https://www.docker.com/)
* [Helm 3](https://helm.sh)

### Recommended Dependencies

* [Kind](https://kind.sigs.k8s.io/) or [Minikube](https://github.com/kubernetes/minikube)
* [golangci-lint](https://github.com/golangci/golangci-lint)

### Download and build the source code

Start by making a fork of the `operator-for-redis-cluster` repository. Then, clone your forked repo:

```console
$ git clone git@github.com:<YOUR_USERNAME>/operator-for-redis-cluster.git
Cloning into 'operator-for-redis-cluster'...
$ cd operator-for-redis-cluster
```

Build the project:

```console
$ make build
CGO_ENABLED=0 go build -installsuffix cgo -ldflags "-w -X github.com/IBM/operator-for-redis-cluster/pkg/utils.TAG=0.3.4 -X github.com/IBM/operator-for-redis-cluster/pkg/utils.COMMIT=81c58d3bb6e713679637d9971fc8f795ca5a3e2f -X github.com/IBM/operator-for-redis-cluster/pkg/utils.OPERATOR_VERSION= -X github.com/IBM/operator-for-redis-cluster/pkg/utils.REDIS_VERSION= -X github.com/IBM/operator-for-redis-cluster/pkg/utils.BUILDTIME=2021-10-21/12:44:33 -s" -o bin/operator ./cmd/operator
CGO_ENABLED=0 go build -installsuffix cgo -ldflags "-w -X github.com/IBM/operator-for-redis-cluster/pkg/utils.TAG=0.3.4 -X github.com/IBM/operator-for-redis-cluster/pkg/utils.COMMIT=81c58d3bb6e713679637d9971fc8f795ca5a3e2f -X github.com/IBM/operator-for-redis-cluster/pkg/utils.OPERATOR_VERSION= -X github.com/IBM/operator-for-redis-cluster/pkg/utils.REDIS_VERSION= -X github.com/IBM/operator-for-redis-cluster/pkg/utils.BUILDTIME=2021-10-21/12:44:35 -s" -o bin/node ./cmd/node
CGO_ENABLED=0 go build -installsuffix cgo -ldflags "-w -X github.com/IBM/operator-for-redis-cluster/pkg/utils.TAG=0.3.4 -X github.com/IBM/operator-for-redis-cluster/pkg/utils.COMMIT=81c58d3bb6e713679637d9971fc8f795ca5a3e2f -X github.com/IBM/operator-for-redis-cluster/pkg/utils.OPERATOR_VERSION= -X github.com/IBM/operator-for-redis-cluster/pkg/utils.REDIS_VERSION= -X github.com/IBM/operator-for-redis-cluster/pkg/utils.BUILDTIME=2021-10-21/12:44:37 -s" -o bin/metrics ./cmd/metrics
```

Run the test suite to make sure everything works:

```console
$ make test
./go.test.sh
ok  	github.com/IBM/operator-for-redis-cluster/pkg/controller	5.162s	coverage: 33.1% of statements
ok  	github.com/IBM/operator-for-redis-cluster/pkg/controller/clustering	0.711s	coverage: 75.6% of statements
ok  	github.com/IBM/operator-for-redis-cluster/pkg/controller/pod	1.726s	coverage: 40.0% of statements
ok  	github.com/IBM/operator-for-redis-cluster/pkg/controller/sanitycheck	0.631s	coverage: 21.5% of statements
ok  	github.com/IBM/operator-for-redis-cluster/pkg/garbagecollector	1.740s	coverage: 75.0% of statements
ok  	github.com/IBM/operator-for-redis-cluster/pkg/redis	0.728s	coverage: 22.4% of statements
ok  	github.com/IBM/operator-for-redis-cluster/pkg/redis/fake	0.148s	coverage: 85.8% of statements
ok  	github.com/IBM/operator-for-redis-cluster/pkg/redisnode	1.924s	coverage: 43.4% of statements
```

Install the kubectl Redis cluster plugin (more info [here](./kubectl-plugin.md))

```console
$ make plugin
```

## Create a Kubernetes cluster
To run the Redis operator, you need to have a running Kubernetes cluster. You can use local k8s cluster frameworks such as `kind` or `minikube`. Use the following guide to install a `kind` cluster similar to what we use in our e2e tests.

From the project root directory, create your kind cluster using the e2e test configuration:
```console
$ kind create cluster --config ./test/e2e/kind_config.yml
```

Build the required docker images:
```console
make container PREFIX= TAG=latest
```

Once the kind cluster is up and running, load the images into the kind cluster:
```console
$ kind load docker-image operator-for-redis:latest
$ kind load docker-image node-for-redis:latest
$ kind load docker-image metrics-for-redis:latest
```

### Deploy a Redis operator

Install the `operator-for-redis` Helm chart:
```console
$ helm install op charts/operator-for-redis --wait --set image.repository=operator-for-redis --set image.tag=latest
NAME: op
LAST DEPLOYED: Thu Oct 21 15:11:51 2021
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

Confirm that the operator is running properly:

```console
$ kubectl get pods
NAME                                     READY   STATUS    RESTARTS   AGE
op-operator-for-redis-64dbfb4b59-xjttw   1/1     Running   0          31s
```

### Deploy a Redis cluster

Install the `node-for-redis` Helm chart:
```console
$ helm install --wait cluster charts/node-for-redis --set image.repository=node-for-redis --set image.tag=latest
NAME: cluster
LAST DEPLOYED: Thu Oct 21 15:12:05 2021
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

Check the cluster status:
```console
$ kubectl rc
  POD NAME                                        IP           NODE        ID                                        ZONE   USED MEMORY  MAX MEMORY  KEYS  SLOTS
  + rediscluster-cluster-node-for-redis-2h92v  10.244.1.89  172.18.0.3  5606ea9ab09678124a4b17de10ab92a78aac0b4d  dal13  35.55M       10.95G            5462-10923
  | rediscluster-cluster-node-for-redis-nf24b  10.244.2.89  172.18.0.2  6840c0e5db16ebf073f57c67a6487c1c7f0d12d1  dal10  2.62M        10.95G
  + rediscluster-cluster-node-for-redis-4h4s2  10.244.2.88  172.18.0.2  8a2f2db39b85cf059e88dc80d7c9cafefac94de0  dal10  34.63M       10.95G            10924-16383
  | rediscluster-cluster-node-for-redis-77bt2  10.244.3.87  172.18.0.4  74582d0e0cedb458e81f1e9d4f32cdc3f5e9399b  dal12  2.60M        10.95G
  + rediscluster-cluster-node-for-redis-dh6pt  10.244.3.86  172.18.0.4  81f5c13bec9a0545a62de08b2a309a87d29855c7  dal12  2.83M        10.95G            0-5461
  | rediscluster-cluster-node-for-redis-jnh2h  10.244.1.88  172.18.0.3  ffae381633377414597731597518529255fd9b69  dal13  2.64M        10.95G

  NAME                       NAMESPACE  PODS   OPS STATUS  REDIS STATUS  NB PRIMARY  REPLICATION  ZONE SKEW
  cluster-node-for-redis  default    6/6/6  ClusterOK   OK            3/3         1-1/1        0/0/BALANCED
```

### Clean up your environment

Delete the Redis cluster:
```console
$ helm uninstall cluster
release "cluster" deleted
```

Delete the Redis operator:
```console
$ helm uninstall op
release "op" deleted
```

Ensure all pods have been deleted:
```console
$ kubectl get pods
No resources found in default namespace.
```

## Run end-to-end tests
If you followed the steps for creating a `kind` cluster with the e2e test configuration, then running the e2e tests is simple.

Build the required docker images:
```console
make container PREFIX=ibmcom/ TAG=local
make container-node PREFIX=ibmcom/ TAG=new
```
Note that we need both `local` and `new` image tags for a rolling update e2e test case.

Load the required images into the kind cluster:
```console
$ kind load docker-image ibmcom/operator-for-redis:local
$ kind load docker-image ibmcom/node-for-redis:local
$ kind load docker-image ibmcom/node-for-redis:new
```

Once the kind cluster is up and running, deploy the `operator-for-redis` Helm chart:
```console
$ helm install op charts/operator-for-redis --wait --set image.repository=ibmcom/operator-for-redis --set image.tag=local
NAME: op
LAST DEPLOYED: Thu Oct 21 15:11:51 2021
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

When the `operator-for-redis` pod is up and running, you can start the e2e regression:
```console
$ go test -timeout 30m ./test/e2e --kubeconfig=$HOME/.kube/config --ginkgo.v --test.v
Running Suite: RedisCluster Suite
=================================
Random Seed: 1634845111
Will run 11 of 11 specs

Oct 21 15:38:31.261: INFO: KubeconfigPath-> "/Users/kscharm/.kube/config"
Oct 21 15:38:31.295: INFO: Check whether RedisCluster resource is registered...
RedisCluster CRUD operations
  should create a RedisCluster
  
...

Ran 11 of 11 Specs in 517.299 seconds
SUCCESS! -- 11 Passed | 0 Failed | 0 Pending | 0 Skipped
PASS
ok  	github.com/IBM/operator-for-redis-cluster/test/e2e	517.776s
```
