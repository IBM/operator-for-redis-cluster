---
title: Kubectl Plugin
slug: /kubectl-plugin
---

# Kubectl Plugin

The Redis Operator kubectl plugin helps you visualise the status of your Redis cluster. Please visit the [official documentation](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/) for more details.

## Installation

By default, the plugin will install in ```~/.kube/plugins```.

Run `make plugin` to install the plugin. After installation is complete, add the plugin to your PATH so [`kubectl`](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/#installing-kubectl-plugins) can find it. By default, the plugin is installed to `$HOME/.kube/plugins/rediscluster`.

Alternatively, you can download the plugin manually from the assets tab on the [releases page](https://github.com/IBM/operator-for-redis-cluster/releases)

## Usage
Example usage:

```text
kubectl rc
  POD NAME                                    IP              NODE           ID                                        ZONE   USED MEMORY  MAX MEMORY  KEYS        SLOTS
  + rediscluster-rc1-node-for-redis-7jl8q  172.30.255.112  10.183.176.60  5478771ba4c34dbad9df8d30ac4bec5c9ba0842e  wdc04  1023.75M     1.00G       db0=669808  2731-5461
  | rediscluster-rc1-node-for-redis-7q9jn  172.30.68.235   10.191.41.164  15c388f164ad0946691482c7de72939848ca86e2  wdc07  1023.76M     1.00G       db0=669808
  | rediscluster-rc1-node-for-redis-wjkk4  172.30.255.169  10.188.125.58  442de26f8cc9a011df307932a683177a5cd034a9  wdc06  1023.77M     1.00G       db0=669808
  + rediscluster-rc1-node-for-redis-bmrgm  172.30.217.164  10.183.176.51  dd59697e82edf1554468f239f63ea1efd6718d4b  wdc04  1023.78M     1.00G       db0=669675  5462-8192
  | rediscluster-rc1-node-for-redis-7lw4l  172.30.61.98    10.188.125.33  fc5718a7e9bd963f80b9cbd786bbc02d80b7f191  wdc06  1023.78M     1.00G       db0=669675
  | rediscluster-rc1-node-for-redis-qtzx8  172.30.188.104  10.191.41.140  8cd31d550d6935868d5da12ab78ddfb8c6fea1a2  wdc07  1023.78M     1.00G       db0=669675
  + rediscluster-rc1-node-for-redis-dbrmg  172.30.140.228  10.183.176.53  c56420993afd35596425ae9e10a7e902cad5b6f8  wdc04  1023.78M     1.00G       db0=670310  13655-16383
  | rediscluster-rc1-node-for-redis-f4mxd  172.30.68.236   10.191.41.164  b3356e03f83227832662f1bc3bae50273e99059b  wdc07  1023.82M     1.00G       db0=670310
  | rediscluster-rc1-node-for-redis-hhzdx  172.30.255.170  10.188.125.58  cc7bb986e42dd2fe9ead405e39a1a559b1b86e71  wdc06  1023.77M     1.00G       db0=670310
  + rediscluster-rc1-node-for-redis-srxrs  172.30.188.105  10.191.41.140  31e880eef26377e719b28894a8fa469939b05a98  wdc07  1023.79M     1.00G       db0=669522  10924-13654
  | rediscluster-rc1-node-for-redis-hg6q4  172.30.255.113  10.183.176.60  bd9cd001e5bdcba527277e39f63908ea18504f75  wdc04  1023.80M     1.00G       db0=669522
  | rediscluster-rc1-node-for-redis-zgmqk  172.30.16.231   10.188.125.3   caaac384a8cd2cb6181925c889557bdb028aec0e  wdc06  1023.81M     1.00G       db0=669522
  + rediscluster-rc1-node-for-redis-szb9x  172.30.61.97    10.188.125.33  07de0c67262e816f7792f0a68c4c4a5b47291f46  wdc06  1023.80M     1.00G       db0=669743  0-2730
  | rediscluster-rc1-node-for-redis-gvkb4  172.30.217.165  10.183.176.51  f1ff1e5c0a77cb7b5850d8808f950c1303f96056  wdc04  1023.76M     1.00G       db0=669743
  | rediscluster-rc1-node-for-redis-znn4x  172.30.67.94    10.191.41.163  e0ab667b6629a660b01c20574750e3a487c69a1b  wdc07  1023.78M     1.00G       db0=669743
  + rediscluster-rc1-node-for-redis-xf8mr  172.30.67.95    10.191.41.163  8e9fe3e022f63f5fcc2a117edaadd93e39bfbb27  wdc07  1023.78M     1.00G       db0=669711  8193-10923
  | rediscluster-rc1-node-for-redis-r2qd2  172.30.16.232   10.188.125.3   0da8f5799b86c40f7ea0c3ebf71ce3955afa1dd1  wdc06  1023.79M     1.00G       db0=669711
  | rediscluster-rc1-node-for-redis-zfsxt  172.30.140.229  10.183.176.53  b4c22806c4c2529713e6e05b0d6fd994f0e801c7  wdc04  1023.78M     1.00G       db0=669711

  POD NAME                                    IP              NODE           ID                                        ZONE   USED MEMORY  MAX MEMORY  KEYS        SLOTS
  + rediscluster-rc2-node-for-redis-57g56  172.30.68.237   10.191.41.164  cc82878c2bdd6381d569709334db86ef6cfade19  wdc07  175.67M      1.00G       db0=113397  5462-10923
  + rediscluster-rc2-node-for-redis-942rt  172.30.140.230  10.183.176.53  291f2f06d13a1d821219bc9acfbd5323be1552d9  wdc04  174.82M      1.00G       db0=112681  10924-16383
  + rediscluster-rc2-node-for-redis-gqmhs  172.30.16.233   10.188.125.3   85f3c1c6652b5fb76b40c8bdffe3a9af57f22d4d  wdc06  175.34M      1.00G       db0=113014  0-5461

  NAME                   NAMESPACE  PODS      OPS STATUS  REDIS STATUS  NB PRIMARY  REPLICATION
  rc1-node-for-redis  default    18/18/18  ClusterOK   OK            6/6         2-2/2
  rc2-node-for-redis  default    3/3/3     ClusterOK   OK            3/3         0-0/0
```

The example above illustrates the Redis cluster plugin's output when there are two clusters being managed by one Redis operator. The top portion of the output is referred to as the Redis cluster's status and the bottom portion refers to the Redis cluster's state.

## Redis Cluster Status
The Redis cluster status provides an easy way to see how your cluster is set up and which replicas belong to which primary. Without the cluster state, it's really difficult to determine in which `ZONE` a Redis pod resides. The `POD NAME`, `IP`, and `ID` fields help when parsing logs because these properties are used differently depending on what part of the code is logging messages. The `USED MEMORY` and `MAX MEMORY` fields help determine if you should be seeing evictions, while the `KEYS` and `SLOTS` fields can help determine if you have any hot spots in your cluster.

### Redis Cluster Status Fields
|Field|Description|
| --- | --- |
|POD NAME|Name of Redis pod where the first character indicates its role. See the [legend](#redis-cluster-pod-role-prefix-legend) for more details|
|IP|Internal IP address of the Redis pod|
|NODE|IP address of the worker node on which the pod has been scheduled|
|ID|Redis ID of the pod|
|ZONE|Zone of the worker node on which the pod has been scheduled|
|USED MEMORY|Human-readable representation of the total number of bytes allocated by Redis using its allocator|
|MAX MEMORY|Human-readable representation of Redis' `maxmemory` configuration directive|
|KEYS|List of the number of keys in each database using the form `db0:db0_key_count`,`db1:db1_key_count`|
|SLOTS|List of the slot ranges owned by this primary|

### Redis Cluster Pod Role Prefix Legend
|Prefix|Description|
| :---: | --- |
|+|Primary|
|\||Replica to the primary above|
|?|Pod that matches the label selector but is not a part of the cluster yet|
|^|Pod that is currently joining the cluster but has yet to be assigned a role|

## Redis Cluster State
The Redis cluster state provides additional information that is useful when trying to determine the cluster's health. It also indicates if a scaling, rebalancing, or rolling update operation is occurring.

### Redis Cluster State Fields
|Field|Description|
| --- | --- |
|NAME|Redis cluster name|
|NAMESPACE|Namespace of Redis cluster deployment|
|OPS STATUS|ClusterOK \| Scaling \| Rebalancing \| RollingUpdate|
|REDIS STATUS|OK \| KO \| Scaling \| Calculating Rebalancing \| Rebalancing \| RollingUpdate|
|PODS|Current number of ready pods / current number of total pods / desired number of pods|
|NB PRIMARY|Current number of primaries / desired number of primaries|
|REPLICATION|Current min RF - current max RF / desired RF|
|ZONE SKEW|Primary node zone skew / replica node zone skew / BALANCED \| UNBALANCED |
