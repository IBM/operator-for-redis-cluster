---
title: Kubectl Plugin
slug: /kubectl-plugin
---

# Kubectl Plugin

The Redis Operator kubectl plugin helps you visualise the status of your Redis cluster. Please visit the [official documentation](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/) for more details.

## Installation

By default, the plugin will install in ```~/.kube/plugins```.

Run `make plugin` to install the plugin. After installation is complete, add the path to plugin to your PATH so [`kubectl`](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/#installing-kubectl-plugins) can find it. By default, the plugin is installed to `$HOME/.kube/plugins/rediscluster`.

Example usage:

```shell
kubectl rc redis-cluster-icm-redis-cluster
NAME                             NAMESPACE  PODS   OPS STATUS  REDIS STATUS  NB PRIMARY  REPLICATION
redis-cluster-icm-redis-cluster  default    4/4/4  ClusterOK   OK            2/2         1-1/1
```
