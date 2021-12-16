---
title: Rolling Update Procedure
slug: /rolling-update
---

# Rolling Update Procedure

## Overview
In production, developers aim for zero downtime when periodically deploying newer versions of their application. Per Kubernetes documentation:

> rolling updates allow Deployments' update to take place with zero downtime by incrementally updating Pods instances with new ones

To learn more about how rolling updates work in k8s, see [Performing a Rolling Update](https://kubernetes.io/docs/tutorials/kubernetes-basics/update/update-intro/).

## Redis cluster upgrades
A rolling update occurs when the user applies a change to the Redis cluster pod template spec. For example, a user might update the Redis cluster pod image tag in `charts/node-for-redis/values.yaml` and run `helm upgrade`. When the Redis operator detects the pod template spec change, the following procedure takes place:

1. Compare the number of running Redis pods with the number of pods required for the rolling update:
    ```
    # migration pods = 1 + replication factor
    # required pods = # primaries x # migration pods
    # pods to create = # required pods + # migration pods - # of running pods
    ```
   where `# migration pods` is the number of pods needed to migrate one primary and all of its replicas, `# required pods` is the total number of pods required for the cluster, and `# pods to create` is the number of pods to create on a single rolling update iteration.
1. If `# pods to create > 0`, create additional pods with the new pod template spec.
1. Separate old nodes and new nodes according to their pod spec hash annotation.
1. Select the old primary node to replace with one of the newly created pods.
1. Generate the primary to replicas mapping for the newly created pods.
1. Attach the new replicas to the new primary.   
1. Migrate slots (and by default, keys) from the old primary to the new primary. 
1. Detach, forget, and delete the old pods.

The Redis cluster rolling update procedure ensures that there is no downtime as new nodes replace old ones. However, because the migration of keys from old primaries to new ones is a time intensive operation, you may see a temporary decrease in the performance of your cluster during this process. To learn more about step 7, see [key migration](key-migration.md).

## Resource limitations

This procedure requires additional resources beyond what is normally allocated to the Redis cluster. More specifically, this procedure creates an extra `1 + replication factor` pods on each rolling update iteration, so you will need ensure that you have allocated sufficient resources. For standard configurations that allow multiple pods per node, you may need to increase memory + cpu on your existing nodes. If you have configured your cluster topology to limit one Redis pod per k8s node, you may need to increase the number of k8s nodes in your worker pool.

In the case where there are insufficient resources to schedule new Redis pods, the pods will get stuck in `Pending` state. This state is difficult to recover from because the Redis operator will continue to apply the rolling update procedure until it completes. If you find your newly created pods are in `Pending` state, increase the allocated memory + cpus.
