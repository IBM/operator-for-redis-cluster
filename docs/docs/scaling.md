---
title: Scaling Operations
slug: /scaling
---

# Scaling Operations

## Overview
There are many reasons why you would want to scale the number of Redis nodes in your cluster. A few of the most common reasons are: 
- Memory pressure - the nodes in your cluster are close to full capacity (or are at fully capacity and evictions are causing the backend to take more traffic than desired)
  - Horizontally scale the number of primaries to better serve requests
  - Vertically scale your current Redis nodes by allocating more memory
- CPU bottleneck - throughput is low and impacting system performance
  - Horizontally scale the number of primaries to better serve requests
  - Vertically scale your current Redis nodes by allocating more CPUs
- Over-provisioning - you have allocated too many resources for your cluster
  - Scale down if it does not hurt the performance of your system
  - Scale down the number of primaries to save on costs
  - If you are running a Redis cluster with a high replication factor (RF), consider reducing it
  - In multi-zone clusters, scaling down may reduce availability in the case of a zone outage
   
## Impact of scaling
Scaling operations happen in real-time while the Redis cluster receives requests. They are computationally intensive, so expect a decrease in performance while the scaling operation takes place. The extent of the performance impact depends on the size of the data stored in Redis, as well as CPU utilization. See [key migration](key-migration.md) for more information about how Redis keys are migrated from one primary to another during the scaling process.

### Resource Requirements
Like the rolling update procedure, scaling up requires additional resources to create new Redis pods. In the case where there are insufficient resources to schedule new Redis pods, the pods will get stuck in `Pending` state. If you find your newly created pods are in `Pending` state, increase the memory + cpu allocated to your k8s nodes, or add more nodes to your worker pool.

## Scaling primaries
The first option for scaling your cluster is scaling the number of primaries. You can trigger a scaling operation by modifying the `numberOfPrimaries` field in `charts/node-for-redis/values.yaml` and running `helm upgrade` on your cluster.

### Scaling up
Scale up operations take place when the desired number of primaries is greater than the current number of primaries. We take the following actions for scale up operations:

```
For the number of primaries added,
    1. Create a new Redis pod.
    2. Wait for the pod to become ready.

Once the number of desired Redis pods matches the current number of running pods,
    3. Check if we have sufficient primaries. If not, promote replicas to primaries. This only happens when you scale up the number of primaries AND scale down RF.
    4. Add the new Redis nodes to the selection of current primaries.
    5. Place and attach replicas to their respective primaries.
    6. Dispatch slots and migrate keys to the new primaries.
```

After this last step, your cluster will be in normal operating state. The primaries nodes will have an equal number of slots and replica nodes will be properly attached.

#### Example
Given a Redis cluster with the following config:
```yaml
numberOfPrimaries: 3
replicationFactor: 1
```
Assuming your helm release name is `redis-cluster`, scale up `numberOfPrimaries` by running the following:
```
helm upgrade redis-cluster charts/node-for-redis --set numberOfPrimaries=5
```

### Scaling down
Scale down operations take place when the desired number of primaries is less than the current number of primaries. We take the following actions for scale down operations:

```
For the number of primaries deleted,
    1. Select one primary to remove.
    2. Migrate keys from the primary to be removed to the other primaries. Slots are equally distributed across the remaining primaries.
    3. Detach, forget, and delete the primary to be removed.

Once the number of desired Redis pods matches the current number of running pods,
    4. Dispatch slots and migrate keys to the new primaries.
    5. Place and attach replicas to their respective primaries.
```

After this last step, your cluster will be in normal operating state.

#### Example
Given a Redis cluster with the following config:
```yaml
numberOfPrimaries: 5
replicationFactor: 1
```

Scale down `numberOfPrimaries` by running the following:
```
helm upgrade redis-cluster charts/node-for-redis --set numberOfPrimaries=3
```

## Scaling replication factor
The second option for scaling your cluster is scaling RF. You can trigger a scaling operation by modifying the `replicationFactor` field in `charts/node-for-redis/values.yaml` and running `helm upgrade` on your cluster. 

### Scaling up
Scale up operations for RF take place when the desired RF is greater than the current RF. We take the following actions for scale up operations:

```
For the number of replicas added,
    1. Create a new Redis pod.
    2. Wait for the pod to become ready.

Once the number of desired Redis pods matches the current number of running pods,
    3. Add the new Redis nodes to the selection of replicas.
    4. Place and attach replicas to their respective primaries such that each primary has the same number of replicas.
    5. Dispatch slots and migrate keys to the new primaries.
```

After this step, your cluster will be in normal operating state.

#### Example
Given a Redis cluster with the following config:
```yaml
numberOfPrimaries: 3
replicationFactor: 1
```

Scale up `replicationFactor` by running the following:
```
helm upgrade redis-cluster charts/node-for-redis --set replicationFactor=2
```

### Scaling down
Scale down operations for RF take place when the desired RF is less than the current RF. We take the following actions for scale down operations:

```
For the number of replicas deleted,
    For each primary in the cluster,
        1. Calculate the difference between the current RF and desired RF.
            2. If we do not have sufficient replicas for this primary, select new replicas and attach them to the primary.
            3. If we have too many replicas, select replicas to delete. Detach, forget, and delete the replica to be removed.
    
Once the number of desired Redis pods matches the current number of running pods,
    4. Place and attach replicas to their respective primaries.
```

After this step, your cluster will be in normal operating state.

#### Example
Given a Redis cluster with the following config:
```yaml
numberOfPrimaries: 3
replicationFactor: 2
```

Scale down `replicationFactor` by running the following:
```
helm upgrade redis-cluster charts/node-for-redis --set replicationFactor=1
```

## Scaling primaries and replication factor
You may scale both the number of primaries and replication factor in a single `helm upgrade` command. The number of pods created or deleted will be calculated and actions will be taken according to the algorithms described in the previous sections. The following is an example of scaling up `numberOfPrimaries` and `replicationFactor`.

#### Example 1
Given a Redis cluster with the following config:
```yaml
numberOfPrimaries: 3
replicationFactor: 1
```

Increase `numberOfPrimaries` and `replicationFactor` by running the following:
```
helm upgrade redis-cluster charts/node-for-redis --set numberOfPrimaries=4 --set replicationFactor=2
```
---
#### Example 2
You may also scale up one field while scaling down the other:
```yaml
numberOfPrimaries: 4
replicationFactor: 2
```

Increase `numberOfPrimaries` and decrease `replicationFactor` by running the following:
```
helm upgrade redis-cluster charts/node-for-redis --set numberOfPrimaries=5 --set replicationFactor=1
```
