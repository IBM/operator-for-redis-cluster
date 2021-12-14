---
title: Key Migration
slug: /key-migration
---

# Key Migration

## Overview
Key migration is the process by which Redis migrates keys from a source primary node to a destination primary node. The high-level steps for migrating keys can be found [here](https://redis.io/commands/cluster-setslot#redis-cluster-live-resharding-explained). This feature allows users to better configure how keys are migrated, if at all.

Depending on the size of the Redis cluster, the key migration process can be time-consuming. For example, a cluster with thousands of Gigabytes of data can take hours to migrate keys during a scaling or rolling update operation. To speed up the scaling process, we give users the option to migrate slots without keys and provide configuration to control batching.

## Redis cluster migration configuration

### Default
```yaml
rollingUpdate:
  keyMigration: true
  keyBatchSize: 10000
  slotBatchSize: 16
  idleTimeoutMillis: 30000
  warmingDelayMillis: 0

scaling:
  keyBatchSize: 10000
  slotBatchSize: 16
  idleTimeoutMillis: 30000
```

If you observe the default configuration above, you will notice that there are two separate sections for configuring key migration during rolling updates and scaling operations. The `rollingUpdate` section determines how keys are migrated during [rolling updates](rolling-update.md), and the `scaling` section determines how keys are migrated during [scaling operations](scaling.md). The following definitions apply to both configurations.

### Definitions

`keyMigration` specifies whether to migrate keys during rolling updates. For most use cases, users will want to keep this value set to `true`. However, for users who use Redis cluster as a caching tool instead of a persistent database, you may want to consider setting this to `false`. When set to `false`, this feature will transfer slots from the old Redis primary to the new primary without migrating keys. For large clusters, this can save a significant amount of time with the tradeoff of temporarily increasing the number of requests to the backend. The increase in backend hit rate can be mitigated by modifying `warmingDelayMillis`. The next sections will discuss the two different configuration options for key migration.

`keyBatchSize` determines the number of keys to get from a single slot during each migration iteration. By default, this value is `10000` keys.

`slotBatchSize` specifies the number of slots to migrate on each iteration. By default, this value is `16` slots. For most use cases, set this value to the number of logical CPUs. Usually, this is two times the CPU resource limits in the Redis operator deployment. See [runtime.CPU](https://pkg.go.dev/runtime#NumCPU) for more information on how Go checks the number of available CPUs. Also, keep in mind a Redis cluster has `16384` total slots, and those slots are evenly distributed across the primary nodes.

`idleTimeoutMillis` is the maximum idle time at any point during key migration. This means the migration should make progress without blocking for more than the specified number of milliseconds. See the [Redis migrate command](https://redis.io/commands/migrate) for more information about the timeout.

`warmingDelayMillis` is the amount of time in between each batch of slots. As the name suggests, it allows the new Redis node to warm its cache before moving on to the next node in the rolling update.

### Rolling update key migration enabled - `rollingUpdate.keyMigration: true`
`keyBatchSize`: change this value depending on the total number of keys in your Redis cluster. Increasing this value can reduce the amount of time it takes to migrate keys by moving a larger number of keys per batch of slots.

`slotBatchSize`: increasing this value higher than the number of logical CPUs will have minimal effect on rolling update performance.

`idleTimeoutMillis`: do not modify this value unless you receive this specific error.

`warmingDelayMillis`: it's best to set this value to zero unless you want to introduce a delay between slot migrations. You can still set a non-zero delay if you want to reduce the overall strain on the cluster by the migration calls, and you do not care about how long the migration takes.

#### Examples
Assume all clusters have allocated 4 physical CPUs and 1 Gb memory for the Redis operator. We have 8 logical CPUs.

Given a small redis cluster with 3 primaries, RF = 1, and maximum memory of 1 Gb per node - we have the following configuration:

```yaml
rollingUpdate:                # For rolling updates,
  keyMigration: true          # Key migration is enabled
  keyBatchSize: 1000          # Migrate keys in batches of 1,000 per slot 
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
  warmingDelayMillis: 0       # No delay between each batch of 2,500 slots

scaling:                      # For scaling operations,
  keyBatchSize: 1000          # Migrate keys in batches of 1,000 per slot
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
```

Given a large redis cluster with 20 primaries, RF = 1, and maximum memory of 10 Gb per node:
```yaml
rollingUpdate:                # For rolling updates,
  keyMigration: true          # Key migration is enabled
  keyBatchSize: 10000         # Migrate keys in batches of 10,000 per slot
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
  warmingDelayMillis: 0       # No delay between each batch of slots

scaling:                      # For scaling operations,
  keyBatchSize: 10000         # Migrate keys in batches of 10,000 per slot
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
```
Observe how `keyBatchSize` is significantly greater than in the previous example because we have ten times the data per node.

Here we have a cluster with 10 primaries, RF = 1, and maximum memory of 5Gb per node. We optimize for fast key migration on both rolling updates and scaling operations:
```yaml
rollingUpdate:                # For rolling updates,
  keyMigration: true          # Key migration is enabled
  keyBatchSize: 50000         # Migrate keys in batches of 50,000 per slot
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
  warmingDelayMillis: 0       # No delay between each batch of slots

scaling:                      # For scaling operations,
  keyBatchSize: 50000         # Migrate keys in batches of 50,000 per slot
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
```

### Rolling update key migration disabled - `rollingUpdate.keyMigration: false`
`keyBatchSize`: not used when no keys are migrated.

`slotBatchSize`: depends on how quickly you want to wipe the cache. You can use a smaller `slotBatchSize` and increase `warmingDelayMillis` to make this process go slower. If your backend can handle a higher increase in requests, you can set `warmingDelayMillis` to something small or even zero.

`idleTimeoutMillis`: not used in when no keys are migrated.

`warmingDelayMillis`: increasing this value will ease the strain on your backend as slot ownership transfers during a rolling update by pausing after migrating a single batch of slots. 

Please be sure to properly configure `rollingUpdate` based on your Redis cluster if you plan on using this configuration. Setting too small a value for `warmingDelayMillis` will quickly wipe all the keys in your cluster without yielding sufficient time for new Redis nodes to warm up.

#### Examples

Given a small redis cluster with 3 primaries, RF = 1, and maximum memory of 1 Gb per node - we have the following configuration:
```yaml
rollingUpdate:                # For rolling updates,
  keyMigration: false         # Key migration is disabled
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  warmingDelayMillis: 1000    # Wait 1 second between each batch of slots

scaling:                      # For scaling operations,
  keyBatchSize: 10000         # Migrate keys in batches of 10,000 per slot
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
```

Given a large redis cluster with 20 primaries, RF = 1, and maximum memory of 10 Gb per node:
```yaml
rollingUpdate:                # For rolling updates,
  keyMigration: false         # Key migration is disabled
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  warmingDelayMillis: 10000   # Wait 10 seconds between each batch of slots

scaling:                      # For scaling operations,
  keyBatchSize: 10000         # Migrate keys in batches of 10,000 per slot
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
```

Here we have a cluster with 10 primaries, RF = 1, and maximum memory of 5Gb per node. We want to wipe the cache as quickly as possible on rolling updates, while also maintaining fast key migration on scaling operations:
```yaml
rollingUpdate:                # For rolling updates,
  keyMigration: false         # Key migration is disabled
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  warmingDelayMillis: 0       # No delay between each batch of slots

scaling:                      # For scaling operations,
  keyBatchSize: 50000         # Migrate keys in batches of 50,000 per slot
  slotBatchSize: 8            # Transfer 8 slots on each migration iteration
  idleTimeoutMillis: 30000    # Wait up to 30 seconds for any delay in communication during the migration
```