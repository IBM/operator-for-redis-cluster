---
title: Redis Server Configuration
slug: /configuration
---

# Redis Server Configuration

## Overview
A Redis server can be configured by providing a Redis configuration file called `redis.conf`. To read more about the format of this file, see the [configuration documentation](https://redis.io/topics/config).

## Redis Cluster Configuration
The Redis operator manages clusters that operate in **cluster mode**. This means every node in the cluster specifies the `cluster-enabled yes` configuration option. It also means that every node in the cluster will have the same configuration. You do not need to set `cluster-enabled` explicitly in your configuration because we automatically add the setting if it is not present when a Redis pod starts.

Redis clusters that operate in cluster mode support data sharding, which is essential to ensuring high availability. See the [Redis cluster specification](https://redis.io/topics/cluster-spec) to learn more.

## Configuration Options
There are various configuration options you will want to consider whether you are using Redis as a cache or as a persistent database. We urge you to read the [Redis configuration documentation](https://raw.githubusercontent.com/redis/redis/6.2/redis.conf) to understand the tradeoffs for each option.

### Defaults
A Redis server is able to start without specifying a `redis.conf` configuration file or providing override configuration; it will instead use the default settings. We do not recommend using the defaults in a production environment, as your Redis database can quickly exceed the amount of memory allocated to your Redis pods. Our operator currently deploys and manages Redis clusters using Redis 6.2. Read the default configuration for [Redis 6.2](https://raw.githubusercontent.com/redis/redis/6.2/redis.conf) to learn more about the specific settings.

### Persistence
If you use Redis as a database, you will need to enable persistence. Redis provides multiple persistence options, such as Redis Database (RDB) and Append Only File (AOF). You can read more about the advantages and disadvantages of each persistence option in the [persistence documentation](https://redis.io/topics/persistence).

#### Snapshotting
To quote the Redis documentation:
> By default Redis saves snapshots of the dataset on disk, in a binary file called dump.rdb. You can configure Redis to have it save the dataset every N seconds if there are at least M changes in the dataset, or you can manually call the SAVE or BGSAVE commands.

Using the default settings, snapshotting will occur:

- after 3600 sec (60 min) if at least 1 key changed
- after 300 sec (5 min) if at least 10 keys changed
- after 60 sec if at least 10000 keys changed

Snapshots can be extremely useful for backups and faster restarts. We recommend configuring `save` to a reasonable value depending on the number of requests per second your database processes.

If you use Redis as a cache, be sure to disable snapshotting by setting `save ""` in `redis.conf`. For a large Redis cluster processing thousands of requests per second, disk can fill up fairly quickly with database snapshots. Disabling snapshotting will prevent Redis from dumping the entire dataset to disk all together.

### Max Memory
To quote the Redis documentation:
> The `maxmemory` configuration directive is used in order to configure Redis to use a specified amount of memory for the data set.

We highly encourage setting `maxmemory` to a value lower than the allocated memory to each Redis pod. By default, we set `maxmemory` to 70% of the allocated Redis pod memory. You should change this value depending on how much additional memory the Redis process consumes doing other operations.

### Eviction Policies
See [Using Redis as an LRU cache](https://redis.io/topics/lru-cache) to learn more about which `maxmemory-policy` is best for your needs. If you use Redis as a database, you will likely want to keep the default `maxmemory-policy` set to `noeviction`.

## Overriding redis.conf
You have two separate options for overriding the default `redis.conf` when deploying a Redis cluster. The first is to specify your configuration as key-value pairs in `redis.configuration.valueMap`:

```yaml
redis:
  configuration:
    valueMap:
      maxmemory-policy: "volatile-lfu"
      maxmemory: "10Gb"
      ...
```

Note: be sure to always quote values if you decide to use this approach.

The second option is to specify `redis.configuration.file` with the path to your `redis.conf` file. For example:

```yaml
redis:
  configuration:
    file: "/path/to/redis.conf"
```

Your `redis.conf` file should have the same format as any Redis configuration file:

```text
maxmemory-policy volatile-lfu
maxmemory 10gb
...
```

Note: do **not** quote values in Redis configuration files.

### Configuration Examples

#### Redis as a Database
```yaml
redis:
  configuration:
    valueMap:
      maxmemory-policy: "noeviction"       # Do not evict keys even if memory is full
      maxmemory: "100gb"
      save: "3600 1 300 10 60 10000"       # Enable RDB persistence to perform snapshots (see Snapshotting section)
      appendonly: "yes"                    # Enable AOF persistence
      
```

#### Redis as a Cache
```yaml
redis:
  configuration:
    valueMap:
      maxmemory-policy: "volatile-lfu"      # Expire keys based on least frequently used policy
      maxmemory: "10gb"
      save: ""                              # Disable saving snapshots of the database to disk
      lazyfree-lazy-eviction: "yes"         # Asynchronously evict keys
      lazyfree-lazy-expire: "yes"           # Asynchronously delete expired keys
      lazyfree-lazy-server-del: "yes"       # Asynchronously delete keys during specific server commands
      replica-lazy-flush: "yes"             # Asynchronously flush keys after replica resynchronization
      cluster-require-full-coverage: "no"   # Accept queries even when only part of key space is covered
      cluster-allow-reads-when-down: "yes"  # Allow nodes to serve reads while cluster is down
```