#!/usr/bin/env bash

KUBE_CONFIG_PATH=$HOME"/.kube"
if [ -n "$1" ]
then KUBE_CONFIG_PATH=$1
fi

REDIS_PLUGIN_BIN_NAME="kubectl-rc"
REDIS_PLUGIN_PATH=$KUBE_CONFIG_PATH/plugins/rediscluster

if [[ ! -d $REDIS_PLUGIN_PATH ]]; then
  mkdir -p $REDIS_PLUGIN_PATH
fi

GIT_ROOT=$(git rev-parse --show-toplevel)
cp $GIT_ROOT/bin/$REDIS_PLUGIN_BIN_NAME $REDIS_PLUGIN_PATH/$REDIS_PLUGIN_BIN_NAME

cat > $REDIS_PLUGIN_PATH/plugin.yaml << EOF1
name: "rediscluster"
shortDesc: "RedisCluster shows redis cluster resources"
longDesc: >
  RedisCluster shows redis cluster resources
command: ./$REDIS_PLUGIN_BIN_NAME
flags:
- name: "rc"
  shorthand: "r"
  desc: "Cluster name"
  defValue: ""
EOF1
