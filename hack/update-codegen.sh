#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

#	`go get sigs.k8s.io/controller-tools/cmd/controller-gen@v0.2.5` to install the tool
controller-gen object:headerFile=./hack/custom-boilerplate.go.txt paths="github.com/IBM/operator-for-redis-cluster/pkg/..."

# old code generator command
#./vendor/k8s.io/code-generator/generate-groups.sh all github.com/IBM/operator-for-redis-cluster/pkg/client github.com/IBM/operator-for-redis-cluster/pkg/api redis:v1 --go-header-file ./hack/custom-boilerplate.go.txt
