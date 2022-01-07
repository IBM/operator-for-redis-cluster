---
title: Contributing
slug: /contributing
---

# Contributing

## Set up your machine

Refer to our [cookbook](cookbook.md#installation) to learn how to set up your machine.

## Development process

This section assumes you have already set up your environment to build and install the Redis operator and cluster.

### Create a branch

The first step to contributing is creating a branch off of the `main` branch in your forked project. Branch names should be well formatted. Start your branch name with a type. Choose one of the following:
`feat`, `fix`, `bug`, `docs`, `style`, `refactor`, `perf`, `test`, `add`, `remove`, `move`, `bump`, `update`, `release`

Example:
```console
$ git checkout -b feat/node-scaling
```
### Commit your code

Make your desired changes to the branch and then commit your work:
```console
$ git add .
$ git commit -m "<USEFUL_MESSAGE>"
$ git push --set-upstream origin <BRANCH_NAME>
```

When you are ready to make a pull request, we suggest you run:

```console
$ make generate
//path/to/go/bin/controller-gen object paths="./..."
$ make fmt
find . -name '*.go' -not -wholename './vendor/*' | while read -r file; do gofmt -w -s "$file"; goimports -w "$file"; done
$ make lint
golangci-lint run --enable exportloopref
$ make test
./go.test.sh
...
```

These steps will:
1. Regenerate the RedisCluster CRD
2. Format the code according to `gofmt` standards
3. Run the linter
4. Run the unit tests

### End-to-end tests

To run the end-to-end tests, you need to have a running Kubernetes cluster. Follow the steps in the [cookbook](cookbook.md#run-end-to-end-tests).

### Submit a pull request

Push your branch to your `redis-operator` fork and open a pull request against the `main` branch in the official project. When you open a PR, be sure to include a description explaining your changes, as well as
```text
Resolves #<ISSUE_NUMBER>
```
We also ask that you add labels describing the t-shirt size of the task (S, M, L, XL) and the task type (enhancement, documentation, bug, etc.).
