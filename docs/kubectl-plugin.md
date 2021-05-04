# Kubectl plugin

The Redis-operator kubectl plugin helps you visualise the status of your Redis-Cluster running in your cluster.
Please visit the [official documentation](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/) for more details.

## installation

By default, the plugin will install in ```~/.kube/plugins```. After it installs, run: ```make plugin```

If you want to install in another path you can run:

```shell
make plugin
```