# Try {{product-name}}

This section describes various installation options of the {{product-name}}.

## Using Docker

For debugging or testing purposes, it is possible to run [Docker](https://docs.docker.com/get-docker/)-container {{product-name}}.
The code for cluster deployment is available [by the link](https://github.com/ytsaurus/ytsaurus/tree/main/yt/docker/local).

### Building Docker-image

1. Build binary file ytserver-all.
2. Launch `./build.sh --ytserver-all <YTSERVER_ALL_PATH>`.
3. Launch `./run_local_cluster.sh --yt-skip-pull true`.

### Starting a local cluster

To start a local cluster, run the command:
```
./run_local_cluster.sh
```

## Demo Stand

A stand is available to demonstrate the capabilities of the {{product-name}} system.
Go to [link](https://ytsaurus.tech/#demo ) to get access to it.

## Kubernetes

Currently, a version of {{product-name}} is available for self-installation without support for the SQL query language. The corresponding code will be made freely available in the near future.

To deploy {{product-name}} in Kubernetes, it is recommended to use the [operator](https://github.com/ytsaurus/yt-k8s-operator). Ready-made docker images with operator, UI, server components and examples can be found on [dockerhub](https://hub.docker.com/u/ytsaurus).

### Deployment in a Kubernetes cluster

This section describes the installation of {{product-name}} in a Kubernetes cluster with support for dynamic creation of volumes, for example in Managed Kubernetes in Yandex.Cloud. It is assumed that you have the kubectl utility installed and configured. For successful deployment of {{product-name}}, there must be at least three nodes in the Kubernetes cluster, of the following configuration: at least 4 CPU cores and 8 GB RAM.

#### Installing the operator

1. Install the [helm utility](https://help.sh/docs/intro/install/).
2. Download the chart `helm pull oci://docker.io/ytsaurus/top-chart --version 0.1.6 --untar'.
3. Install the `helm install ytsaurus ytop-chart/` operator.
4. Check the result:

```
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-5765c5f995-dntph   2/2     Running    0          7m57s
```

#### Starting {{product-name}} cluster

Create a namespace to run the cluster. Create a secret containing the username, password, and token of the cluster administrator.
```
kubectl create namespace <namespace>
kubectl create secret generic ytadminsec --from-literal=login=admin --from-literal=password=<password> --from-literal=token=<password>  -n <namespace>
```

Download [specification](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/cluster_v1_demo_without_yql.yaml) , correct as necessary and upload to the cluster `kubectl apply -f cluster_v1_demo_without_yql.yaml -n <namespace>`.

It is necessary to specify guarantees or resource limits in the `execNodes` section, the specified values will be reflected in the node configuration, and will be visible to the scheduler. For reliable data storage, be sure to allocate persistent volumes.

To access the {{product-name}} UI, you can use the LoadBalancer service type or configure the load balancer separately to service HTTP requests. Currently, the {{product-name}} UI does not have the built-in HTTPS support.

To run applications using a cluster, use the same Kubernetes cluster. As the cluster address, substitute the address of the http proxy service - `http-proxies.<namespace>.svc.cluster.local`.

### Minikube

You need 150 GB of disk space and at least 8 cores on the host for the cluster to work correctly.

#### Installing Minikube

Minikube installation guide is available via [link](https://kubernetes.io/ru/docs/tasks/tools/install-minikube/).

Prerequisites:
1. Install [Docker](https://docs.docker.com/engine/install/);
2. Install [kubectl](https://kubernetes.io/ru/docs/tasks/tools/install-kubectl/#установка-kubectl-в-linux);
3. Install [Minikube](https://kubernetes.io/ru/docs/tasks/tools/install-minikube/);
4. Run the command `minikube start --driver=docker`.

As a result, the `kubectl cluster-info` command should be executed successfully.

#### Installing the operator

1. Install the [helm utility](https://help.sh/docs/intro/install/).
2. Download the chart `helm pull oci://docker.io/ytsaurus/top-chart --version 0.1.6 --untar'.
3. Install the `helm install ytsaurus ytop-chart/` operator.
4. Check the result:

```
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-5765c5f995-dntph   2/2     Running    0          7m57s
```

#### Starting {{product-name}} cluster

Download [specification](https://github.com/ytsaurus/yt-k8s-operator/blog/main/config/samples/clusters_v1_mini cube_without_yql.yaml) to the cluster via `kubectl apply -f cluster_v1_minikube_without_yql.yaml`.

If the download was successful, after a while the list of running hearths will look like this:

```
$ kubectl get pod
NAME                                      READY   STATUS      RESTARTS   AGE
m-0                                       1/1     Running     0          2m16s
s-0                                       1/1     Running     0          2m11s
ca-0                                      1/1     Running     0          2m11s
dn-0                                      1/1     Running     0          2m11s
dn-1                                      1/1     Running     0          2m11s
dn-2                                      1/1     Running     0          2m11s
en-0                                      1/1     Running     0          2m11s
ytsaurus-ui-deployment-67db6cc9b6-nwq25   1/1     Running     0          2m11s
...
```

Configure network access to the web interface and proxy
```bash
$ minikube service ytsaurus-ui --url
http://192.168.49.2:30539

$ minikube service http-proxies --url
http://192.168.49.2:30228
```

The web interface will be available at the first link. To log in, use:
```
Login: admin
Password: password
```

The second link allows you to connect to the cluster from the command line and python client:
```bash
export YT_CONFIG_PATCHES='{proxy={enable_proxy_discovery=%false}}' 
export YT_TOKEN=password
export YT_PROXY=192.168.49.2:30228

echo '{a=b}' | yt write-table //home/t1 --format yson
yt map cat --src //home/t1 --dst //home/t2 --format json 
```

### Deleting a cluster

To delete a {{product-name}} cluser, run the command:
```
kubectl delete -f cluster_v1_minikube_without_yql.yaml
```
