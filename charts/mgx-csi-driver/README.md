# Installation with Helm 3

Follow this guide to install the SPDK-CSI Driver for Kubernetes.

## Prerequisites

### [Install Helm](https://helm.sh/docs/intro/quickstart/#install-helm)

## Install latest CSI Driver via `helm install`

```console
cd charts
helm install mgx-csi-driver ./spdk-csi --namespace mgx-csi-driver
```

## After installation succeeds, you can get a status of Chart

```console
helm status "mgx-csi-driver"
```

## Delete Chart

If you want to delete your Chart, use this command

```bash
helm uninstall "mgx-csi-driver" --namespace "mgx-csi-driver"
```

If you want to delete the namespace, use this command

```bash
kubectl delete namespace mgx-csi-driver
```
