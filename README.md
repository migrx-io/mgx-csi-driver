# MGX CSI Driver for Kubernetes

A [Container Storage Interface (CSI)](https://github.com/container-storage-interface/spec)
driver that provisions block storage from an MGX storage cluster and exposes it
to Kubernetes workloads over **NVMe-oF**. Volumes are thin-provisioned,
cache-accelerated, and support online expansion, QoS limits, encryption,
compression, and S3-backed snapshots.

Driver name (CSI provisioner): **`csi.migrx.io`**.

---

## Contents

- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
  - [Provision a volume](#provision-a-volume)
  - [Expand a volume](#expand-a-volume)
  - [Snapshot a volume](#snapshot-a-volume)
  - [Restore from a snapshot](#restore-from-a-snapshot)
- [Configuration](#configuration)
  - [Helm values](#helm-values)
  - [StorageClass parameters](#storageclass-parameters)
  - [VolumeSnapshotClass parameters](#volumesnapshotclass-parameters)
  - [Node NVMe-oF tuning](#node-nvme-of-tuning)
- [Operations](#operations)
- [Development](#development)

---

## Architecture

The driver runs two components, both deployed by the Helm chart:

| Component | Workload | Role |
| --- | --- | --- |
| **Controller** | `StatefulSet` (`mgxcsi-controller`) | Talks to the MGX management API (MGM) to create/delete/expand volumes and snapshots. Bundles the upstream `csi-provisioner`, `csi-attacher`, `csi-resizer`, and `csi-snapshotter` sidecars. Runs a background reconciler that drives volumes to the `READY` state. |
| **Node** | `DaemonSet` (`mgxcsi-node`) | Runs on every node. Connects/disconnects NVMe-oF targets, formats and mounts the device into the pod. Bundles `csi-node-driver-registrar`. |

Data path: a workload's PVC → controller asks MGM to carve a volume out of the
storage pool → node connects the NVMe-oF target exposed by the MGX/SPDK backend
→ kubelet mounts the filesystem into the pod.

```
  ┌──────────────┐   CSI gRPC    ┌────────────────────┐   MGM API    ┌──────────────┐
  │  kube-api /  │──────────────▶│  mgxcsi-controller │─────────────▶│  MGX cluster │
  │  sidecars    │               │  (provision/snap)  │   :8082      │  (SPDK)      │
  └──────────────┘               └────────────────────┘              └──────┬───────┘
                                                                            │ NVMe-oF
  ┌──────────────┐   CSI gRPC    ┌────────────────────┐                     │
  │   kubelet    │──────────────▶│   mgxcsi-node      │◀────────────────────┘
  │   (mount)    │               │  (connect/mount)   │   /dev/nvmeXnY
  └──────────────┘               └────────────────────┘
```

## Features

- **Dynamic provisioning** — `CreateVolume` / `DeleteVolume`.
- **Online expansion** — `allowVolumeExpansion: true`; grow a PVC with no downtime.
- **Snapshots** — full + incremental backups to S3, restorable as new volumes.
- **Restore from snapshot** — provision a new PVC from a `VolumeSnapshot`.
- **Read/write caching** — configurable min/max/ratio cache sizing per StorageClass.
- **QoS limits** — per-volume read/write MB/s and IOPS caps.
- **Encryption & compression** — optional, set per StorageClass.
- **Volume health** — reports `VolumeCondition` via the CSI `GetVolume` capability.

**Access modes:** `ReadWriteOnce` only (`SINGLE_NODE_WRITER` /
`SINGLE_NODE_SINGLE_WRITER`). The driver is block-storage / NVMe-oF based and
does not support multi-node read-write.

## Prerequisites

- A running **MGX storage cluster** reachable from the Kubernetes nodes, with the
  MGM management API exposed (default port `8082`) and API credentials.
- Worker nodes with the **`nvme-cli`** stack and the kernel NVMe-oF/TCP modules
  available (the node image already ships `nvme-cli`, `e2fsprogs`, `xfsprogs`,
  `util-linux`).
- Kubernetes **1.20+** with the CSI feature set enabled.
- [Helm 3](https://helm.sh/docs/intro/quickstart/#install-helm).
- For snapshots: the **external-snapshotter CRDs** and a snapshot controller must
  be present in the cluster (see [Installation](#installation)).

## Installation

### 1. Install the snapshot CRDs (once per cluster)

Skip this if your cluster already has the `snapshot.storage.k8s.io` CRDs and a
snapshot controller (many managed clusters do).

```sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml
```

If the cluster has **no** snapshot controller of its own, let the chart deploy
one with `--set externalSnapshotter.enabled=true` in the next step.

### 2. Install the driver via Helm

From the published OCI chart:

```sh
helm install mgx-csi-driver oci://docker.io/migrx/mgx-csi-driver \
  --namespace mgx-system --create-namespace \
  --version 0.1.0 \
  --set csiSecret.clusterConfig.nodes='{172.31.96.14:8082,172.31.96.15:8082,172.31.96.16:8082}' \
  --set csiSecret.clusterConfig.username=admin \
  --set csiSecret.clusterConfig.password=secret \
  --set externalSnapshotter.enabled=true
```

### 3. Verify

```sh
kubectl get pods -n mgx-system
kubectl get storageclass mgxcsi-sc
kubectl get volumesnapshotclass mgxcsi-snapshotclass
```

You should see the `mgxcsi-controller-0` pod and one `mgxcsi-node-*` pod per
node, all `Running`.

### Uninstall

```sh
helm uninstall mgx-csi-driver -n mgx-system
kubectl delete namespace mgx-system
```

## Usage

The chart creates a `StorageClass` named **`mgxcsi-sc`** and a
`VolumeSnapshotClass` named **`mgxcsi-snapshotclass`** by default.

### Provision a volume

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: mgxcsi-sc
  resources:
    requests:
      storage: 100Gi
```

```sh
kubectl apply -f pvc.yaml
kubectl get pvc data -w        # waits for Bound
```

Mount it in a pod like any other PVC:

```yaml
    volumeMounts:
      - { name: data, mountPath: /data }
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: data
```

### Snapshot a volume

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: data-snap
spec:
  volumeSnapshotClassName: mgxcsi-snapshotclass
  source:
    persistentVolumeClaimName: data
```

```sh
kubectl apply -f snapshot.yaml
kubectl get volumesnapshot data-snap -w   # waits for READYTOUSE=true
```

With `incremental: "yes"` (the default) each snapshot is an independently
restorable point in time. With `"no"` snapshots overwrite a single rolling
latest-only backup and point-in-time restore is not possible.

### Restore from a snapshot

Create a new PVC whose `dataSource` is the snapshot:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-restored
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: mgxcsi-sc
  resources:
    requests:
      storage: 100Gi
  dataSource:
    name: data-snap
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
```

The controller drives the restore (the snapshot plugin provisions the new volume
on copy completion) and the PVC binds once the volume reaches `READY`.

## Configuration

### Helm values

Connection to the MGX cluster is supplied through `csiSecret.clusterConfig`,
which is rendered into a `mgxcsi-secret` Secret:

| Value | Default | Description |
| --- | --- | --- |
| `csiSecret.clusterConfig.protocol` | `http` | MGM API protocol. |
| `csiSecret.clusterConfig.nodes` | `[localhost:8082]` | MGM API endpoints (`host:port`). |
| `csiSecret.clusterConfig.cluster` | `main` | Target cluster name. |
| `csiSecret.clusterConfig.ns` | `main` | Target namespace within the cluster. |
| `csiSecret.clusterConfig.username` / `password` | `""` | API credentials. |
| `controller.replicas` | `1` | Controller StatefulSet replicas. |
| `controller.timeoutVolumeCheck` | `2` | Interval (minutes) at which the reconciler scans all PVs for idle volumes. `0` disables the reconciler. |
| `controller.idleVolumeMin` | `10` | Minutes a volume may stay unattached (used by no pod) before the reconciler idles/stops it on the backend. |
| `storageclass.create` | `true` | Create the `mgxcsi-sc` StorageClass. |
| `volumeSnapshotClass.create` | `true` | Create the `mgxcsi-snapshotclass`. |
| `externalSnapshotter.enabled` | `false` | Deploy a snapshot-controller (only if the cluster has none). |
| `externalSnapshotter.customResourceDefinitions.enabled` | `false` | Let the chart create the snapshot CRDs. |
| `serviceAccount.create` / `rbac.create` | `true` | Create SA and RBAC. |

> If your cluster already runs a snapshot controller, leave
> `externalSnapshotter.enabled=false` — a single snapshot controller serves all
> CSI drivers in a cluster.

See [`charts/mgx-csi-driver/values.yaml`](charts/mgx-csi-driver/values.yaml)
for the full, commented list.

### StorageClass parameters

Set these via `--set storageclass.<key>=...` at install time, or author your own
StorageClass with these `parameters`:

| Helm value | SC parameter | Default | Description |
| --- | --- | --- | --- |
| `config` | `config` | _(server default)_ | Storage config profile name. Leave unset to let the pool default it. |
| `labels` | `labels` | `storage=mgx` | Custom labels applied to the volume. |
| `minCacheRCacheSize` | `min_cache_r_cache_size` | `1024` | Min read-cache size (MB). |
| `minCacheRWCacheSize` | `min_cache_rw_cache_size` | `1024` | Min write-cache size (MB). |
| `maxCacheRCacheSize` | `max_cache_r_cache_size` | `20480` | Max read-cache size (MB). |
| `maxCacheRWCacheSize` | `max_cache_rw_cache_size` | `3072` | Max write-cache size (MB). |
| `ratioCacheRCacheSize` | `ratio_cache_r_cache_size` | `0.1` | Read-cache size as a ratio of volume size. |
| `ratioCacheRWCacheSize` | `ratio_cache_rw_cache_size` | `0.05` | Write-cache size as a ratio of volume size. |
| `qosRMBPerSec` | `qos_r_mbytes_per_sec` | _(unset)_ | Read bandwidth cap (MB/s). |
| `qosWMBPerSec` | `qos_w_mbytes_per_sec` | _(unset)_ | Write bandwidth cap (MB/s). |
| `qosRWMBPerSec` | `qos_rw_mbytes_per_sec` | _(unset)_ | Combined R/W bandwidth cap (MB/s). |
| `qosRWIOPS` | `qos_rw_ios_per_sec` | _(unset)_ | Combined R/W IOPS cap. |
| `storageEncryptSecret` | `storage_encrypt_secret` | _(unset)_ | Name of the encryption secret. |
| `storageCompress` | `storage_compress` | _(unset)_ | Compression level, `0`–`9`. |
| `reclaimPolicy` | — | `Delete` | `Delete` or `Retain`. |

The StorageClass is created with `volumeBindingMode: Immediate` and
`allowVolumeExpansion: true`.

### VolumeSnapshotClass parameters

| Helm value | Parameter | Default | Description |
| --- | --- | --- | --- |
| `config` | `config` | _(server default)_ | Snapshot plugin config profile name. |
| `incremental` | `incremental` | `yes` | `yes` keeps full + incremental history (restorable PITs); `no` keeps a single rolling latest-only backup. |
| `storageClass` | `storage_class` | _(bucket default)_ | Destination S3 storage class (`STANDARD`, `STANDARD_IA`, `GLACIER`, `DEEP_ARCHIVE`, …). |
| `labels` | `labels` | _(unset)_ | Per-snapshot labels. |
| `deletionPolicy` | — | `Delete` | `Delete` or `Retain` — fate of the backup when the VolumeSnapshot is removed. |
| `isDefault` | — | `false` | Mark as the cluster default VolumeSnapshotClass. |

### Node NVMe-oF tuning

The node DaemonSet exposes NVMe-oF connection tuning under `node.*` in values.
Defaults are sensible; override only when diagnosing connection or teardown
behavior:

| Value | Default | Description |
| --- | --- | --- |
| `node.nrIoQueues` | `2` | NVMe-oF I/O queues. |
| `node.queueSize` | `16` | NVMe-oF queue depth. |
| `node.fastIoFailTmo` | `0` | Seconds to queue I/O on a lost controller before failing fast (`0` = immediate). |
| `node.ctrlLossTmo` | `10` | Seconds to retry reconnect before removing the controller (≥ `fastIoFailTmo`). |
| `node.reconnectDelay` | `2` | Pause between reconnect attempts (s). |
| `node.keepAliveTmo` | `5` | Keep-alive timeout (s). |
| `node.nvmeTimeout` | `30` | Deadline for `nvme connect`/`disconnect` and post-disconnect cleanup waits (s). |
| `node.mkfsFsckTimeout` | `120` | Per-command deadline for `fsck`/`mkfs`/`mount` (s). |
| `node.volumeCleanEnabled` | `false` | Call `storage.volume_clean` on unpublish and wait for `READY`. |
| `node.volumeCleanPollInterval` | `2` | Gap between `volume_get` probes while waiting for clean (s). |
| `node.volumeCleanReadyTimeout` | `60` | Total budget waiting for `READY` after clean (s). |
| `node.volumeCleanFstrimTimeout` | `30` | `fstrim` timeout forwarded to `volume_clean` (s). |

## Operations

Useful logs:

```sh
# controller (provisioning, snapshots, reconcile)
kubectl logs -f -n mgx-system mgxcsi-controller-0 -c mgxcsi-controller

# node (NVMe-oF connect, mount) — pick the pod on the node hosting the workload
kubectl logs -f -n mgx-system <mgxcsi-node-pod> -c mgxcsi-node
```

Common checks:

```sh
kubectl get pvc,pv
kubectl get volumesnapshot,volumesnapshotcontent
kubectl describe pvc <name>     # events show provisioning / attach errors
```

## Development

The repo ships a Makefile for the common loops:

```sh
make build            # build the mgxcsi binary into ./bin
make test             # go mod verify + unit tests (race + cover)
make lint             # golangci-lint
make docker-build     # build the container image
make docker-buildx    # multi-arch build + push
make helm-buildx      # package + push the Helm chart to the OCI registry
```

Local end-to-end against a [kind](https://kind.sigs.k8s.io/) cluster:

```sh
make kind-create      # 3-worker kind cluster (e2e/kind-config.yaml)
make kind-run         # build image, load into kind, helm install
make e2e-test         # run the e2e suite
make kind-delete
```

Override the registry/tag or MGM endpoint as needed:

```sh
make docker-build CSI_IMAGE_REGISTRY=myrepo CSI_IMAGE_TAG=dev
make helm-install MGM_ENDPOINT=10.0.0.5 MGM_API_USERNAME=admin MGM_API_PASSWD=secret
```

### Driver flags

The driver binary (`mgxcsi`) is started by the chart with `--controller` or
`--node`. Key flags (see [`cmd/main.go`](cmd/main.go) for the full list):

| Flag | Default | Description |
| --- | --- | --- |
| `--drivername` | `csi.migrx.io` | CSI driver name. |
| `--endpoint` | `unix://tmp/mgxcsi.sock` | CSI gRPC endpoint. |
| `--nodeid` | _(required for node)_ | Node identifier. |
| `--controller` / `--node` | `false` | Which server(s) to run. |
| `--timeout-volume-check` | `2` | Reconciler scan interval (minutes); `0` disables it. |
| `--idle-volume-min` | `10` | Minutes unattached before a volume is idled/stopped. |

NVMe-oF and volume-clean flags mirror the `node.*` Helm values above.
