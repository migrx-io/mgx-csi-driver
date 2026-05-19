package mgx

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
	mount "k8s.io/mount-utils"
	"k8s.io/utils/exec"

	csicommon "github.com/migrx-io/mgx-csi-driver/pkg/csi-common"
	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

type nodeServer struct {
	*csicommon.DefaultNodeServer
	mounter     mount.Interface
	volumeLocks *util.VolumeLocks
	conf        *util.Config
}

func newNodeServer(d *csicommon.CSIDriver, conf *util.Config) *nodeServer {
	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
		mounter:           mount.New(""),
		volumeLocks:       util.NewVolumeLocks(),
		conf:              conf,
	}
}

func (ns *nodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.Driver.GetNodeID(),
	}, nil
}

// NodePublishVolume does the full per-pod setup in one step: tear down any
// prior mount/connection for this volume on this node, then connect NVMe
// and mount the device directly at the kubelet target path. The driver
// does not advertise STAGE_UNSTAGE_VOLUME, so there is no shared staging
// mount to inherit stale or broken state from a previous pod.
//
// Rolling-update overlap is handled by the otherPodMounts check: if another
// pod on this node still has the volume mounted (kubelet ordered our
// Publish before its Unpublish), we return Aborted so kubelet retries.
// On retry, the outgoing pod's NodeUnpublishVolume has freed the device
// and we proceed without ever yanking I/O out from under the live pod.
func (ns *nodeServer) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	unlock := ns.volumeLocks.Lock(volumeID)
	defer unlock()

	vc := req.GetVolumeCapability()
	mode := vc.GetAccessMode().GetMode()
	if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
		mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
		return nil, status.Error(codes.InvalidArgument,
			"Only ReadWriteOnce (RWO) and ReadWriteOncePod (RWOP) volumes are supported by this driver")
	}

	targetPath := req.GetTargetPath()
	targetParentPath := filepath.Dir(targetPath)
	volumeContext := req.GetVolumeContext()

	klog.Infof("NodePublishVolume: start, volumeID: %s targetPath: %s", volumeID, targetPath)

	others, oerr := ns.otherPodMounts(targetPath, volumeContext["name"])
	if oerr != nil {
		klog.Warningf("NodePublishVolume: otherPodMounts probe failed (continuing): %v", oerr)
	}
	if len(others) > 0 {
		klog.Infof("NodePublishVolume: volume %s still mounted by other pod(s) %v, returning Aborted for kubelet retry", volumeID, others)
		return nil, status.Errorf(codes.Aborted,
			"volume %s currently mounted by other pod(s) %v on this node; retry after their NodeUnpublishVolume completes",
			volumeID, others)
	}

	initiator, err := util.NewMGXCsiInitiator(volumeContext, ns.conf)
	if err != nil {
		klog.Errorf("failed to create mgx initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Tear down anything left over from a previous Publish attempt on this
	// target (e.g. kubelet retried after our gRPC timed out). Each step is
	// idempotent — on the common first-Publish case all three are no-ops.
	if err = ns.deleteMountPoint(targetPath); err != nil {
		klog.Errorf("failed to unmount target before re-publish, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if derr := initiator.Disconnect(); derr != nil {
		klog.Warningf("NodePublishVolume: pre-connect disconnect failed (continuing): %v", derr)
	}

	klog.Infof("NodePublishVolume: connecting NVMe target, volumeID: %s", volumeID)
	devicePath, err := initiator.Connect(ns.conf.NrIoQueues, ns.conf.QueueSize)
	if err != nil {
		klog.Errorf("failed to connect initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("NodePublishVolume: NVMe connected, volumeID: %s devicePath: %s", volumeID, devicePath)
	defer func() {
		if err != nil {
			initiator.Disconnect() //nolint:errcheck // best-effort rollback after publish failure; surfaced error already logged
		}
	}()

	if _, err = ns.createMountPoint(targetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	mntFlags := vc.GetMount().GetMountFlags()
	klog.Infof("NodePublishVolume: formatting+mounting, volumeID: %s device: %s -> %s flags: %v", volumeID, devicePath, targetPath, mntFlags)
	sfMounter := mount.SafeFormatAndMount{
		Interface: ns.mounter,
		Exec:      util.NewTimeoutExec(exec.New(), time.Duration(ns.conf.MkfsFsckTimeoutSec)*time.Second),
	}
	if err = sfMounter.FormatAndMount(devicePath, targetPath, "ext4", mntFlags); err != nil {
		klog.Errorf("failed to format and mount device, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("NodePublishVolume: mounted, volumeID: %s device: %s -> %s", volumeID, devicePath, targetPath)

	// Stash devicePath + nqn under the per-pod parent so NodeUnpublishVolume
	// (which receives only volume_id + target_path) and NodeExpandVolume
	// (which receives volume_path) can recover the backing device without
	// re-querying the backend.
	volumeContext["devicePath"] = devicePath
	if err = util.StashVolumeContext(volumeContext, targetParentPath); err != nil {
		klog.Errorf("failed to stash volume context, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodePublishVolume: success, volumeID: %s targetPath: %s devicePath: %s", volumeID, targetPath, devicePath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume tears down the per-pod mount and, when this is the
// last pod on this node using the volume, disconnects the NVMe controller
// and runs volume_clean on the backend. The node is the only place clean
// runs — ControllerUnpublishVolume is a no-op — so every pod restart
// (same-node or cross-node) goes through a clean before the next attach.
// The cross-node race against in-flight clean is closed by
// ControllerPublishVolume returning Aborted while the volume isn't READY.
func (ns *nodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	unlock := ns.volumeLocks.Lock(volumeID)
	defer unlock()

	targetPath := req.GetTargetPath()
	targetParentPath := filepath.Dir(targetPath)

	klog.Infof("NodeUnpublishVolume: start, volumeID: %s targetPath: %s", volumeID, targetPath)

	volumeContext, ctxErr := util.LookupVolumeContext(targetParentPath)
	if ctxErr != nil {
		klog.Warningf("no volume context for volume %s (idempotent unpublish?): %v", volumeID, ctxErr)
	}

	klog.Infof("NodeUnpublishVolume: unmounting, volumeID: %s targetPath: %s", volumeID, targetPath)
	if err := ns.deleteMountPoint(targetPath); err != nil {
		klog.Errorf("failed to unmount target path, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("NodeUnpublishVolume: unmounted, volumeID: %s targetPath: %s", volumeID, targetPath)

	if volumeContext == nil {
		klog.Infof("NodeUnpublishVolume: no stashed context, idempotent return, volumeID: %s", volumeID)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Ref-count guard: if any other pod on this node still has the
	// volume's device mounted, we must not disconnect the NVMe controller
	// or call volume_clean — both would yank I/O out from under the live
	// pod. The last Unpublish (no peers left) does the teardown.
	others, oerr := ns.otherPodMounts(targetPath, volumeContext["name"])
	if oerr != nil {
		klog.Warningf("NodeUnpublishVolume: otherPodMounts probe failed (assuming no peers): %v", oerr)
	}
	if len(others) > 0 {
		klog.Infof("NodeUnpublishVolume: %d other pod mount(s) still using volume %s, skipping NVMe disconnect and volume_clean: %v", len(others), volumeID, others)
		if err := util.CleanUpVolumeContext(targetParentPath); err != nil {
			klog.Errorf("failed to clean up volume context, volumeID: %s err: %v", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("NodeUnpublishVolume: success (peers remain), volumeID: %s targetPath: %s", volumeID, targetPath)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	initiator, err := util.NewMGXCsiInitiator(volumeContext, ns.conf)
	if err != nil {
		klog.Errorf("failed to create mgx initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("NodeUnpublishVolume: disconnecting NVMe target, volumeID: %s", volumeID)
	if err = initiator.Disconnect(); err != nil {
		klog.Errorf("failed to disconnect initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("NodeUnpublishVolume: NVMe disconnected, volumeID: %s", volumeID)

	if err = util.CleanUpVolumeContext(targetParentPath); err != nil {
		klog.Errorf("failed to clean up volume context, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := ns.cleanVolume(volumeID); err != nil {
		klog.Errorf("volume_clean cycle failed, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodeUnpublishVolume: success, volumeID: %s targetPath: %s", volumeID, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// cleanVolume asks the storage backend to clean the volume and waits for
// READY. No-op when VolumeCleanEnabled is false on the node config.
func (ns *nodeServer) cleanVolume(volumeID string) error {
	if !ns.conf.VolumeCleanEnabled {
		klog.Infof("NodeUnpublishVolume: volume_clean disabled, skipping, volumeID: %s", volumeID)
		return nil
	}
	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return fmt.Errorf("init mgx client: %w", err)
	}
	klog.Infof("NodeUnpublishVolume: calling volume_clean, volumeID: %s", volumeID)
	if err := mgxClient.CleanVolume(volumeID, ns.conf.VolumeCleanFstrimTimeoutSec); err != nil {
		return fmt.Errorf("volume_clean: %w", err)
	}
	return waitVolumeReady(volumeID, mgxClient, ns.conf)
}

// otherPodMounts lists kubelet pod target paths (other than targetPath)
// that have the same volume's block device mounted. Used to detect a
// rolling-update overlap where kubelet has issued Publish(new) before
// Unpublish(old) — the new Publish must back off until the old pod's
// mount is gone.
//
// Identification is by block-device path: the NQN resolves to a single
// /dev/disk/by-id symlink, so any /proc/mounts entry whose device matches
// is for this volume. Restricting hits to the kubelet pods dir avoids
// false positives from non-pod mounts of the same device.
func (ns *nodeServer) otherPodMounts(targetPath, volName string) ([]string, error) {
	if volName == "" {
		return nil, nil
	}
	matches, err := filepath.Glob(fmt.Sprintf("/dev/disk/by-id/*%s*", volName))
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		// No /dev symlink means no NVMe controller is connected for this
		// volume, so there can't be any pod mounts of it.
		return nil, nil
	}
	devicePath, err := filepath.EvalSymlinks(matches[0])
	if err != nil {
		return nil, err
	}
	mps, err := ns.mounter.List()
	if err != nil {
		return nil, err
	}
	podsDir := kubeletPodsDir(targetPath)
	var others []string
	for i := range mps {
		if mps[i].Device != devicePath {
			continue
		}
		if mps[i].Path == targetPath {
			continue
		}
		if podsDir != "" && strings.HasPrefix(mps[i].Path, podsDir) {
			others = append(others, mps[i].Path)
		}
	}
	return others, nil
}

// kubeletPodsDir returns the kubelet per-pod root (e.g. /var/lib/kubelet/pods/)
// derived from the CSI targetPath, which kubelet always shapes as
// <root>/pods/<podUID>/volumes/kubernetes.io~csi/<pv>/mount. Deriving the
// prefix instead of hard-coding it lets the driver work on clusters that
// run kubelet with a custom --root-dir. Returns "" if targetPath doesn't
// contain the expected "/pods/" segment.
func kubeletPodsDir(targetPath string) string {
	const segment = "/pods/"
	idx := strings.Index(targetPath, segment)
	if idx < 0 {
		return ""
	}
	return targetPath[:idx+len(segment)]
}

func (*nodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_VOLUME_CONDITION,
					},
				},
			},
		},
	}, nil
}

func (*nodeServer) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	volumeMountPath := req.GetVolumePath()
	targetParentPath := filepath.Dir(volumeMountPath)

	volumeContext, err := util.LookupVolumeContext(targetParentPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve volume context for volume %s: %v", volumeID, err)
	}

	devicePath, ok := volumeContext["devicePath"]
	if !ok || devicePath == "" {
		return nil, status.Errorf(codes.Internal, "could not find device path for volume %s", volumeID)
	}

	resizer := mount.NewResizeFs(exec.New())
	needsResize, err := resizer.NeedResize(devicePath, volumeMountPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if volume %s needs resizing: %v", volumeID, err)
	}

	if needsResize {
		resized, err := resizer.Resize(devicePath, volumeMountPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to resize volume %s: %v", volumeID, err)
		}
		if resized {
			klog.Infof("Successfully resized volume %s (device: %s, mount path: %s)", volumeID, devicePath, volumeMountPath)
		} else {
			klog.Warningf("Volume %s did not require resizing", volumeID)
		}
	}

	return &csi.NodeExpandVolumeResponse{}, nil
}

func (ns *nodeServer) createMountPoint(path string) (bool, error) {
	isMount, err := ns.mounter.IsMountPoint(path)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(path, 0o755); mkErr != nil {
				return false, mkErr
			}
			klog.Infof("Created mount point path: %s", path)
			return false, nil
		}
		klog.Errorf("Error checking mount point %s: %v", path, err)
		return false, err
	}

	if isMount {
		klog.Infof("%s already mounted", path)
		return true, nil
	}

	return false, nil
}

// unmount and delete mount point, must be idempotent
func (ns *nodeServer) deleteMountPoint(path string) error {
	isMount, err := ns.mounter.IsMountPoint(path)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Infof("%s already deleted", path)
			return nil
		}
		klog.Errorf("Error checking mount point %s: %v", path, err)
		return err
	}

	if isMount {
		if err := ns.mounter.Unmount(path); err != nil {
			return fmt.Errorf("unmount %s: %w", path, err)
		}
	}
	return os.RemoveAll(path)
}
