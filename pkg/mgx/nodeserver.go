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
	ns := &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
		mounter:           mount.New(""),
		volumeLocks:       util.NewVolumeLocks(),
		conf:              conf,
	}

	return ns
}

func (ns *nodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	response := &csi.NodeGetInfoResponse{
		NodeId: ns.Driver.GetNodeID(),
	}

	return response, nil
}

func (ns *nodeServer) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	unlock := ns.volumeLocks.Lock(volumeID)
	defer unlock()

	stagingTargetPath := req.GetStagingTargetPath()
	stagingParentPath := filepath.Dir(stagingTargetPath)
	volumeContext := req.GetVolumeContext()

	klog.Infof("NodeStageVolume: start, volumeID: %s stagingTargetPath: %s", volumeID, stagingTargetPath)

	initiator, err := util.NewMGXCsiInitiator(volumeContext, ns.conf)
	if err != nil {
		klog.Errorf("failed to create mgx initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodeStageVolume: connecting NVMe target, volumeID: %s", volumeID)
	devicePath, err := initiator.Connect(ns.conf.NrIoQueues, ns.conf.QueueSize) // idempotent
	if err != nil {
		klog.Errorf("failed to connect initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("NodeStageVolume: NVMe connected, volumeID: %s devicePath: %s", volumeID, devicePath)
	defer func() {
		if err != nil {
			initiator.Disconnect() //nolint:errcheck // best-effort rollback after stage failure; surfaced error already logged
		}
	}()

	mounted, err := ns.createMountPoint(stagingTargetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !mounted {
		mntFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
		klog.Infof("NodeStageVolume: formatting+mounting, volumeID: %s device: %s -> %s flags: %v", volumeID, devicePath, stagingTargetPath, mntFlags)
		sfMounter := mount.SafeFormatAndMount{Interface: ns.mounter, Exec: exec.New()}
		if err = sfMounter.FormatAndMount(devicePath, stagingTargetPath, "ext4", mntFlags); err != nil {
			klog.Errorf("failed to format and mount device, volumeID: %s err: %v", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("NodeStageVolume: mounted, volumeID: %s device: %s -> %s", volumeID, devicePath, stagingTargetPath)
	} else {
		klog.Infof("NodeStageVolume: staging path already mounted, volumeID: %s stagingTargetPath: %s", volumeID, stagingTargetPath)
	}

	volumeContext["devicePath"] = devicePath
	if err = util.StashVolumeContext(volumeContext, stagingParentPath); err != nil {
		klog.Errorf("failed to stash volume context, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodeStageVolume: success, volumeID: %s stagingTargetPath: %s devicePath: %s", volumeID, stagingTargetPath, devicePath)
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	unlock := ns.volumeLocks.Lock(volumeID)
	defer unlock()

	stagingTargetPath := req.GetStagingTargetPath()
	stagingParentPath := filepath.Dir(stagingTargetPath)

	klog.Infof("NodeUnstageVolume: start, volumeID: %s stagingTargetPath: %s", volumeID, stagingTargetPath)

	volumeContext, ctxErr := util.LookupVolumeContext(stagingParentPath)
	if ctxErr != nil {
		klog.Warningf("no volume context for volume %s, skipping disconnect: %v", volumeID, ctxErr)
	}

	klog.Infof("NodeUnstageVolume: unmounting, volumeID: %s stagingTargetPath: %s", volumeID, stagingTargetPath)
	if err := ns.deleteMountPoint(stagingTargetPath); err != nil {
		klog.Errorf("failed to unmount staging path, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("NodeUnstageVolume: unmounted, volumeID: %s stagingTargetPath: %s", volumeID, stagingTargetPath)

	if volumeContext != nil {
		initiator, err := util.NewMGXCsiInitiator(volumeContext, ns.conf)
		if err != nil {
			klog.Errorf("failed to create mgx initiator, volumeID: %s err: %v", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("NodeUnstageVolume: disconnecting NVMe target, volumeID: %s", volumeID)
		if err := initiator.Disconnect(); err != nil { // idempotent
			klog.Errorf("failed to disconnect initiator, volumeID: %s err: %v", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("NodeUnstageVolume: NVMe disconnected, volumeID: %s", volumeID)

		if err := util.CleanUpVolumeContext(stagingParentPath); err != nil {
			klog.Errorf("failed to clean up volume context, volumeID: %s err: %v", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	klog.Infof("NodeUnstageVolume: success, volumeID: %s stagingTargetPath: %s", volumeID, stagingTargetPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

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

	stagingTargetPath := req.GetStagingTargetPath()
	stagingParentPath := filepath.Dir(stagingTargetPath)
	targetPath := req.GetTargetPath()

	if err := ns.ensureStagingHealthy(volumeID, stagingTargetPath, stagingParentPath); err != nil {
		klog.Errorf("failed to recover staging, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodePublishVolume: bind-mounting, volumeID: %s %s -> %s", volumeID, stagingTargetPath, targetPath)

	mounted, err := ns.createMountPoint(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if mounted {
		klog.Infof("NodePublishVolume: target already bind-mounted, volumeID: %s targetPath: %s", volumeID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := ns.mounter.Mount(stagingTargetPath, targetPath, "", []string{"bind"}); err != nil {
		klog.Errorf("failed to bind-mount, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodePublishVolume: success, volumeID: %s targetPath: %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// ensureStagingHealthy probes the staging mount and the underlying NVMe device.
// If either is gone or corrupted, it tears down the broken state and rebuilds
// the staging mount so that a pod restart (Unpublish → Publish) can recover
// without waiting for ref-count to drop to zero and trigger Unstage.
//
// Kubelet only calls NodeStageVolume once per volume per node — so without
// this, a SPDK target restart that orphans the NVMe controller leaves staging
// wedged and every subsequent pod inherits the broken mount.
func (ns *nodeServer) ensureStagingHealthy(volumeID, stagingTargetPath, stagingParentPath string) error {
	volumeContext, err := util.LookupVolumeContext(stagingParentPath)
	if err != nil {
		// No stashed context means Stage was never completed; nothing to recover here.
		klog.V(5).Infof("skipping staging health check for volume %s: %v", volumeID, err)
		return nil
	}

	h, err := ns.stagingHealth(volumeContext["devicePath"], volumeContext["nqn"], stagingTargetPath)
	if err != nil {
		return err
	}
	if h.healthy {
		return nil
	}

	switch {
	case h.nvmeDegraded:
		klog.Warningf("staging unhealthy for volume %s: NVMe controller present but no live path (target may have failed), recovering", volumeID)
	case h.readOnly:
		klog.Warningf("staging unhealthy for volume %s: filesystem remounted read-only (likely ext4 errors=remount-ro after I/O errors), recovering", volumeID)
	case h.deviceExists && !mount.IsCorruptedMnt(h.mountErr):
		klog.Infof("staging not mounted for volume %s, rebuilding (expected after volume_clean or pod restart)", volumeID)
	default:
		klog.Warningf("staging unhealthy for volume %s (deviceExists=%v isMounted=%v mountErr=%v), recovering", volumeID, h.deviceExists, h.isMounted, h.mountErr)
	}
	return ns.recoverStaging(volumeID, stagingTargetPath, stagingParentPath, volumeContext, h)
}

type stagingHealthResult struct {
	healthy      bool
	deviceExists bool
	isMounted    bool
	mountErr     error
	// nvmeDegraded is true when the NVMe subsystem is present but no path is
	// in the `live` state — controller is in connecting/resetting/dead state,
	// I/O blocks or returns EIO, but /dev still has the device node so the
	// cheap os.Stat / IsMountPoint checks lie.
	nvmeDegraded bool
	// readOnly is true when the staging mount appears in /proc/mounts with the
	// `ro` flag despite being staged read-write. ext4 with errors=remount-ro
	// flips here after an I/O error and stays read-only even if the NVMe
	// controller later reconnects, so we must unmount+remount to recover.
	readOnly bool
}

// stagingHealth reports whether the staging mount + backing device look intact.
// A non-nil error means the mount probe itself failed with a non-recoverable error.
//
// Beyond the cheap inode/mountinfo checks, it also probes:
//   - NVMe controller path state (`live` vs connecting/dead) — catches the
//     case where the SPDK target died but ctrl-loss-tmo keeps the host's
//     /dev node alive in a broken state.
//   - Mount RO flag in /proc/mounts — catches the case where ext4 has
//     remounted itself read-only after I/O errors.
func (ns *nodeServer) stagingHealth(devicePath, nqn, stagingTargetPath string) (stagingHealthResult, error) {
	_, deviceErr := os.Stat(devicePath)
	deviceExists := deviceErr == nil

	isMounted, mountErr := ns.mounter.IsMountPoint(stagingTargetPath)
	if mountErr != nil && !os.IsNotExist(mountErr) && !mount.IsCorruptedMnt(mountErr) {
		return stagingHealthResult{deviceExists: deviceExists, isMounted: isMounted, mountErr: mountErr}, mountErr
	}

	res := stagingHealthResult{
		deviceExists: deviceExists,
		isMounted:    isMounted,
		mountErr:     mountErr,
	}

	if nqn != "" {
		connected, live, nerr := util.NvmeSubsysStatus(nqn)
		if nerr != nil {
			// Best-effort: a failed list-subsys probe shouldn't mask the rest
			// of the health check. Log and treat as not-degraded.
			klog.Warningf("NvmeSubsysStatus(%s) failed during staging health check: %v", nqn, nerr)
		} else if connected && !live {
			res.nvmeDegraded = true
		}
	}

	if isMounted && mountErr == nil {
		ro, roErr := ns.isStagingReadOnly(stagingTargetPath)
		if roErr != nil {
			klog.Warningf("read-only probe for %s failed: %v", stagingTargetPath, roErr)
		} else {
			res.readOnly = ro
		}
	}

	res.healthy = deviceExists && isMounted && mountErr == nil && !res.nvmeDegraded && !res.readOnly
	return res, nil
}

// isStagingReadOnly scans /proc/mounts (via mount-utils) for the staging path
// and reports whether the kernel has it flagged `ro`. ext4 with the default
// errors=remount-ro silently flips to ro on the first I/O error; subsequent
// pods that bind-mount through staging inherit that ro state and the workload
// fails with EROFS even after the underlying device recovers.
func (ns *nodeServer) isStagingReadOnly(path string) (bool, error) {
	mps, err := ns.mounter.List()
	if err != nil {
		return false, err
	}
	for i := range mps {
		if mps[i].Path != path {
			continue
		}
		for _, opt := range mps[i].Opts {
			if opt == "ro" {
				return true, nil
			}
		}
		return false, nil
	}
	return false, nil
}

func (ns *nodeServer) recoverStaging(volumeID, stagingTargetPath, stagingParentPath string,
	volumeContext map[string]string, h stagingHealthResult,
) error {
	// Unmount when the mount exists in any form OR when our deeper probes
	// flagged it as degraded — even though the kernel still considers it a
	// mount, the underlying device or filesystem is broken and bind-mounting
	// pods onto it would inherit the failure.
	forceUnmount := h.isMounted || mount.IsCorruptedMnt(h.mountErr) || h.nvmeDegraded || h.readOnly
	if forceUnmount {
		if uerr := ns.deleteMountPoint(stagingTargetPath); uerr != nil {
			klog.Warningf("failed to clean staging mount during recovery for volume %s: %v", volumeID, uerr)
		}
	}

	initiator, err := util.NewMGXCsiInitiator(volumeContext, ns.conf)
	if err != nil {
		return err
	}

	// Disconnect first (idempotent) to clear any zombie NVMe controller before reconnecting.
	if derr := initiator.Disconnect(); derr != nil {
		klog.Warningf("disconnect during recovery for volume %s: %v", volumeID, derr)
	}

	klog.Infof("recoverStaging: reconnecting NVMe, volumeID: %s", volumeID)
	newDevicePath, err := initiator.Connect(ns.conf.NrIoQueues, ns.conf.QueueSize)
	if err != nil {
		return err
	}
	klog.Infof("recoverStaging: NVMe reconnected, volumeID: %s devicePath: %s", volumeID, newDevicePath)

	mounted, err := ns.createMountPoint(stagingTargetPath)
	if err != nil {
		return err
	}
	if !mounted {
		sfMounter := mount.SafeFormatAndMount{Interface: ns.mounter, Exec: exec.New()}
		if err := sfMounter.FormatAndMount(newDevicePath, stagingTargetPath, "ext4", nil); err != nil {
			return err
		}
		klog.Infof("recoverStaging: staging remounted, volumeID: %s -> %s", volumeID, stagingTargetPath)
	}

	volumeContext["devicePath"] = newDevicePath
	return util.StashVolumeContext(volumeContext, stagingParentPath)
}

func (ns *nodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	unlock := ns.volumeLocks.Lock(volumeID)
	defer unlock()

	targetPath := req.GetTargetPath()

	// Snapshot bind-mount refs BEFORE unmounting target so we can tell whether
	// another pod is already publishing the same volume (rolling update /
	// recreate where the new pod's NodePublishVolume ran before our
	// NodeUnpublishVolume). In that case we must not run volume_clean — it
	// would tear down the live pod's I/O path.
	stagingPath, otherPodMounts := ns.classifyMountRefs(targetPath)

	klog.Infof("NodeUnpublishVolume: unmounting bind, volumeID: %s targetPath: %s", volumeID, targetPath)
	if err := ns.deleteMountPoint(targetPath); err != nil {
		klog.Errorf("failed to unmount target path, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if otherPodMounts > 0 {
		klog.Infof("NodeUnpublishVolume: %d other pod mount(s) still bound to staging, skipping volume_clean, volumeID: %s", otherPodMounts, volumeID)
		klog.Infof("NodeUnpublishVolume: success, volumeID: %s targetPath: %s", volumeID, targetPath)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if stagingPath == "" {
		klog.Infof("NodeUnpublishVolume: no staging mount detected, skipping volume_clean, volumeID: %s", volumeID)
		klog.Infof("NodeUnpublishVolume: success, volumeID: %s targetPath: %s", volumeID, targetPath)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if !ns.conf.VolumeCleanEnabled {
		klog.Infof("NodeUnpublishVolume: volume_clean disabled, unmounting staging only, volumeID: %s stagingPath: %s", volumeID, stagingPath)
		if err := ns.deleteMountPoint(stagingPath); err != nil {
			klog.Errorf("failed to unmount staging path, volumeID: %s err: %v", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if err := ns.cleanVolumeAfterUnpublish(volumeID, stagingPath); err != nil {
		klog.Errorf("volume_clean cycle failed, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodeUnpublishVolume: success, volumeID: %s targetPath: %s", volumeID, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// classifyMountRefs splits the bind-mount peers of targetPath into the staging
// mount and any other pod target mounts. Other pod target mounts are an
// overlap signal: another pod is already (or still) using the same volume on
// this node, so we must skip the destructive clean cycle.
//
// Different volumes on the same node have different NVMe namespace devices,
// so GetMountRefs (which keys on major:minor + root) only returns refs
// belonging to this volume — multi-volume / multi-NVMe nodes are handled
// implicitly without needing to filter by volumeID.
func (ns *nodeServer) classifyMountRefs(targetPath string) (stagingPath string, otherPodMounts int) {
	refs, err := ns.mounter.GetMountRefs(targetPath)
	if err != nil {
		klog.Warningf("GetMountRefs(%s) failed: %v", targetPath, err)
		return "", 0
	}
	podsDir := kubeletPodsDir(targetPath)
	for _, ref := range refs {
		if ref == targetPath {
			continue
		}
		if podsDir != "" && strings.HasPrefix(ref, podsDir) {
			otherPodMounts++
			klog.V(5).Infof("classifyMountRefs: other pod mount %s", ref)
			continue
		}
		// First non-pod ref is the staging mount. Subsequent ones shouldn't
		// exist in normal CSI flow; keep the first and log the rest.
		if stagingPath == "" {
			stagingPath = ref
		} else {
			klog.Warningf("classifyMountRefs: unexpected extra non-pod ref %s (already have staging %s)", ref, stagingPath)
		}
	}
	return stagingPath, otherPodMounts
}

// kubeletPodsDir returns the kubelet per-pod root (e.g. /var/lib/kubelet/pods/)
// derived from the CSI targetPath, which kubelet always shapes as
// <root>/pods/<podUID>/volumes/kubernetes.io~csi/<pv>/mount. Deriving the
// prefix instead of hard-coding it lets the driver work on clusters that run
// kubelet with a custom --root-dir. Returns "" if targetPath doesn't contain
// the expected "/pods/" segment.
func kubeletPodsDir(targetPath string) string {
	const segment = "/pods/"
	idx := strings.Index(targetPath, segment)
	if idx < 0 {
		return ""
	}
	return targetPath[:idx+len(segment)]
}

// cleanVolumeAfterUnpublish unmounts the global staging mount, asks the
// storage backend to clean the volume, then polls until the volume reports
// READY. The next NodePublishVolume rebuilds staging via ensureStagingHealthy.
func (ns *nodeServer) cleanVolumeAfterUnpublish(volumeID, stagingPath string) error {
	klog.Infof("NodeUnpublishVolume: unmounting staging, volumeID: %s stagingPath: %s", volumeID, stagingPath)
	if err := ns.deleteMountPoint(stagingPath); err != nil {
		return fmt.Errorf("unmount staging %s: %w", stagingPath, err)
	}

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return fmt.Errorf("init mgx client: %w", err)
	}

	klog.Infof("NodeUnpublishVolume: calling volume_clean, volumeID: %s", volumeID)
	if err := mgxClient.CleanVolume(volumeID); err != nil {
		return fmt.Errorf("volume_clean: %w", err)
	}

	return ns.waitVolumeReady(volumeID, mgxClient)
}

// waitVolumeReady polls volume_get on cfg-tunable intervals until the volume
// reports READY or the total timeout elapses.
func (ns *nodeServer) waitVolumeReady(volumeID string, mgxClient *util.NodeNVMf) error {
	interval := time.Duration(ns.conf.VolumeCleanPollIntervalSec) * time.Second
	timeout := time.Duration(ns.conf.VolumeCleanReadyTimeoutSec) * time.Second
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	deadline := time.Now().Add(timeout)
	for {
		vol, err := mgxClient.GetVolume(volumeID)
		if err != nil {
			klog.Warningf("waitVolumeReady: GetVolume failed, volumeID: %s err: %v", volumeID, err)
		} else if vol.Status == VolumeStatusReady {
			klog.Infof("waitVolumeReady: volume READY, volumeID: %s", volumeID)
			return nil
		} else {
			klog.V(5).Infof("waitVolumeReady: volume not READY yet, volumeID: %s status: %s", volumeID, vol.Status)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("volume %s did not reach READY within %s", volumeID, timeout)
		}
		time.Sleep(interval)
	}
}

func (*nodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
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
	stagingParentPath := filepath.Dir(req.GetStagingTargetPath())

	volumeContext, err := util.LookupVolumeContext(stagingParentPath)
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
