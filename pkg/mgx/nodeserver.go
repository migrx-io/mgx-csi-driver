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
	if vc.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
		return nil, status.Error(codes.InvalidArgument,
			"Only ReadWriteOnce (RWO) volumes is supported by this driver")
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

	h, err := ns.stagingHealth(volumeContext["devicePath"], stagingTargetPath)
	if err != nil {
		return err
	}
	if h.healthy {
		return nil
	}

	klog.Warningf("staging unhealthy for volume %s, recovering", volumeID)
	return ns.recoverStaging(volumeID, stagingTargetPath, stagingParentPath, volumeContext, h.isMounted, h.mountErr)
}

type stagingHealthResult struct {
	healthy   bool
	isMounted bool
	mountErr  error
}

// stagingHealth reports whether the staging mount + backing device look intact.
// A non-nil error means the mount probe itself failed with a non-recoverable error.
func (ns *nodeServer) stagingHealth(devicePath, stagingTargetPath string) (stagingHealthResult, error) {
	_, deviceErr := os.Stat(devicePath)
	deviceExists := deviceErr == nil

	isMounted, mountErr := ns.mounter.IsMountPoint(stagingTargetPath)
	if mountErr != nil && !os.IsNotExist(mountErr) && !mount.IsCorruptedMnt(mountErr) {
		return stagingHealthResult{isMounted: isMounted, mountErr: mountErr}, mountErr
	}

	return stagingHealthResult{
		healthy:   deviceExists && isMounted && mountErr == nil,
		isMounted: isMounted,
		mountErr:  mountErr,
	}, nil
}

func (ns *nodeServer) recoverStaging(volumeID, stagingTargetPath, stagingParentPath string,
	volumeContext map[string]string, isMounted bool, mountErr error,
) error {
	if isMounted || mount.IsCorruptedMnt(mountErr) {
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

	// fstrim before unmount so the thin-provisioned backend can reclaim freed
	// blocks on every pod restart. Best-effort: never blocks unpublish.
	ns.bestEffortFstrim(targetPath)

	klog.Infof("NodeUnpublishVolume: unmounting bind, volumeID: %s targetPath: %s", volumeID, targetPath)
	if err := ns.deleteMountPoint(targetPath); err != nil {
		klog.Errorf("failed to unmount target path, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodeUnpublishVolume: success, volumeID: %s targetPath: %s", volumeID, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
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

// bestEffortFstrim trims unused blocks at path. Failures (EIO, timeout, not
// mounted, discard unsupported) are logged and swallowed — fstrim must never
// block unpublish.
func (ns *nodeServer) bestEffortFstrim(path string) {
	isMount, err := ns.mounter.IsMountPoint(path)
	if err != nil || !isMount {
		klog.V(5).Infof("skipping fstrim on %s (mounted=%v err=%v)", path, isMount, err)
		return
	}
	// Probe FS health: a wedged ext4 over a dead NVMe controller returns EIO
	// on stat. Skipping fstrim in that case avoids hanging on the trim ioctl.
	if _, statErr := os.Stat(path); statErr != nil {
		klog.Warningf("skipping fstrim on %s: stat failed: %v", path, statErr)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.New().CommandContext(ctx, "fstrim", path).CombinedOutput()
	if err != nil {
		klog.Warningf("fstrim %s failed (best-effort): %v: %s", path, err, strings.TrimSpace(string(out)))
		return
	}
	klog.Infof("fstrim %s: %s", path, strings.TrimSpace(string(out)))
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
