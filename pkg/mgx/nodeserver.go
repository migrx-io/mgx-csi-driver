package mgx

import (
	"context"
	"os"
	"path/filepath"

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

func (*nodeServer) NodeStageVolume(_ context.Context, _ *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return &csi.NodeStageVolumeResponse{}, nil
}

func (*nodeServer) NodeUnstageVolume(_ context.Context, _ *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
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

	targetPath := req.GetTargetPath()
	volumeContext := req.GetVolumeContext()

	initiator, err := util.NewMGXCsiInitiator(volumeContext)
	if err != nil {
		klog.Errorf("failed to create mgx initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	devicePath, err := initiator.Connect(ns.conf.NrIoQueues, ns.conf.QueueSize) // idempotent
	if err != nil {
		klog.Errorf("failed to connect initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer func() {
		if err != nil {
			initiator.Disconnect() //nolint:errcheck
		}
	}()

	mounted, err := ns.createMountPoint(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !mounted {
		mntFlags := vc.GetMount().GetMountFlags()
		sfMounter := mount.SafeFormatAndMount{Interface: ns.mounter, Exec: exec.New()}
		if err = sfMounter.FormatAndMount(devicePath, targetPath, "ext4", mntFlags); err != nil {
			klog.Errorf("failed to format and mount device, volumeID: %s err: %v", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	volumeContext["devicePath"] = devicePath
	if err = util.StashVolumeContext(volumeContext, filepath.Dir(targetPath)); err != nil {
		klog.Errorf("failed to stash volume context, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	unlock := ns.volumeLocks.Lock(volumeID)
	defer unlock()

	targetPath := req.GetTargetPath()
	contextPath := filepath.Dir(targetPath)

	volumeContext, err := util.LookupVolumeContext(contextPath)
	if err != nil {
		klog.Warningf("no volume context for volume %s, skipping disconnect: %v", volumeID, err)
		if err := ns.deleteMountPoint(targetPath); err != nil {
			klog.Errorf("failed to delete mount point, volumeID: %s err: %v", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if err = ns.deleteMountPoint(targetPath); err != nil {
		klog.Errorf("failed to unmount target path, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	initiator, err := util.NewMGXCsiInitiator(volumeContext)
	if err != nil {
		klog.Errorf("failed to create mgx initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err = initiator.Disconnect(); err != nil { // idempotent
		klog.Errorf("failed to disconnect initiator, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err = util.CleanUpVolumeContext(contextPath); err != nil {
		klog.Errorf("failed to clean up volume context, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
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

	volumeContext, err := util.LookupVolumeContext(filepath.Dir(volumeMountPath))
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

		// Corrupted mount entry — treat as mounted to prevent accidental mount over it.
		if mount.IsCorruptedMnt(err) {
			klog.Warningf("Corrupted mount point detected for %s: %v", path, err)
			return true, nil
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
		} else if mount.IsCorruptedMnt(err) {
			klog.Warningf("Corrupted mount point detected at %s", path)
			isMount = true
		} else {
			klog.Errorf("Error checking mount point %s: %v", path, err)
			return err
		}
	}

	if isMount {
		err = ns.mounter.Unmount(path)
		if err != nil {
			return err
		}
	}
	return os.RemoveAll(path)
}
