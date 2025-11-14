package mgx

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog"

	csicommon "github.com/migrx-io/mgx-csi-driver/pkg/csi-common"
	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	volumeLocks *util.VolumeLocks
}

type mgxVolume struct {
	lvolID string
}

type mgxSnapshot struct {
	snapshotID string
}

// CreateVolume creates a new volume in the SimplyBlock storage system.
func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	volumeID := util.PvcToVolName(req.GetName())
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	var err error

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("failed to init mgxClient, err: %s", err)
		return nil, err
	}

	klog.V(5).Info("mgxClient is created..")

	// check if volume exists and READY
	volume, err := mgxClient.GetVolume(volumeID)

	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("failed to get volume, volumeID: %s err: %s", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if errors.Is(err, util.ErrNotFound) {
		klog.V(5).Infof("volume doesn't exists: %s", volumeID)

		err = cs.createVolume(ctx, req, mgxClient)
		if err != nil {
			klog.Errorf("failed to create volume, volumeID: %s err: %s", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}

		klog.Infof("volume is creating: %s", volumeID)
		// reconcile
		return nil, status.Error(codes.Aborted, fmt.Sprintf("volume %s is creating", volumeID))
	}

	// there is no errors, check is state is READY
	if volume.Status != "READY" {
		klog.V(5).Infof("volume: %v is not READY", volume)
		// reconcile
		return nil, status.Error(codes.Aborted, fmt.Sprintf("volume %s is not READY: %s", volumeID, volume.Status))
	}

	// volume is created and READY
	volumeInfo, err := cs.publishVolume(volumeID, mgxClient)
	if err != nil {
		klog.Errorf("failed to publish volume, volumeID: %s err: %s", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("volume is published: volumeInfo: %v", volumeInfo)

	csiVolume := cs.GetCSIVolume(req)

	// copy volume info. node needs these info to contact target(ip, port, nqn, ...)
	if csiVolume.VolumeContext == nil {
		csiVolume.VolumeContext = volumeInfo
	} else {
		for k, v := range volumeInfo {
			csiVolume.VolumeContext[k] = v
		}
	}

	return &csi.CreateVolumeResponse{Volume: csiVolume}, nil
}

func (cs *controllerServer) DeleteVolume(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := util.PvcToVolName(req.GetVolumeId())
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return nil, err
	}

	klog.V(5).Info("mgxClient is created..")

	// check if volume exists and DELETED
	volume, err := mgxClient.GetVolume(volumeID)

	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("failed to get volume, volumeID: %s err: %s", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if errors.Is(err, util.ErrNotFound) {
		klog.V(5).Infof("volume is not found: %v", volume)
		return &csi.DeleteVolumeResponse{}, nil
	}

	// there is no errors, check is state is READY
	if volume.Status == "DELETED" {
		klog.V(5).Infof("volume is DELETED: %v", volume)
		// reconcile
		return &csi.DeleteVolumeResponse{}, nil
	}

	// no harm if volume already unpublished
	err = cs.unpublishVolume(volumeID, mgxClient)
	if err != nil {
		return nil, err
	}

	// no harm if volume already deleted
	err = cs.deleteVolume(volumeID, mgxClient)
	if err != nil {
		return nil, err
	}

	return nil, status.Error(codes.Aborted, fmt.Sprintf("volume %s is still deleting", volumeID))
}

func (cs *controllerServer) ValidateVolumeCapabilities(_ context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	// make sure we support all requested caps
	for _, cap := range req.GetVolumeCapabilities() {
		supported := false
		for _, accessMode := range cs.Driver.GetVolumeCapabilityAccessModes() {
			if cap.GetAccessMode().GetMode() == accessMode.GetMode() {
				supported = true
				break
			}
		}
		if !supported {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

func (cs *controllerServer) CreateSnapshot(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	volumeID := util.PvcToVolName(req.GetSourceVolumeId())
	klog.Infof("CreateSnapshot : volumeID=%s", volumeID)

	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	snapshotName := req.GetName()
	klog.Infof("CreateSnapshot : snapshotName=%s", snapshotName)
	mgxVol := getMGXVol(volumeID)

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("failed to create mgx client: %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	snapshotID, err := mgxClient.CreateSnapshot(mgxVol.lvolID, snapshotName)
	klog.Infof("CreateSnapshot : snapshotID: %s", snapshotID)
	if err != nil {
		klog.Errorf("failed to create snapshot, volumeID: %s snapshotName: %s err: %s", volumeID, snapshotName, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volSize, err := mgxClient.GetVolumeSize(mgxVol.lvolID)
	klog.Infof("CreateSnapshot : volSize: %d", volSize)
	if err != nil {
		klog.Errorf("failed to get volume info, volumeID: %s err: %s", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	creationTime := timestamppb.Now()
	snapshotData := csi.Snapshot{
		SizeBytes:      int64(volSize),
		SnapshotId:     snapshotID,
		SourceVolumeId: mgxVol.lvolID,
		CreationTime:   creationTime,
		ReadyToUse:     true,
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &snapshotData,
	}, nil
}

func (cs *controllerServer) DeleteSnapshot(_ context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	csiSnapshotID := req.GetSnapshotId()
	mgxSnapshot := getSnapshot(csiSnapshotID)

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("failed to create mgx client: %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	unlock := cs.volumeLocks.Lock(csiSnapshotID)
	defer unlock()

	klog.Infof("Deleting Snapshot : csiSnapshotID=%s mgxSnapshotID=%s", csiSnapshotID, mgxSnapshot.snapshotID)

	err = mgxClient.DeleteSnapshot(mgxSnapshot.snapshotID)
	if err != nil {
		klog.Errorf("failed to delete snapshot, snapshotID: %s err: %s", csiSnapshotID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func getIntParameter(params map[string]string, key string) (int, error) {
	if valueStr, exists := params[key]; exists {
		value, err := strconv.Atoi(valueStr)
		if err != nil {
			return 0, fmt.Errorf("error converting %s: %w", key, err)
		}
		return value, nil
	}
	return 0, nil
}

func prepareCreateVolumeReq(_ context.Context, req *csi.CreateVolumeRequest, sizeMiB int64) (*util.CreateLVolData, error) {
	params := req.GetParameters()

	// cache_r_cache_size: <cache_r_cache_size>
	// cache_rw_cache_size: <cache_rw_cache_size>
	// config: <config>
	// labels: <labels>
	// name: <name>
	// qos_r_mbytes_per_sec: <qos_r_mbytes_per_sec>
	// qos_rw_ios_per_sec: <qos_rw_ios_per_sec>
	// qos_w_mbytes_per_sec: <qos_w_mbytes_per_sec>
	// size: <size>
	// storage_compress: <storage_compress>
	// storage_encrypt_secret: <storage_encrypt_secret>

	//
	// calculate cache size based on volume size request
	//

	cache_r_cache_size, err := getIntParameter(params, "cache_r_cache_size")
	if err != nil {
		return nil, err
	}
	cache_rw_cache_size, err := getIntParameter(params, "cache_rw_cache_size")
	if err != nil {
		return nil, err
	}

	qos_r_mbytes_per_sec, err := getIntParameter(params, "qos_r_mbytes_per_sec")
	if err != nil {
		return nil, err
	}
	qos_w_mbytes_per_sec, err := getIntParameter(params, "qos_w_mbytes_per_sec")
	if err != nil {
		return nil, err
	}
	qos_rw_ios_per_sec, err := getIntParameter(params, "qos_rw_ios_per_sec")
	if err != nil {
		return nil, err
	}

	storage_compress, err := getIntParameter(params, "storage_compress")
	if err != nil {
		return nil, err
	}

	createVolReq := util.CreateLVolData{
		Name:                 util.PvcToVolName(req.GetName()),
		Size:                 sizeMiB,
		Config:               params["config"],
		Labels:               params["labels"],
		CacheRCacheSize:      cache_r_cache_size,
		CacheRWCacheSize:     cache_rw_cache_size,
		QosRMbytesPerSec:     qos_r_mbytes_per_sec,
		QosWMbytesPerSec:     qos_w_mbytes_per_sec,
		QosRWIosPerSec:       qos_rw_ios_per_sec,
		StorageEncryptSecret: params["storage_encrypt_secret"],
		StorageCompress:      storage_compress,
	}
	return &createVolReq, nil
}

func (*controllerServer) GetCSIVolume(req *csi.CreateVolumeRequest) *csi.Volume {
	size := req.GetCapacityRange().GetRequiredBytes()

	sizeRounded := util.RoundToMiB(size)

	vol := csi.Volume{
		CapacityBytes: sizeRounded,
		VolumeContext: req.GetParameters(),
		ContentSource: req.GetVolumeContentSource(),
	}

	vol.VolumeId = util.PvcToVolName(req.GetName())

	return &vol
}

func (cs *controllerServer) createVolume(ctx context.Context, req *csi.CreateVolumeRequest, mgxClient *util.NodeNVMf) error {
	vol := cs.GetCSIVolume(req)

	sizeMiB := util.BytesToMB(vol.CapacityBytes)

	createVolReq, err := prepareCreateVolumeReq(ctx, req, sizeMiB)
	if err != nil {
		return err
	}

	err = mgxClient.CreateVolume(createVolReq)
	if err != nil {
		klog.Errorf("error creating mgx volume: %v", err)
		return err
	}
	klog.V(5).Infof("successfully created volume from mgx with VolumeID: %s", vol.VolumeId)

	return nil
}

func getMGXVol(csiVolumeID string) *mgxVolume {
	return &mgxVolume{
		lvolID: csiVolumeID,
	}
}

func getSnapshot(csiSnapshotID string) *mgxSnapshot {
	return &mgxSnapshot{
		snapshotID: csiSnapshotID,
	}
}

func (cs *controllerServer) publishVolume(volumeID string, mgxClient *util.NodeNVMf) (map[string]string, error) {
	mgxVol := getMGXVol(volumeID)

	err := mgxClient.PublishVolume(mgxVol.lvolID)
	if err != nil {
		return nil, err
	}

	volumeInfo, err := mgxClient.VolumeInfo(mgxVol.lvolID)
	if err != nil {
		_ = cs.unpublishVolume(volumeID, mgxClient)
		return nil, err
	}

	return volumeInfo, nil
}

func (*controllerServer) deleteVolume(volumeID string, mgxClient *util.NodeNVMf) error {
	mgxVol := getMGXVol(volumeID)

	return mgxClient.DeleteVolume(mgxVol.lvolID)
}

func (*controllerServer) unpublishVolume(volumeID string, mgxClient *util.NodeNVMf) error {
	mgxVol := getMGXVol(volumeID)

	return mgxClient.UnpublishVolume(mgxVol.lvolID)
}

func (*controllerServer) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := util.PvcToVolName(req.GetVolumeId())
	updatedSize := req.GetCapacityRange().GetRequiredBytes()

	mgxVol := getMGXVol(volumeID)

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return nil, err
	}

	_, err = mgxClient.ResizeVolume(mgxVol.lvolID, updatedSize)
	if err != nil {
		klog.Errorf("failed to resize vol, LVolID: %s err: %v", mgxVol.lvolID, err)
		return nil, err
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         updatedSize,
		NodeExpansionRequired: true,
	}, nil
}

func (cs *controllerServer) ControllerGetVolume(_ context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	volumeID := util.PvcToVolName(req.GetVolumeId())

	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	mgxVol := getMGXVol(volumeID)

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("failed to create mgx client: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeInfo, err := mgxClient.VolumeInfo(mgxVol.lvolID)
	if err != nil {
		klog.Errorf("failed to get mgxVol for %s: %s", volumeID, err)

		return &csi.ControllerGetVolumeResponse{
			Volume: &csi.Volume{
				VolumeId: volumeID,
			},
			Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
				VolumeCondition: &csi.VolumeCondition{
					Abnormal: true,
					Message:  err.Error(),
				},
			},
		}, nil
	}

	volume := &csi.Volume{
		VolumeId:      mgxVol.lvolID,
		VolumeContext: volumeInfo,
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: volume,
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: &csi.VolumeCondition{
				Abnormal: false,
				Message:  "",
			},
		},
	}, nil
}

func newControllerServer(d *csicommon.CSIDriver) *controllerServer {
	server := controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		volumeLocks:             util.NewVolumeLocks(),
	}
	return &server
}
