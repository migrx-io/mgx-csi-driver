package mgx

import (
	"context"
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
	csi.UnimplementedControllerServer
	driver *csicommon.CSIDriver
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
	volumeID := req.GetName()
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return nil, err
	}

	csiVolume, err := cs.createVolume(ctx, req, mgxClient)
	if err != nil {
		klog.Errorf("failed to create volume, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeInfo, err := cs.publishVolume(csiVolume.GetVolumeId(), mgxClient)
	if err != nil {
		klog.Errorf("failed to publish volume, volumeID: %s err: %v", volumeID, err)
		cs.deleteVolume(csiVolume.GetVolumeId(), mgxClient) //nolint:errcheck
		return nil, status.Error(codes.Internal, err.Error())
	}

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
	volumeID := req.GetVolumeId()
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return nil, err
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

	return &csi.DeleteVolumeResponse{}, nil
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

	volumeID := req.GetSourceVolumeId()
	klog.Infof("CreateSnapshot : volumeID=%s", volumeID)

	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	snapshotName := req.GetName()
	klog.Infof("CreateSnapshot : snapshotName=%s", snapshotName)
	mgxVol, err := getMGXVol(volumeID)
	if err != nil {
		klog.Errorf("failed to get mgx volume, volumeID: %s err: %v", volumeID, err)
		return nil, err
	}

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("failed to create mgx client: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	snapshotID, err := mgxClient.CreateSnapshot(mgxVol.lvolID, snapshotName)
	klog.Infof("CreateSnapshot : snapshotID: %s", snapshotID)
	if err != nil {
		klog.Errorf("failed to create snapshot, volumeID: %s snapshotName: %s err: %v", volumeID, snapshotName, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volSize, err := mgxClient.GetVolumeSize(mgxVol.lvolID)
	klog.Infof("CreateSnapshot : volSize: %s", volSize)
	if err != nil {
		klog.Errorf("failed to get volume info, volumeID: %s err: %v", volumeID, err)
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
	mgxSnapshot, err := getSnapshot(csiSnapshotID)
	if err != nil {
		klog.Errorf("failed to get mgx snapshot, snapshotID: %s err: %v", csiSnapshotID, err)
		return nil, err
	}
	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("failed to create mgx client: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	unlock := cs.volumeLocks.Lock(csiSnapshotID)
	defer unlock()

	klog.Infof("Deleting Snapshot : csiSnapshotID=%s mgxSnapshotID=%s", csiSnapshotID, mgxSnapshot.snapshotID)

	err = mgxClient.DeleteSnapshot(mgxSnapshot.snapshotID)
	if err != nil {
		klog.Errorf("failed to delete snapshot, snapshotID: %s err: %v", csiSnapshotID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func getIntParameter(params map[string]string, key string, defaultValue int) (int, error) {
	if valueStr, exists := params[key]; exists {
		value, err := strconv.Atoi(valueStr)
		if err != nil {
			return 0, fmt.Errorf("error converting %s: %w", key, err)
		}
		return value, nil
	}
	return defaultValue, nil
}

func prepareCreateVolumeReq(ctx context.Context, req *csi.CreateVolumeRequest, sizeMiB int64) (*util.CreateLVolData, error) {
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

	cache_r_cache_size, err := getIntParameter(params, "cache_r_cache_size", 0)
	if err != nil {
		return nil, err
	}
	cache_rw_cache_size, err := getIntParameter(params, "cache_rw_cache_size", 0)
	if err != nil {
		return nil, err
	}

	qos_r_mbytes_per_sec, err := getIntParameter(params, "qos_r_mbytes_per_sec", 0)
	if err != nil {
		return nil, err
	}
	qos_w_mbytes_per_sec, err := getIntParameter(params, "qos_w_mbytes_per_sec", 0)
	if err != nil {
		return nil, err
	}
	qos_rw_ios_per_sec, err := getIntParameter(params, "qos_rw_ios_per_sec", 0)
	if err != nil {
		return nil, err
	}

	storage_compress, err := getIntParameter(params, "storage_compress", 0)
	if err != nil {
		return nil, err
	}



	createVolReq := util.CreateLVolData{
		Name:                 req.GetName(),
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

func (cs *controllerServer) getExistingVolume(name string, mgxClient *util.NodeNVMf, vol *csi.Volume) (*csi.Volume, error) {

	volume, err := mgxClient.GetVolume(name)
	if err == nil {
		vol.VolumeId = volume.Name
		klog.V(5).Info("volume already exists", vol.GetVolumeId())
		return vol, nil
	}
	return nil, err
}

func (cs *controllerServer) createVolume(ctx context.Context, req *csi.CreateVolumeRequest, mgxClient *util.NodeNVMf) (*csi.Volume, error) {

	size := req.GetCapacityRange().GetRequiredBytes()
	if size == 0 {
		klog.Warningln("invalid volume size, resize to 1G")
		size = 1024 * 1024 * 1024
	}

	sizeMiB := util.ToMiB(size)

	vol := csi.Volume{
		CapacityBytes: sizeMiB * 1024 * 1024,
		VolumeContext: req.GetParameters(),
		ContentSource: req.GetVolumeContentSource(),
	}

	klog.V(5).Info("provisioning volume from mgx..")

	existingVolume, err := cs.getExistingVolume(req.GetName(), mgxClient, &vol)
	if err == nil {
		return existingVolume, nil
	}

	createVolReq, err := prepareCreateVolumeReq(ctx, req, sizeMiB)
	if err != nil {
		return nil, err
	}

	volumeID, err := mgxClient.CreateVolume(createVolReq)
	if err != nil {
		klog.Errorf("error creating mgx volume: %v", err)
		return nil, err
	}

	vol.VolumeId = volumeID
	klog.V(5).Info("successfully created volume from mgx with Volume ID: ", vol.GetVolumeId())

	return &vol, nil
}

func getMGXVol(csiVolumeID string) (*mgxVolume, error) {
	return &mgxVolume{
		lvolID: csiVolumeID,
	}, nil
}

func getSnapshot(csiSnapshotID string) (*mgxSnapshot, error) {
	return &mgxSnapshot{
		snapshotID: csiSnapshotID,
	}, nil
}

func (cs *controllerServer) publishVolume(volumeID string, mgxClient *util.NodeNVMf) (map[string]string, error) {
	mgxVol, err := getMGXVol(volumeID)
	if err != nil {
		return nil, err
	}

	err = mgxClient.PublishVolume(mgxVol.lvolID)
	if err != nil {
		return nil, err
	}

	volumeInfo, err := mgxClient.VolumeInfo(mgxVol.lvolID)
	if err != nil {
		cs.unpublishVolume(volumeID, mgxClient) //nolint:errcheck
		return nil, err
	}

	return volumeInfo, nil
}

func (cs *controllerServer) deleteVolume(volumeID string, mgxClient *util.NodeNVMf) error {
	mgxVol, err := getMGXVol(volumeID)
	if err != nil {
		return err
	}

	return mgxClient.DeleteVolume(mgxVol.lvolID)
}

func (cs *controllerServer) unpublishVolume(volumeID string, mgxClient *util.NodeNVMf) error {
	mgxVol, err := getMGXVol(volumeID)
	if err != nil {
		return err
	}

	return mgxClient.UnpublishVolume(mgxVol.lvolID)
}

func (cs *controllerServer) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {

	volumeID := req.GetVolumeId()
	updatedSize := req.GetCapacityRange().GetRequiredBytes()

	mgxVol, err := getMGXVol(volumeID)
	if err != nil {
		return nil, err
	}

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

	volumeID := req.GetVolumeId()
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	mgxVol, err := getMGXVol(volumeID)
	if err != nil {
		return nil, err
	}

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("failed to create mgx client: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeInfo, err := mgxClient.VolumeInfo(mgxVol.lvolID)
	if err != nil {
		klog.Errorf("failed to get mgxVol for %s: %v", volumeID, err)

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

func newControllerServer(d *csicommon.CSIDriver) (*controllerServer, error) {
	server := controllerServer{
		UnimplementedControllerServer: csi.UnimplementedControllerServer{},
		driver: csicommon.NewDefaultControllerServer(d),
		volumeLocks:             util.NewVolumeLocks(),
	}
	return &server, nil
}
