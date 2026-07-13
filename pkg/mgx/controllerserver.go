package mgx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"

	csicommon "github.com/migrx-io/mgx-csi-driver/pkg/csi-common"
	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

const (
	VolumeStatusStopped = "STOPPED"
	VolumeStatusReady   = "READY"
	VolumeStatusDeleted = "DELETED"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	volumeLocks *util.VolumeLocks
	conf        *util.Config
}

type mgxVolume struct {
	lvolID string
}

// CreateVolume creates a new volume in the SimplyBlock storage system.
func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	volumeID := util.PvcToVolName(req.GetName())
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	// --- reject unsupported access modes ---
	if err := validateAccessModes(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("failed to init mgxClient, err: %s", err)
		return nil, err
	}

	klog.V(5).Info("mgxClient is created..")

	// check if volume exists and READY
	volume, err := mgxClient.GetVolume(volumeID)

	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("failed to get volume, volumeID: %s err: %s", volumeID, err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}

	if errors.Is(err, util.ErrNotFound) {
		klog.V(5).Infof("volume doesn't exists: %s", volumeID)

		// Restore-from-snapshot: the snapshot plugin provisions the storage
		// volume itself (restore_add -> volume_create on copy completion), so
		// drive the restore instead of calling volume_create directly. Once
		// the restore is READY the storage volume exists and the normal flow
		// below picks it up on the next reconcile tick.
		if req.GetVolumeContentSource().GetSnapshot() != nil {
			return nil, cs.restoreVolume(req, mgxClient)
		}

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
	if volume.Status != VolumeStatusReady {
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

	// A restore-provisioned volume now exists and is published, so the
	// restore-<vol> bookkeeping record is no longer needed. Drop it (keep the
	// data, purge=false - the target IS this volume) so a future PVC with the
	// same name isn't blocked by a stale record. Best-effort.
	if req.GetVolumeContentSource().GetSnapshot() != nil {
		restoreName := restoreNamePrefix + volumeID
		if derr := mgxClient.DeleteSnapshot(restoreName, "", false); derr != nil && !errors.Is(derr, util.ErrNotFound) {
			klog.Warningf("CreateVolume: cleanup restore record %s failed (ignored): %s", restoreName, derr)
		}
	}

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

// validateAccessModes rejects any access mode other than RWO/RWOP.
func validateAccessModes(caps []*csi.VolumeCapability) error {
	for _, vc := range caps {
		mode := vc.GetAccessMode().GetMode()
		if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
			mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
			return status.Error(codes.InvalidArgument,
				"Only ReadWriteOnce (RWO) and ReadWriteOncePod (RWOP) are supported by this driver")
		}
	}
	return nil
}

func (cs *controllerServer) UnIdleVolume(volumeID string) error {
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return err
	}

	klog.V(5).Info("mgxClient is created..")

	volume, err := mgxClient.GetVolume(volumeID)
	if err != nil {
		klog.Errorf("failed to get volume, volumeID: %s err: %s", volumeID, err.Error())
		return err
	}

	// Only un-idle volumes that are actually idle (STOPPED). Transient states
	// (CLEANING, INIT, PENDING, STOPPING, DELETING) mean the backend is mid-flight
	// — calling volume_start here would re-stamp the volume to INIT and force a
	// full re-provisioning cycle, racing with whatever op is in progress. Most
	// commonly this fired during a NodeUnpublishVolume volume_clean cycle.
	if volume.Status != VolumeStatusStopped {
		klog.V(5).Infof("UnIdleVolume: skipping, volumeID: %s status: %s", volumeID, volume.Status)
		return nil
	}

	if err := cs.startVolume(volumeID, mgxClient); err != nil {
		klog.Errorf("failed to start volume, volumeID: %s err: %s", volumeID, err.Error())
		return err
	}

	klog.Infof("volume is starting: %s", volumeID)
	return nil
}

func (cs *controllerServer) IdleVolume(volumeID string) error {
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return err
	}

	klog.V(5).Info("mgxClient is created..")

	// check if volume exists and STOPPED
	volume, err := mgxClient.GetVolume(volumeID)

	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("failed to get volume, volumeID: %s err: %s", volumeID, err.Error())
		return err
	}

	if errors.Is(err, util.ErrNotFound) {
		klog.V(5).Infof("volume is not found: %v", volume)
		return nil
	}

	// there is no errors, check is state is READY
	if volume.Status != VolumeStatusReady {
		klog.V(5).Infof("volume is not READY: %v", volume)
		// reconcile
		return nil
	}

	// if volume is not STOPPED then stop it first
	if volume.Status != VolumeStatusStopped {
		klog.V(5).Infof("volume is not STOPPED: %v", volume)

		err = cs.stopVolume(volumeID, mgxClient)
		if err != nil {
			klog.Errorf("failed to stop volume, volumeID: %s err: %s", volumeID, err.Error())
			return err
		}

		klog.Infof("volume is stopping: %s", volumeID)
		// reconcile
		return nil
	}

	return nil
}

func (cs *controllerServer) DeleteVolume(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return nil, err
	}

	klog.V(5).Info("mgxClient is created..")

	// Drop any lingering restore-<vol> bookkeeping record (e.g. a restore the
	// user gave up on). Done before the volume lookup so it also clears records
	// for volumes that never finished provisioning. Keep the data (purge=false)
	// - the storage volume itself is removed below. Best-effort.
	restoreName := restoreNamePrefix + volumeID
	if derr := mgxClient.DeleteSnapshot(restoreName, "", false); derr != nil && !errors.Is(derr, util.ErrNotFound) {
		klog.Warningf("DeleteVolume: cleanup restore record %s failed (ignored): %s", restoreName, derr)
	}

	// check if volume exists and DELETED
	volume, err := mgxClient.GetVolume(volumeID)

	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("failed to get volume, volumeID: %s err: %s", volumeID, err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}

	if errors.Is(err, util.ErrNotFound) {
		klog.V(5).Infof("volume is not found: %v", volume)
		return &csi.DeleteVolumeResponse{}, nil
	}

	// there is no errors, check if state is DELETED
	if volume.Status == VolumeStatusDeleted {
		klog.V(5).Infof("volume is DELETED: %v", volume)
		// reconcile
		return &csi.DeleteVolumeResponse{}, nil
	}

	// if volume is not STOPPED then stop it first
	if volume.Status != VolumeStatusStopped {
		klog.V(5).Infof("volume is not STOPPED: %v", volume)

		err = cs.stopVolume(volumeID, mgxClient)
		if err != nil {
			klog.Errorf("failed to stop volume, volumeID: %s err: %s", volumeID, err.Error())
			return nil, status.Error(codes.Internal, err.Error())
		}

		klog.Infof("volume is stopping: %s", volumeID)
		// reconcile
		return nil, status.Error(codes.Aborted, fmt.Sprintf("volume %s is stopping", volumeID))
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

// ControllerPublishVolume gates the cross-node attach. external-attacher
// calls this before the new node's NodePublishVolume; we look up the volume
// on the backend and return Aborted if it isn't READY yet, which happens
// when ControllerUnpublishVolume on the previous node is still running its
// volume_clean cycle. Aborted causes the attacher to retry with backoff
// until the backend reports READY. The backend has no per-node ACL — the
// volume is exposed at CreateVolume time — so there's no real "attach" RPC
// to issue here; an empty PublishContext is returned because everything the
// node plugin needs already lives in volume_context from CreateVolume.
func (cs *controllerServer) ControllerPublishVolume(_ context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	nodeID := req.GetNodeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id missing")
	}
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id missing")
	}

	vc := req.GetVolumeCapability()
	if vc == nil {
		return nil, status.Error(codes.InvalidArgument, "volume_capability missing")
	}
	mode := vc.GetAccessMode().GetMode()
	if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
		mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
		return nil, status.Error(codes.InvalidArgument,
			"Only ReadWriteOnce (RWO) and ReadWriteOncePod (RWOP) are supported by this driver")
	}

	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	klog.Infof("ControllerPublishVolume: start, volumeID: %s nodeID: %s", volumeID, nodeID)

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("ControllerPublishVolume: init mgx client, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volume, err := mgxClient.GetVolume(volumeID)
	if err != nil {
		if errors.Is(err, util.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
		}
		klog.Errorf("ControllerPublishVolume: get volume, volumeID: %s err: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if volume.Status != VolumeStatusReady {
		klog.Infof("ControllerPublishVolume: volume not READY, returning Aborted for attacher retry; volumeID: %s status: %s nodeID: %s", volumeID, volume.Status, nodeID)
		return nil, status.Errorf(codes.Aborted,
			"volume %s not READY (current: %s); retry after backend completes in-flight cycle",
			volumeID, volume.Status)
	}

	klog.Infof("ControllerPublishVolume: success, volumeID: %s nodeID: %s", volumeID, nodeID)
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume is the matching half of the
// PUBLISH_UNPUBLISH_VOLUME capability — required by the CSI spec but
// intentionally lightweight here. The real volume_clean runs in
// NodeUnpublishVolume on the node that owned the mount; this method
// just signals external-attacher that the VolumeAttachment can be
// reconciled away. The cross-node race is closed by
// ControllerPublishVolume's READY check, not by anything done here.
func (cs *controllerServer) ControllerUnpublishVolume(_ context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	nodeID := req.GetNodeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id missing")
	}

	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	klog.Infof("ControllerUnpublishVolume: success (no-op), volumeID: %s nodeID: %s", volumeID, nodeID)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// waitVolumeReady polls volume_get on cfg-tunable intervals until the
// volume reports READY or the total timeout elapses. Shared between
// ControllerUnpublishVolume (post volume_clean) and any other site that
// needs a backend readiness barrier.
func waitVolumeReady(volumeID string, mgxClient *util.NodeNVMf, conf *util.Config) error {
	interval := time.Duration(conf.VolumeCleanPollIntervalSec) * time.Second
	timeout := time.Duration(conf.VolumeCleanReadyTimeoutSec) * time.Second
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

func getFloatParameter(params map[string]string, key string) (float64, error) {
	if valueStr, exists := params[key]; exists {
		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return 0, fmt.Errorf("error converting %s: %w", key, err)
		}
		return value, nil
	}
	return 0, nil
}

func calculateCacheSize(sizeMiB, minMiB, maxMiB int, ratio float64) int {
	// multiply sizeMiB by ratio
	calculated := int(float64(sizeMiB) * ratio)

	// clamp to min
	if calculated < minMiB {
		return minMiB
	}

	// clamp to max
	if calculated > maxMiB {
		return maxMiB
	}

	return calculated
}

// volumeTuning holds the per-volume cache/QoS/storage knobs derived from
// StorageClass parameters. It is shared by CreateVolume and the restore path so
// a restored volume is provisioned with the same StorageClass-driven settings
// as a freshly created one, instead of falling back to the target pool's
// default config.
type volumeTuning struct {
	CacheRCacheSize      int
	CacheRWCacheSize     int
	QosRMbytesPerSec     int
	QosWMbytesPerSec     int
	QosRWMbytesPerSec    int
	QosRWIosPerSec       int
	StorageCompress      int
	StorageEncryptSecret string
}

// extractVolumeTuning derives the cache/QoS/storage settings from StorageClass
// parameters. Cache sizes are computed from the requested volume size using the
// min/max/ratio parameters (see calculateCacheSize).
func extractVolumeTuning(params map[string]string, sizeMiB int64) (*volumeTuning, error) {
	min_cache_r_cache_size, err := getIntParameter(params, "min_cache_r_cache_size")
	if err != nil {
		return nil, err
	}
	min_cache_rw_cache_size, err := getIntParameter(params, "min_cache_rw_cache_size")
	if err != nil {
		return nil, err
	}

	max_cache_r_cache_size, err := getIntParameter(params, "max_cache_r_cache_size")
	if err != nil {
		return nil, err
	}
	max_cache_rw_cache_size, err := getIntParameter(params, "max_cache_rw_cache_size")
	if err != nil {
		return nil, err
	}

	ratio_cache_r_cache_size, err := getFloatParameter(params, "ratio_cache_r_cache_size")
	if err != nil {
		return nil, err
	}
	ratio_cache_rw_cache_size, err := getFloatParameter(params, "ratio_cache_rw_cache_size")
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
	qos_rw_mbytes_per_sec, err := getIntParameter(params, "qos_rw_mbytes_per_sec")
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

	return &volumeTuning{
		// calc cache size based on cache attributes and the requested size
		CacheRCacheSize:      calculateCacheSize(int(sizeMiB), min_cache_r_cache_size, max_cache_r_cache_size, ratio_cache_r_cache_size),
		CacheRWCacheSize:     calculateCacheSize(int(sizeMiB), min_cache_rw_cache_size, max_cache_rw_cache_size, ratio_cache_rw_cache_size),
		QosRMbytesPerSec:     qos_r_mbytes_per_sec,
		QosWMbytesPerSec:     qos_w_mbytes_per_sec,
		QosRWMbytesPerSec:    qos_rw_mbytes_per_sec,
		QosRWIosPerSec:       qos_rw_ios_per_sec,
		StorageCompress:      storage_compress,
		StorageEncryptSecret: params["storage_encrypt_secret"],
	}, nil
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
	// qos_rw_mbytes_per_sec: <qos_rw_mbytes_per_sec>
	// qos_w_mbytes_per_sec: <qos_w_mbytes_per_sec>
	// size: <size>
	// storage_compress: <storage_compress>
	// storage_encrypt_secret: <storage_encrypt_secret>

	//
	// calculate cache size based on volume size request
	//

	tuning, err := extractVolumeTuning(params, sizeMiB)
	if err != nil {
		return nil, err
	}

	createVolReq := util.CreateLVolData{
		Name:                 util.PvcToVolName(req.GetName()),
		Size:                 sizeMiB,
		Config:               params["config"],
		Labels:               params["labels"],
		CacheRCacheSize:      tuning.CacheRCacheSize,
		CacheRWCacheSize:     tuning.CacheRWCacheSize,
		QosRMbytesPerSec:     tuning.QosRMbytesPerSec,
		QosWMbytesPerSec:     tuning.QosWMbytesPerSec,
		QosRWMbytesPerSec:    tuning.QosRWMbytesPerSec,
		QosRWIosPerSec:       tuning.QosRWIosPerSec,
		StorageEncryptSecret: tuning.StorageEncryptSecret,
		StorageCompress:      tuning.StorageCompress,
	}
	return &createVolReq, nil
}

func (*controllerServer) GetCSIVolume(req *csi.CreateVolumeRequest) *csi.Volume {
	size := req.GetCapacityRange().GetRequiredBytes()

	vol := csi.Volume{
		CapacityBytes: size,
		VolumeContext: req.GetParameters(),
		ContentSource: req.GetVolumeContentSource(),
	}

	vol.VolumeId = util.PvcToVolName(req.GetName())

	return &vol
}

func (cs *controllerServer) createVolume(ctx context.Context, req *csi.CreateVolumeRequest, mgxClient *util.NodeNVMf) error {
	vol := cs.GetCSIVolume(req)

	sizeMiB := util.BytesToMB(vol.CapacityBytes)

	klog.V(5).Infof("CreateVolume req: %v, sizeMiB: %d", req, sizeMiB)

	createVolReq, err := prepareCreateVolumeReq(ctx, req, sizeMiB)
	if err != nil {
		return err
	}

	klog.V(5).Infof("CreateVolume VolumeID: %s, createVolReq: %v", vol.VolumeId, createVolReq)

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

func (*controllerServer) stopVolume(volumeID string, mgxClient *util.NodeNVMf) error {
	mgxVol := getMGXVol(volumeID)

	return mgxClient.StopVolume(mgxVol.lvolID)
}

func (*controllerServer) startVolume(volumeID string, mgxClient *util.NodeNVMf) error {
	mgxVol := getMGXVol(volumeID)

	return mgxClient.StartVolume(mgxVol.lvolID)
}

func (*controllerServer) resizeVolume(volumeID string, mgxClient *util.NodeNVMf, updatedSize int64) error {
	mgxVol := getMGXVol(volumeID)

	return mgxClient.ResizeVolume(mgxVol.lvolID, updatedSize)
}

func (*controllerServer) unpublishVolume(volumeID string, mgxClient *util.NodeNVMf) error {
	mgxVol := getMGXVol(volumeID)

	return mgxClient.UnpublishVolume(mgxVol.lvolID)
}

func (cs *controllerServer) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()

	newSize := req.GetCapacityRange().GetRequiredBytes()
	updatedSize := util.BytesToMB(newSize)

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		return nil, err
	}

	klog.V(5).Info("mgxClient is created..")

	// check if volume exists and READY
	volume, err := mgxClient.GetVolume(volumeID)

	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("failed to get volume, volumeID: %s err: %s", volumeID, err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}

	if errors.Is(err, util.ErrNotFound) {
		klog.Errorf("volume is not found: %v", volume)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// check if size is changed than means node was resized and stopped before
	// and we just need to start it exit

	if int64(volume.Size) != updatedSize {
		klog.V(5).Infof("start resizing: %v", volume)

		if volume.Status != "STOPPED" {
			klog.V(5).Info("stop volume before resizing..")

			err = cs.stopVolume(volumeID, mgxClient)
			if err != nil {
				klog.Errorf("failed to stop volume, volumeID: %s err: %s", volumeID, err)
				return nil, status.Error(codes.Internal, err.Error())
			}

			// reconcile
			return nil, status.Error(codes.Aborted, fmt.Sprintf("volume %s is stopping", volumeID))
		}
		// volume STOPPED then resize it
		err = cs.resizeVolume(volumeID, mgxClient, updatedSize)
		if err != nil {
			klog.Errorf("failed to resize volume, volumeID: %s err: %s", volumeID, err.Error())
			return nil, status.Error(codes.Internal, err.Error())
		}

		// reconcile
		return nil, status.Error(codes.Aborted, fmt.Sprintf("volume %s resized", volumeID))
	}

	// there is no errors, check is state is READY
	if volume.Status != VolumeStatusReady {
		klog.V(5).Infof("volume is not READY: %v", volume)
		// reconcile
		err = cs.startVolume(volumeID, mgxClient)
		if err != nil {
			klog.Errorf("failed to start volume, volumeID: %s err: %s", volumeID, err)
			return nil, status.Error(codes.Internal, err.Error())
		}

		// reconcile
		return nil, status.Error(codes.Aborted, fmt.Sprintf("volume %s is not READY", volumeID))
	}

	klog.V(5).Infof("volume is resized: %v", volume)

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         newSize,
		NodeExpansionRequired: true,
	}, nil
}

func (cs *controllerServer) ControllerGetVolume(_ context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	volumeID := req.GetVolumeId()

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

func newControllerServer(d *csicommon.CSIDriver, conf *util.Config) *controllerServer {
	server := controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		volumeLocks:             util.NewVolumeLocks(),
		conf:                    conf,
	}
	return &server
}
