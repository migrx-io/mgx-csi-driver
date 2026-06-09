package mgx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog"

	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

const (
	// Snapshot plugin statuses (subset the driver branches on). The full set is
	// PENDING -> RUNNING -> READY|FAILED, plus STOPPING/STOPPED, DELETING/DELETED.
	SnapshotStatusReady  = "READY"
	SnapshotStatusFailed = "FAILED"

	// snapshotIDSep joins the backup record name and the restore-point stamp
	// into the CSI snapshot_id: "<record>@<stamp>".
	snapshotIDSep = "@"

	// bytesPerMB matches the storage volume's MB unit (see util.BytesToMB).
	bytesPerMB = int64(1024 * 1024)

	// restoreNamePrefix keeps a kind=restore record from colliding with the
	// kind=snapshot record of the volume it provisions (both keyed by name).
	restoreNamePrefix = "restore-"
)

// makeSnapshotID builds the CSI snapshot_id from a backup record and a stamp.
func makeSnapshotID(record, stamp string) string {
	return record + snapshotIDSep + stamp
}

// parseSnapshotID splits a CSI snapshot_id into its record and stamp. Neither
// component contains the separator (records are vol-<hash>, stamps are sanitized).
func parseSnapshotID(id string) (record, stamp string, err error) {
	i := strings.Index(id, snapshotIDSep)
	if i < 0 {
		return "", "", fmt.Errorf("invalid snapshot id %q (want <record>@<stamp>)", id)
	}
	return id[:i], id[i+1:], nil
}

// snapshotStamp derives a deterministic, caller-owned restore-point id from the
// VolumeSnapshot name. Being stable across retries is what makes snapshot_add
// idempotent (a retried CreateSnapshot maps to the same increment).
func snapshotStamp(snapshotName string) string {
	return strings.ReplaceAll(snapshotName, snapshotIDSep, "-")
}

// parseSnapshotTime parses the plugin's "created" field. It has no timezone
// (ex 2026-06-06T04:38:26.574000); fall back to now if it can't be parsed.
func parseSnapshotTime(s string) *timestamppb.Timestamp {
	if s == "" {
		return timestamppb.Now()
	}
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return timestamppb.New(t)
		}
	}
	klog.Warningf("parseSnapshotTime: unrecognized time %q", s)
	return timestamppb.Now()
}

// CreateSnapshot backs up the source volume via the snapshot plugin. One record
// owns the volume's whole backup chain (keyed by the source volume id); each
// VolumeSnapshot is one restore point (stamp) within it. snapshot_add is always
// the entry point and is idempotent on the caller-supplied stamp, so the
// sidecar can retry until the record reports READY.
func (cs *controllerServer) CreateSnapshot(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	volumeID := req.GetSourceVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "source_volume_id missing")
	}

	snapshotName := req.GetName()
	if snapshotName == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name missing")
	}

	config := req.GetParameters()["config"]
	if config == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot class parameter 'config' is required")
	}

	record := volumeID
	stamp := snapshotStamp(snapshotName)
	snapshotID := makeSnapshotID(record, stamp)

	klog.Infof("CreateSnapshot: volumeID=%s record=%s stamp=%s config=%s", volumeID, record, stamp, config)

	// Serialize per backup record (== source volume) so concurrent snapshots
	// of the same volume don't race arming the chain.
	unlock := cs.volumeLocks.Lock(record)
	defer unlock()

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("CreateSnapshot: init mgx client, err: %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Resolve the node that runs the rclone copy from the source volume.
	volume, err := mgxClient.GetVolume(volumeID)
	if err != nil {
		if errors.Is(err, util.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "source volume %s not found", volumeID)
		}
		klog.Errorf("CreateSnapshot: get source volume, volumeID: %s err: %s", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	addParams := map[string]any{
		"name":    record,
		"config":  config,
		"volume":  volumeID,
		"sc_node": volume.SCNode,
		"stamp":   stamp,
	}
	// Optional per-snapshot overrides from the VolumeSnapshotClass.
	for _, k := range []string{"incremental", "mode", "storage_class", "labels"} {
		if v := req.GetParameters()[k]; v != "" {
			addParams[k] = v
		}
	}

	if err = mgxClient.AddSnapshot(addParams); err != nil {
		klog.Errorf("CreateSnapshot: snapshot_add failed, record: %s stamp: %s err: %s", record, stamp, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	rec, err := mgxClient.ShowSnapshot(record)
	if err != nil {
		klog.Errorf("CreateSnapshot: snapshot_show failed, record: %s err: %s", record, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if rec.Status == SnapshotStatusFailed {
		return nil, status.Errorf(codes.Internal, "snapshot %s failed: %s", snapshotID, rec.Error)
	}

	// size is the logical volume size in MB, set on create; fall back to the
	// live source volume size if the record hasn't recorded it yet.
	sizeBytes := rec.Size * bytesPerMB
	if sizeBytes == 0 {
		sizeBytes = int64(volume.Size) * bytesPerMB
	}

	ready := rec.Status == SnapshotStatusReady
	klog.Infof("CreateSnapshot: snapshotID=%s status=%s ready=%v sizeBytes=%d", snapshotID, rec.Status, ready, sizeBytes)

	// ReadyToUse=false makes the external-snapshotter re-call (idempotent on the
	// stamp) until the backup completes.
	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SizeBytes:      sizeBytes,
			SnapshotId:     snapshotID,
			SourceVolumeId: volumeID,
			CreationTime:   parseSnapshotTime(rec.Created),
			ReadyToUse:     ready,
		},
	}, nil
}

// DeleteSnapshot removes one restore point (the stamp) from its backup record.
// Only the oldest stamp may be deleted; k8s retention deletes VolumeSnapshots
// oldest-first, and deleting the last point drops the whole backup.
func (cs *controllerServer) DeleteSnapshot(_ context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	csiSnapshotID := req.GetSnapshotId()
	if csiSnapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id missing")
	}

	record, stamp, err := parseSnapshotID(csiSnapshotID)
	if err != nil {
		// A malformed id maps to nothing we can delete; treat as already gone.
		klog.Warningf("DeleteSnapshot: %v", err)
		return &csi.DeleteSnapshotResponse{}, nil
	}

	unlock := cs.volumeLocks.Lock(record)
	defer unlock()

	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("DeleteSnapshot: init mgx client, err: %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("DeleteSnapshot: record=%s stamp=%s", record, stamp)

	if err := mgxClient.DeleteSnapshot(record, stamp, false); err != nil {
		if errors.Is(err, util.ErrNotFound) {
			return &csi.DeleteSnapshotResponse{}, nil
		}
		klog.Errorf("DeleteSnapshot: snapshot_del failed, record: %s stamp: %s err: %s", record, stamp, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

// restoreVolume drives a restore-from-snapshot. The snapshot plugin reads the
// backed-up data and, on completion, provisions the target storage volume
// itself (volume_create), so the driver only schedules the restore and waits.
// A dedicated kind=restore record (restore-<volumeID>) tracks progress without
// colliding with the kind=snapshot record of the volume it creates. It is
// called from CreateVolume only while the target storage volume does not yet
// exist; once the restore is READY the normal CreateVolume flow picks the
// provisioned volume up and publishes it.
// restoreVolume always returns an error: it only schedules the restore and
// drives it to READY, returning Aborted on every tick so the external-provisioner
// re-calls CreateVolume. The provisioned volume is picked up and published by the
// normal CreateVolume path once it exists, so there is no success response here.
func (*controllerServer) restoreVolume(req *csi.CreateVolumeRequest, mgxClient *util.NodeNVMf) error {
	volumeID := util.PvcToVolName(req.GetName())
	sourceSnapshotID := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()

	record, stamp, err := parseSnapshotID(sourceSnapshotID)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	volumeConfig := req.GetParameters()["config"]
	if volumeConfig == "" {
		return status.Error(codes.InvalidArgument, "storage class parameter 'config' is required")
	}

	restoreName := restoreNamePrefix + volumeID

	rec, err := mgxClient.ShowSnapshot(restoreName)
	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("restoreVolume: show restore record, restoreName: %s err: %s", restoreName, err)
		return status.Error(codes.Internal, err.Error())
	}

	if errors.Is(err, util.ErrNotFound) {
		// First call: inherit the snapshot config + node from the source record
		// and schedule the restore.
		src, serr := mgxClient.ShowSnapshot(record)
		if serr != nil {
			if errors.Is(serr, util.ErrNotFound) {
				return status.Errorf(codes.NotFound, "source snapshot %s not found", record)
			}
			klog.Errorf("restoreVolume: show source snapshot, record: %s err: %s", record, serr)
			return status.Error(codes.Internal, serr.Error())
		}

		restoreParams := map[string]any{
			"name":          restoreName,
			"config":        src.Config,
			"snapshot":      record,
			"target":        volumeID,
			"volume_config": volumeConfig,
			"sc_node":       src.SCNode,
			"restore_to":    stamp,
		}
		if v := req.GetParameters()["labels"]; v != "" {
			restoreParams["labels"] = v
		}

		if aerr := mgxClient.AddRestore(restoreParams); aerr != nil {
			klog.Errorf("restoreVolume: restore_add failed, restoreName: %s err: %s", restoreName, aerr)
			return status.Error(codes.Internal, aerr.Error())
		}

		klog.Infof("restoreVolume: scheduled restoreName=%s target=%s record=%s stamp=%s", restoreName, volumeID, record, stamp)
		return status.Error(codes.Aborted, fmt.Sprintf("restore %s is creating", volumeID))
	}

	if rec.Status == SnapshotStatusFailed {
		return status.Errorf(codes.Internal, "restore %s failed: %s", restoreName, rec.Error)
	}

	if rec.Status != SnapshotStatusReady {
		klog.V(5).Infof("restoreVolume: restore %s not READY: %s", restoreName, rec.Status)
		return status.Error(codes.Aborted, fmt.Sprintf("restore %s is not READY: %s", volumeID, rec.Status))
	}

	// Restore READY: the plugin has provisioned the target storage volume.
	// Bounce through one more reconcile tick so the normal CreateVolume path
	// finds the volume and publishes it once it reaches READY.
	klog.Infof("restoreVolume: restore %s READY, target volume provisioning", restoreName)
	return status.Error(codes.Aborted, fmt.Sprintf("restore %s ready, provisioning volume %s", restoreName, volumeID))
}
