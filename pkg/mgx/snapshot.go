package mgx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
	SnapshotStatusReady     = "READY"
	SnapshotStatusFailed    = "FAILED"
	SnapshotStatusRunning   = "RUNNING"
	SnapshotStatusStopping  = "STOPPING"
	SnapshotStatusRewinding = "REWINDING"
	SnapshotStatusDeleting  = "DELETING"
	SnapshotStatusDeleted   = "DELETED"

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

	// Optional: when unset the snapshot plugin defaults to the pool's single
	// snapshot config (each pool is its own cluster with one config named
	// after the pool).
	config := req.GetParameters()["config"]

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

	// Check whether this restore point is already armed before touching the
	// plugin. The external-snapshotter re-calls CreateSnapshot until
	// ReadyToUse=true (and again on later re-syncs); snapshot_add must fire only
	// once per stamp, not on every reconcile. A record whose current Stamp
	// already equals ours means the increment is armed - just poll its status.
	rec, err := armRestorePoint(mgxClient, req, record, stamp, config, volume)
	if err != nil {
		return nil, err
	}

	// size is the logical volume size in MB, set on create; fall back to the
	// live source volume size if the record hasn't recorded it yet.
	sizeBytes := rec.Size * bytesPerMB
	if sizeBytes == 0 {
		sizeBytes = int64(volume.Size) * bytesPerMB
	}

	// Ready means THIS restore point is committed on the chain - not merely
	// that the record is READY (it may be settled on another stamp, ex after
	// a rewind cleared a failed one), nor not-ready because a newer point is
	// in flight on the same record.
	ready := stampCommitted(rec, stamp)

	if !ready && rec.Status == SnapshotStatusFailed && rec.Stamp == stamp {
		// Re-armed above; the plugin heals (rewind) then the next reconcile
		// re-runs. Report not-ready so the external-snapshotter keeps retrying
		// until the backup completes, instead of a terminal error that wedges
		// the VolumeSnapshot at not-ready forever.
		klog.Warningf("CreateSnapshot: snapshot %s FAILED (%s), re-arming for retry", snapshotID, rec.Error)
	}

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

// recordStamps returns a record's committed restore points, oldest first. The
// plugin returns increments as a JSON-encoded string or as a list.
func recordStamps(rec *util.SnapshotResp) []string {
	var out []string
	switch v := rec.Increments.(type) {
	case string:
		if v != "" {
			if err := json.Unmarshal([]byte(v), &out); err != nil {
				klog.Warningf("recordStamps: %s: bad increments %q: %v", rec.Name, v, err)
			}
		}
	case []any:
		for _, s := range v {
			if str, ok := s.(string); ok {
				out = append(out, str)
			}
		}
	}
	return out
}

// stampCommitted reports whether stamp is a finished restore point of rec. A
// flat (non-incremental) backup keeps no chain; its one point is the READY
// record's stamp.
func stampCommitted(rec *util.SnapshotResp, stamp string) bool {
	stamps := recordStamps(rec)
	for _, s := range stamps {
		if s == stamp {
			return true
		}
	}
	return len(stamps) == 0 && rec.Status == SnapshotStatusReady && rec.Stamp == stamp
}

// heldByOtherStamp reports whether the record is in flight (or parked) for a
// different restore point, so arming ours now would overwrite that one's armed
// stamp. Only a settled record may be re-armed for a new stamp: READY, or
// DELETED (a purged backup starts a fresh chain). A FAILED/STOPPED run is left
// to its own VolumeSnapshot to heal or abandon. The plugin refuses the same
// snapshot_add; this keeps the waiting VolumeSnapshot quietly not-ready instead
// of erroring, and covers plugin nodes that predate that guard.
func heldByOtherStamp(rec *util.SnapshotResp, stamp string) bool {
	if rec.Stamp == stamp {
		return false
	}
	switch rec.Status {
	case SnapshotStatusReady, SnapshotStatusDeleted:
		return false
	}
	return true
}

// needsArm reports whether an existing record must be (re-)armed for stamp, and
// logs why not: the stamp is already committed (a re-sync of a finished point;
// re-arming would start a new run), already armed and not failed, or the record
// is busy with another stamp.
func needsArm(rec *util.SnapshotResp, record, stamp string) bool {
	switch {
	case stampCommitted(rec, stamp):
		klog.Infof("CreateSnapshot: restore point already committed, record: %s stamp: %s", record, stamp)
		return false
	case rec.Stamp == stamp && rec.Status != SnapshotStatusFailed:
		klog.Infof("CreateSnapshot: restore point already armed, record: %s stamp: %s status: %s", record, stamp, rec.Status)
		return false
	case heldByOtherStamp(rec, stamp):
		klog.Infof("CreateSnapshot: record %s is %s with stamp %s, waiting to arm %s", record, rec.Status, rec.Stamp, stamp)
		return false
	}
	return true
}

// armRestorePoint ensures the backup record has this stamp armed and returns its
// current state. It reads the record, and if it's missing or settled on a
// different stamp, fires the idempotent snapshot_add and re-reads. An already
// armed or committed stamp is returned as-is so CreateSnapshot can just poll
// its status, and so is a record busy with another stamp: that VolumeSnapshot
// stays not-ready and arms on a later reconcile, once the record settles.
func armRestorePoint(mgxClient *util.NodeNVMf, req *csi.CreateSnapshotRequest, record, stamp, config string, volume *util.LvolResp) (*util.SnapshotResp, error) {
	rec, err := mgxClient.ShowSnapshot(record)
	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("CreateSnapshot: snapshot_show failed, record: %s err: %s", record, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err == nil && !needsArm(rec, record, stamp) {
		return rec, nil
	}

	// Record missing, settled on a different stamp, or FAILED on ours (needs
	// heal/re-arm): arm this restore point. On a FAILED record the plugin turns
	// this idempotent snapshot_add into a rewind (REWINDING) that heals the torn state, then
	// settles clean so the next call re-arms - so a transient failure self-heals
	// instead of wedging the snapshot.
	addParams := map[string]any{
		"name":    record,
		"config":  config,
		"volume":  req.GetSourceVolumeId(),
		"sc_node": volume.SCNode,
		"stamp":   stamp,
	}
	// Optional per-snapshot overrides from the VolumeSnapshotClass.
	for _, k := range []string{"incremental", "mode", "storage_class", "labels"} {
		if v := req.GetParameters()[k]; v != "" {
			addParams[k] = v
		}
	}
	// Integer overrides of the config's rclone concurrency and incremental
	// chain cap. max_increments 0 is meaningful (unlimited), so any set value
	// is forwarded; unset falls back to the config.
	for _, k := range []string{"transfers", "checkers", "max_increments"} {
		if v := req.GetParameters()[k]; v != "" {
			n, perr := strconv.Atoi(v)
			if perr != nil || n < 0 {
				return nil, status.Errorf(codes.InvalidArgument, "invalid %s %q: expected a non-negative integer", k, v)
			}
			addParams[k] = n
		}
	}

	if aerr := mgxClient.AddSnapshot(addParams); aerr != nil {
		klog.Errorf("CreateSnapshot: snapshot_add failed, record: %s stamp: %s err: %s", record, stamp, aerr)
		return nil, status.Error(codes.Internal, aerr.Error())
	}

	rec, err = mgxClient.ShowSnapshot(record)
	if err != nil {
		klog.Errorf("CreateSnapshot: snapshot_show failed, record: %s err: %s", record, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return rec, nil
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

// scheduleRestore (re)schedules the restore via the snapshot plugin. Called on
// the first CreateVolume (no record yet) and to re-arm a FAILED restore -
// restore_add is idempotent on name and resets a FAILED record to PENDING
// (purging the partial target) so the retry runs clean.
//
// The restore does NOT inherit the source snapshot's config/sc_node: those are
// source-pool values, but a restore may be placed on a DIFFERENT pool (chosen
// by `labels` via mgmt find_pool). The target pool resolves its own snapshot
// config (its single default) and runs on its own node; mgmt pre-resolves the
// source snapshot's data (volume/size/increments/stamp) into the replayed
// request so the target pool needs no local source record, and the backup is
// reachable cross-pool via the shared backup bucket.
func (*controllerServer) scheduleRestore(req *csi.CreateVolumeRequest, mgxClient *util.NodeNVMf, restoreName, record, stamp, volumeID, volumeConfig string) error {
	// validate the source exists (fail fast with a clear NotFound); its
	// config/node are intentionally not copied onto the restore (see above).
	src, serr := mgxClient.ShowSnapshot(record)
	if serr != nil {
		if errors.Is(serr, util.ErrNotFound) {
			return status.Errorf(codes.NotFound, "source snapshot %s not found", record)
		}
		klog.Errorf("scheduleRestore: show source snapshot, record: %s err: %s", record, serr)
		return status.Error(codes.Internal, serr.Error())
	}
	// A backup being deleted can't be restored (the plugin fails such a
	// restore); don't re-arm it on every retry, report why instead.
	if src.Status == SnapshotStatusDeleting || src.Status == SnapshotStatusDeleted {
		return status.Errorf(codes.FailedPrecondition, "source snapshot %s is %s", record, src.Status)
	}

	restoreParams := map[string]any{
		"name":          restoreName,
		"snapshot":      record,
		"target":        volumeID,
		"volume_config": volumeConfig,
		"restore_to":    stamp,
	}
	if v := req.GetParameters()["labels"]; v != "" {
		restoreParams["labels"] = v
	}

	// Propagate the same StorageClass-driven cache/QoS/storage settings a plain
	// CreateVolume applies, so the restored volume is provisioned with them
	// instead of falling back to the target pool's default config. The snapshot
	// plugin forwards these into its volume_create at provision time.
	sizeMiB := util.BytesToMB(req.GetCapacityRange().GetRequiredBytes())
	tuning, terr := extractVolumeTuning(req.GetParameters(), sizeMiB)
	if terr != nil {
		return status.Error(codes.InvalidArgument, terr.Error())
	}
	restoreParams["cache_r_cache_size"] = tuning.CacheRCacheSize
	restoreParams["cache_rw_cache_size"] = tuning.CacheRWCacheSize
	restoreParams["qos_r_mbytes_per_sec"] = tuning.QosRMbytesPerSec
	restoreParams["qos_w_mbytes_per_sec"] = tuning.QosWMbytesPerSec
	restoreParams["qos_rw_mbytes_per_sec"] = tuning.QosRWMbytesPerSec
	restoreParams["qos_rw_ios_per_sec"] = tuning.QosRWIosPerSec
	restoreParams["storage_compress"] = tuning.StorageCompress
	if tuning.StorageEncryptSecret != "" {
		restoreParams["storage_encrypt_secret"] = tuning.StorageEncryptSecret
	}
	if tuning.ExtraParams != "" {
		restoreParams["extra_params"] = tuning.ExtraParams
	}

	if aerr := mgxClient.AddRestore(restoreParams); aerr != nil {
		klog.Errorf("scheduleRestore: restore_add failed, restoreName: %s err: %s", restoreName, aerr)
		return status.Error(codes.Internal, aerr.Error())
	}
	return nil
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
func (cs *controllerServer) restoreVolume(req *csi.CreateVolumeRequest, mgxClient *util.NodeNVMf) error {
	volumeID := util.PvcToVolName(req.GetName())
	sourceSnapshotID := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()

	record, stamp, err := parseSnapshotID(sourceSnapshotID)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	// Optional: when unset the storage plugin defaults the restored volume to
	// the pool's single storage config (named after the pool).
	volumeConfig := req.GetParameters()["config"]

	restoreName := restoreNamePrefix + volumeID

	rec, err := mgxClient.ShowSnapshot(restoreName)
	if err != nil && !errors.Is(err, util.ErrNotFound) {
		klog.Errorf("restoreVolume: show restore record, restoreName: %s err: %s", restoreName, err)
		return status.Error(codes.Internal, err.Error())
	}

	if errors.Is(err, util.ErrNotFound) {
		// First call: schedule the restore.
		if serr := cs.scheduleRestore(req, mgxClient, restoreName, record, stamp, volumeID, volumeConfig); serr != nil {
			return serr
		}
		klog.Infof("restoreVolume: scheduled restoreName=%s target=%s record=%s stamp=%s", restoreName, volumeID, record, stamp)
		return status.Error(codes.Aborted, fmt.Sprintf("restore %s is creating", volumeID))
	}

	if rec.Status == SnapshotStatusFailed {
		// A failed restore is terminal on the plugin and left a partial target
		// behind. Re-arm it (restore_add resets FAILED -> PENDING and purges the
		// partial target) and keep driving it, so a transient failure self-heals
		// instead of wedging the PVC. The error is surfaced via the Aborted
		// message while it retries.
		klog.Warningf("restoreVolume: restore %s FAILED (%s), re-arming", restoreName, rec.Error)
		if serr := cs.scheduleRestore(req, mgxClient, restoreName, record, stamp, volumeID, volumeConfig); serr != nil {
			return serr
		}
		return status.Error(codes.Aborted, fmt.Sprintf("restore %s failed, retrying: %s", restoreName, rec.Error))
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

// ListSnapshots lists committed restore points as CSI snapshots (one per stamp
// of each backup record), optionally filtered by snapshot_id or
// source_volume_id. Used by the external-snapshotter for pre-provisioned
// (static) VolumeSnapshotContents. starting_token is an index into the sorted
// result. A record carries no per-stamp time, so creation_time is the record's.
func (*controllerServer) ListSnapshots(_ context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	mgxClient, err := util.NewMGXClient()
	if err != nil {
		klog.Errorf("ListSnapshots: init mgx client, err: %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	var recs []*util.SnapshotResp
	wantStamp := ""
	if id := req.GetSnapshotId(); id != "" {
		record, stamp, perr := parseSnapshotID(id)
		if perr != nil {
			return &csi.ListSnapshotsResponse{}, nil
		}
		rec, serr := mgxClient.ShowSnapshot(record)
		if errors.Is(serr, util.ErrNotFound) {
			return &csi.ListSnapshotsResponse{}, nil
		}
		if serr != nil {
			klog.Errorf("ListSnapshots: snapshot_show %s: %s", record, serr)
			return nil, status.Error(codes.Internal, serr.Error())
		}
		recs = []*util.SnapshotResp{rec}
		wantStamp = stamp
	} else {
		recs, err = mgxClient.ListSnapshots()
		if err != nil {
			klog.Errorf("ListSnapshots: snapshot_list: %s", err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	entries := listEntries(recs, req.GetSourceVolumeId(), wantStamp)
	return paginateSnapshots(entries, req.GetStartingToken(), req.GetMaxEntries())
}

// paginateSnapshots returns one page of entries starting at the index in
// token (empty = first page), at most maxEntries long (0 = all).
func paginateSnapshots(entries []*csi.Snapshot, token string, maxEntries int32) (*csi.ListSnapshotsResponse, error) {
	start := 0
	if token != "" {
		n, err := strconv.Atoi(token)
		if err != nil || n < 0 || n > len(entries) {
			return nil, status.Errorf(codes.Aborted, "invalid starting_token %q", token)
		}
		start = n
	}
	end := len(entries)
	if m := int(maxEntries); m > 0 && start+m < end {
		end = start + m
	}

	resp := &csi.ListSnapshotsResponse{}
	for _, s := range entries[start:end] {
		resp.Entries = append(resp.Entries, &csi.ListSnapshotsResponse_Entry{Snapshot: s})
	}
	if end < len(entries) {
		resp.NextToken = strconv.Itoa(end)
	}
	return resp, nil
}

// listEntries expands backup records into CSI snapshots, one per committed
// stamp, sorted by record then chain order (stable across pages).
func listEntries(recs []*util.SnapshotResp, sourceVolumeID, wantStamp string) []*csi.Snapshot {
	sort.SliceStable(recs, func(i, j int) bool { return recs[i].Name < recs[j].Name })

	var out []*csi.Snapshot
	for _, rec := range recs {
		if rec.Kind == "restore" {
			continue
		}
		if sourceVolumeID != "" && rec.Volume != sourceVolumeID {
			continue
		}
		stamps := recordStamps(rec)
		if len(stamps) == 0 && rec.Status == SnapshotStatusReady && rec.Stamp != "" {
			stamps = []string{rec.Stamp} // flat backup: its one point
		}
		for _, stamp := range stamps {
			if wantStamp != "" && stamp != wantStamp {
				continue
			}
			out = append(out, &csi.Snapshot{
				SnapshotId:     makeSnapshotID(rec.Name, stamp),
				SourceVolumeId: rec.Volume,
				SizeBytes:      rec.Size * bytesPerMB,
				CreationTime:   parseSnapshotTime(rec.Created),
				ReadyToUse:     true,
			})
		}
	}
	return out
}
