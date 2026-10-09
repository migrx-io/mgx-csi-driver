package mgx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/klog"

	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

const (
	// The external-snapshotter names a dynamic snapshot "snapshot-<VolumeSnapshot
	// UID>" and the snapshot-controller its content "snapcontent-<same UID>".
	snapshotNamePrefix        = "snapshot-"
	snapshotContentNamePrefix = "snapcontent-"

	snapshotTimeLookupTimeout = 5 * time.Second
)

var (
	volumeSnapshotGVR = schema.GroupVersionResource{
		Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots",
	}
	volumeSnapshotContentGVR = schema.GroupVersionResource{
		Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents",
	}
)

// snapshotTimer returns when the VolumeSnapshot behind a restore point (stamp)
// was created. The backup record has no per-stamp time, and the time has to be
// stable across CreateSnapshot retries, so it is taken from Kubernetes.
type snapshotTimer interface {
	SnapshotCreated(ctx context.Context, stamp string) (time.Time, error)
}

type k8sSnapshotTimer struct {
	client dynamic.Interface
}

func newK8sSnapshotTimer() (*k8sSnapshotTimer, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &k8sSnapshotTimer{client: client}, nil
}

// SnapshotCreated maps stamp snapshot-<uid> to VolumeSnapshotContent
// snapcontent-<uid> and returns its VolumeSnapshot's creation time, or the
// content's own when that VolumeSnapshot is gone or is a different object.
func (k *k8sSnapshotTimer) SnapshotCreated(ctx context.Context, stamp string) (time.Time, error) {
	uid, ok := strings.CutPrefix(stamp, snapshotNamePrefix)
	if !ok || uid == "" {
		return time.Time{}, fmt.Errorf("stamp %q is not %s<uid>", stamp, snapshotNamePrefix)
	}

	ctx, cancel := context.WithTimeout(ctx, snapshotTimeLookupTimeout)
	defer cancel()

	content, err := k.client.Resource(volumeSnapshotContentGVR).Get(ctx, snapshotContentNamePrefix+uid, metav1.GetOptions{})
	if err != nil {
		return time.Time{}, fmt.Errorf("get VolumeSnapshotContent %s%s: %w", snapshotContentNamePrefix, uid, err)
	}

	ns, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
	name, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "name")
	if ns != "" && name != "" {
		vs, err := k.client.Resource(volumeSnapshotGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if err == nil && string(vs.GetUID()) == uid {
			return vs.GetCreationTimestamp().Time, nil
		}
	}

	return content.GetCreationTimestamp().Time, nil
}

// snapshotCreationTime is the CSI creation_time of a restore point: when its
// VolumeSnapshot was created, falling back to the backup record's creation
// time when that can't be looked up (no in-cluster client, static snapshot).
func (cs *controllerServer) snapshotCreationTime(ctx context.Context, rec *util.SnapshotResp, stamp string) *timestamppb.Timestamp {
	if cs.snapTimes != nil {
		t, err := cs.snapTimes.SnapshotCreated(ctx, stamp)
		if err == nil {
			return timestamppb.New(t)
		}
		klog.Warningf("snapshotCreationTime: stamp %s: %v; using record time %s", stamp, err, rec.Created)
	}
	return parseSnapshotTime(rec.Created)
}
