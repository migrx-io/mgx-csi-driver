package mgx

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

const (
	testSnapUID   = "b3902a15-5e27-4fef-851b-357e0c7f50fc"
	testSnapStamp = snapshotNamePrefix + testSnapUID
)

var (
	vsCreated      = time.Date(2026, 10, 9, 16, 44, 51, 0, time.UTC)
	contentCreated = time.Date(2026, 10, 9, 16, 44, 52, 0, time.UTC)
)

func newVolumeSnapshot(uid string, created time.Time) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("snapshot.storage.k8s.io/v1")
	u.SetKind("VolumeSnapshot")
	u.SetNamespace("bench-mgx")
	u.SetName("snap-2")
	u.SetUID(types.UID(uid))
	u.SetCreationTimestamp(metav1.NewTime(created))
	return u
}

func newVolumeSnapshotContent(uid string, created time.Time) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"volumeSnapshotRef": map[string]any{"namespace": "bench-mgx", "name": "snap-2"},
		},
	}}
	u.SetAPIVersion("snapshot.storage.k8s.io/v1")
	u.SetKind("VolumeSnapshotContent")
	u.SetName(snapshotContentNamePrefix + uid)
	u.SetCreationTimestamp(metav1.NewTime(created))
	return u
}

func newTestTimer(objs ...runtime.Object) *k8sSnapshotTimer {
	listKinds := map[schema.GroupVersionResource]string{
		volumeSnapshotGVR:        "VolumeSnapshotList",
		volumeSnapshotContentGVR: "VolumeSnapshotContentList",
	}
	return &k8sSnapshotTimer{
		client: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...),
	}
}

func TestSnapshotCreatedFromVolumeSnapshot(t *testing.T) {
	timer := newTestTimer(newVolumeSnapshotContent(testSnapUID, contentCreated), newVolumeSnapshot(testSnapUID, vsCreated))

	got, err := timer.SnapshotCreated(context.Background(), testSnapStamp)
	if err != nil {
		t.Fatalf("SnapshotCreated: %v", err)
	}
	if !got.Equal(vsCreated) {
		t.Fatalf("want VolumeSnapshot time %s, got %s", vsCreated, got)
	}
}

// VolumeSnapshot deleted (content retained), or the name now belongs to a
// different VolumeSnapshot: the content's own creation time is used.
func TestSnapshotCreatedFallsBackToContent(t *testing.T) {
	for name, objs := range map[string][]runtime.Object{
		"snapshot deleted":  {newVolumeSnapshotContent(testSnapUID, contentCreated)},
		"snapshot replaced": {newVolumeSnapshotContent(testSnapUID, contentCreated), newVolumeSnapshot("other-uid", vsCreated)},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := newTestTimer(objs...).SnapshotCreated(context.Background(), testSnapStamp)
			if err != nil {
				t.Fatalf("SnapshotCreated: %v", err)
			}
			if !got.Equal(contentCreated) {
				t.Fatalf("want content time %s, got %s", contentCreated, got)
			}
		})
	}
}

func TestSnapshotCreatedErrors(t *testing.T) {
	timer := newTestTimer()

	if _, err := timer.SnapshotCreated(context.Background(), "manual-point"); err == nil {
		t.Fatal("stamp without snapshot-<uid> form: want error")
	}
	if _, err := timer.SnapshotCreated(context.Background(), testSnapStamp); err == nil {
		t.Fatal("missing VolumeSnapshotContent: want error")
	}
}

type fakeSnapshotTimer struct {
	t   time.Time
	err error
}

func (f fakeSnapshotTimer) SnapshotCreated(context.Context, string) (time.Time, error) {
	return f.t, f.err
}

func TestSnapshotCreationTime(t *testing.T) {
	rec := &util.SnapshotResp{Created: "2026-10-09T01:33:38.628000"}
	recTime := time.Date(2026, 10, 9, 1, 33, 38, 628000000, time.UTC)

	for name, tc := range map[string]struct {
		timer snapshotTimer
		want  time.Time
	}{
		"lookup ok":     {fakeSnapshotTimer{t: vsCreated}, vsCreated},
		"lookup failed": {fakeSnapshotTimer{err: errors.New("not found")}, recTime},
		"no client":     {nil, recTime},
	} {
		t.Run(name, func(t *testing.T) {
			cs := &controllerServer{snapTimes: tc.timer}
			got := cs.snapshotCreationTime(context.Background(), rec, testSnapStamp).AsTime()
			if !got.Equal(tc.want) {
				t.Fatalf("want %s, got %s", tc.want, got)
			}
		})
	}
}
