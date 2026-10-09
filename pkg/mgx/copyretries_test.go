package mgx

import (
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

const snapshotNotFoundReply = "Snapshot not found"

// count is how many times op was sent to the fake gateway.
func (g *fakeGateway) count(op string) int {
	n := 0
	for _, o := range g.ops() {
		if o == op {
			n++
		}
	}
	return n
}

func TestCopyRetries(t *testing.T) {
	c := newCopyRetries(2)
	for want := 1; want <= 2; want++ {
		if n, ok := c.retry("a"); !ok || n != want {
			t.Fatalf("retry %d: got (%d, %v), want (%d, true)", want, n, ok, want)
		}
	}
	if n, ok := c.retry("a"); ok || n != 2 {
		t.Fatalf("over the cap: got (%d, %v), want (2, false)", n, ok)
	}
	if _, ok := c.retry("b"); !ok {
		t.Fatal("keys are counted separately")
	}
	c.reset("a")
	if n, ok := c.retry("a"); !ok || n != 1 {
		t.Fatalf("after reset: got (%d, %v), want (1, true)", n, ok)
	}
}

func TestCopyRetriesUnlimited(t *testing.T) {
	for name, c := range map[string]*copyRetries{"max 0": newCopyRetries(0), "nil": nil} {
		for i := range 10 {
			if _, ok := c.retry("a"); !ok {
				t.Fatalf("%s: retry %d refused", name, i+1)
			}
		}
		c.reset("a")
	}
}

// A restore point whose run keeps FAILING is re-armed up to the cap, then
// CreateSnapshot gives up with FailedPrecondition and stops sending snapshot_add.
func TestArmRestorePointRetryCap(t *testing.T) {
	client, g := newFakeClient(t, func(op string, _ map[string]any) (any, string) {
		if op == opSnapshotShow {
			return map[string]any{"status": "FAILED", "stamp": "s4", "increments": `["s1"]`, "error": "rclone restarted"}, ""
		}
		return okReply, ""
	})
	retries := newCopyRetries(2)
	req := &csi.CreateSnapshotRequest{SourceVolumeId: "vol-1", Name: "s4"}

	for i := 1; i <= 2; i++ {
		if _, err := armRestorePoint(client, retries, req, "vol-1", "s4", "", &util.LvolResp{}); err != nil {
			t.Fatalf("re-arm %d: unexpected error: %v", i, err)
		}
	}
	if n := g.count("snapshot_add"); n != 2 {
		t.Fatalf("want 2 snapshot_add re-arms, got %d", n)
	}

	_, err := armRestorePoint(client, retries, req, "vol-1", "s4", "", &util.LvolResp{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("over the cap: want FailedPrecondition, got %v", err)
	}
	if n := g.count("snapshot_add"); n != 2 {
		t.Fatalf("snapshot_add sent after giving up: %d calls", n)
	}
}

// A restore that keeps FAILING is re-armed up to the cap; then the driver drops
// its record and partial target (snapshot_del purge=yes) and never schedules it
// again, so nothing is left behind once the PVC is deleted.
// failingRestoreReply answers like a backend whose restore restoreName keeps
// FAILING until snapshot_del drops it; any other record is a READY backup.
func failingRestoreReply(restoreName string) func(op string, data map[string]any) (any, string) {
	deleted := false
	return func(op string, data map[string]any) (any, string) {
		switch {
		case op == "snapshot_del":
			deleted = true
		case op != opSnapshotShow:
		case data["name"] != restoreName:
			return map[string]any{"name": data["name"], "kind": "snapshot", "status": "READY"}, ""
		case deleted:
			return nil, snapshotNotFoundReply
		default:
			return map[string]any{"name": restoreName, "kind": "restore", "status": "FAILED", "error": "rclone restarted"}, ""
		}
		return okReply, ""
	}
}

func TestRestoreVolumeGivesUpAndCleansUp(t *testing.T) {
	restoreName := restoreNamePrefix + util.PvcToVolName("pvc-1")
	client, g := newFakeClient(t, failingRestoreReply(restoreName))
	cs := &controllerServer{copyRetries: newCopyRetries(1)}
	req := createReq(1024 * 1024 * 1024)

	if err := cs.restoreVolume(req, client); status.Code(err) != codes.Aborted {
		t.Fatalf("first failure: want Aborted (re-armed), got %v", err)
	}
	if g.count("restore_add") != 1 {
		t.Fatalf("first failure: want 1 restore_add, got %d", g.count("restore_add"))
	}

	for i := range 2 {
		err := cs.restoreVolume(req, client)
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "rclone restarted") {
			t.Fatalf("call %d after the cap: want FailedPrecondition with the last error, got %v", i+2, err)
		}
	}
	if g.count("restore_add") != 1 {
		t.Fatalf("restore re-scheduled after giving up: %d restore_add", g.count("restore_add"))
	}
	del := g.call("snapshot_del")
	if del == nil || del.data["name"] != restoreName || del.data["purge"] != "yes" {
		t.Fatalf("want snapshot_del of %s with purge=yes, got %+v", restoreName, del)
	}
	if g.count("snapshot_del") != 1 {
		t.Fatalf("want 1 snapshot_del, got %d", g.count("snapshot_del"))
	}
}
