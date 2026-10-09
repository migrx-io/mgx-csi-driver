package mgx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

// okReply is the plugin's plain success payload.
const okReply = "ok"

// opSnapshotShow is the plugin op the fakes answer with a record.
const opSnapshotShow = "snapshot_show"

// fakeGateway stands in for the mgx API gateway: each plugin op is answered by
// reply (data, error string) and recorded with its request data.
type fakeGateway struct {
	mu    sync.Mutex
	calls []fakeCall
	reply func(op string, data map[string]any) (any, string)
}

type fakeCall struct {
	op   string
	data map[string]any
}

func (g *fakeGateway) ops() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []string
	for _, c := range g.calls {
		out = append(out, c.op)
	}
	return out
}

func (g *fakeGateway) call(op string) *fakeCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := range g.calls {
		if g.calls[i].op == op {
			return &g.calls[i]
		}
	}
	return nil
}

func newFakeClient(t *testing.T, reply func(op string, data map[string]any) (any, string)) (*util.NodeNVMf, *fakeGateway) {
	t.Helper()
	g := &fakeGateway{reply: reply}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Context struct {
				Op string `json:"op"`
			} `json:"context"`
			Data map[string]any `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		g.mu.Lock()
		g.calls = append(g.calls, fakeCall{op: req.Context.Op, data: req.Data})
		g.mu.Unlock()

		data, errStr := g.reply(req.Context.Op, req.Data)
		resp := map[string]any{"data": data}
		if errStr != "" {
			resp = map[string]any{"error": errStr}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	client := &util.NodeNVMf{Client: &util.RPCClient{
		Protocol:   "http",
		Nodes:      []string{strings.TrimPrefix(srv.URL, "http://")},
		Cluster:    "c1",
		Token:      "token",
		HTTPClient: srv.Client(),
	}}
	return client, g
}

// restoreReply answers snapshot_show with a restore record in the given status
// (or "Snapshot not found" when status is empty) and ok for everything else.
func restoreReply(status string) func(string, map[string]any) (any, string) {
	return func(op string, _ map[string]any) (any, string) {
		if op == opSnapshotShow {
			if status == "" {
				return nil, "Snapshot not found"
			}
			return map[string]any{"name": "restore-vol-1", "kind": "restore", "status": status}, ""
		}
		return okReply, ""
	}
}

func TestCleanupRestore(t *testing.T) {
	cases := []struct {
		status    string
		wantDone  bool
		wantOps   []string
		wantPurge any
	}{
		{"", true, []string{"snapshot_show"}, nil},
		{"RUNNING", false, []string{"snapshot_show", "snapshot_stop"}, nil},
		{"STOPPING", false, []string{"snapshot_show"}, nil},
		{"REWINDING", false, []string{"snapshot_show"}, nil},
		{"DELETING", true, []string{"snapshot_show"}, nil},
		{"DELETED", true, []string{"snapshot_show"}, nil},
		{"STOPPED", true, []string{"snapshot_show", "snapshot_del"}, "yes"},
		{"FAILED", true, []string{"snapshot_show", "snapshot_del"}, "yes"},
		{"PENDING", true, []string{"snapshot_show", "snapshot_del"}, "yes"},
		// provisioned: the target is the volume itself, keep its data
		{"READY", true, []string{"snapshot_show", "snapshot_del"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			client, g := newFakeClient(t, restoreReply(tc.status))
			done, err := cleanupRestore(client, "restore-vol-1")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if done != tc.wantDone {
				t.Errorf("done = %v, want %v", done, tc.wantDone)
			}
			if got := strings.Join(g.ops(), ","); got != strings.Join(tc.wantOps, ",") {
				t.Errorf("ops = %s, want %s", got, strings.Join(tc.wantOps, ","))
			}
			if del := g.call("snapshot_del"); del != nil {
				if del.data["purge"] != tc.wantPurge {
					t.Errorf("purge = %v, want %v", del.data["purge"], tc.wantPurge)
				}
				if _, ok := del.data["del_stamp"]; ok {
					t.Errorf("restore cleanup must delete the whole record, got del_stamp")
				}
			}
		})
	}
}

func TestCleanupRestoreDeleteRefusedRetries(t *testing.T) {
	client, _ := newFakeClient(t, func(op string, _ map[string]any) (any, string) {
		switch op {
		case opSnapshotShow:
			return map[string]any{"name": "restore-vol-1", "status": "STOPPED"}, ""
		case "snapshot_del":
			return nil, "Snapshot is REWINDING, retry once it settles"
		}
		return okReply, ""
	})
	done, err := cleanupRestore(client, "restore-vol-1")
	if err != nil || done {
		t.Fatalf("done=%v err=%v, want not done and no error (retry)", done, err)
	}
}

func createReq(requiredBytes int64) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:          "pvc-1",
		CapacityRange: &csi.CapacityRange{RequiredBytes: requiredBytes},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "vol-src@s1"},
			},
		},
	}
}

func TestGrowRestored(t *testing.T) {
	const gib = int64(1024 * 1024 * 1024)
	cases := []struct {
		name      string
		size      int
		status    string
		wantGrown bool
		wantOps   []string
	}{
		{"big enough and ready", 2048, VolumeStatusReady, true, nil},
		{"larger than requested", 4096, VolumeStatusReady, true, nil},
		{"too small, ready -> stop", 1024, VolumeStatusReady, false, []string{"volume_stop"}},
		{"too small, stopped -> resize", 1024, VolumeStatusStopped, false, []string{"volume_resize"}},
		{"resized, stopped -> start", 2048, VolumeStatusStopped, false, []string{"volume_start"}},
		{"too small, still provisioning -> wait", 1024, "PENDING", false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, g := newFakeClient(t, func(string, map[string]any) (any, string) { return okReply, "" })
			cs := &controllerServer{}
			vol := &util.LvolResp{Size: tc.size, Status: tc.status}
			grown, err := cs.growRestored("vol-1", vol, createReq(2*gib), client)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if grown != tc.wantGrown {
				t.Errorf("grown = %v, want %v", grown, tc.wantGrown)
			}
			if got := strings.Join(g.ops(), ","); got != strings.Join(tc.wantOps, ",") {
				t.Errorf("ops = %s, want %s", got, strings.Join(tc.wantOps, ","))
			}
			if rs := g.call("volume_resize"); rs != nil && rs.data["size"] != float64(2048) {
				t.Errorf("resize size = %v, want 2048", rs.data["size"])
			}
		})
	}
}

func TestStampCommitted(t *testing.T) {
	cases := []struct {
		name string
		rec  util.SnapshotResp
		want bool
	}{
		{"json string chain", util.SnapshotResp{Status: "READY", Stamp: "s2", Increments: `["s1","s2"]`}, true},
		{"list chain", util.SnapshotResp{Status: "READY", Stamp: "s2", Increments: []any{"s1", "s2"}}, true},
		{"older point while newer runs", util.SnapshotResp{Status: "RUNNING", Stamp: "s3", Increments: `["s1","s2"]`}, true},
		{"armed not committed", util.SnapshotResp{Status: "RUNNING", Stamp: "s2", Increments: `["s1"]`}, false},
		{"ready on another stamp after rewind", util.SnapshotResp{Status: "READY", Stamp: "", Increments: `["s1"]`}, false},
		{"flat backup ready", util.SnapshotResp{Status: "READY", Stamp: "s2", Increments: ""}, true},
		{"flat backup running", util.SnapshotResp{Status: "RUNNING", Stamp: "s2", Increments: nil}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stampCommitted(&tc.rec, "s2"); got != tc.want {
				t.Errorf("stampCommitted = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestListEntries(t *testing.T) {
	recs := []*util.SnapshotResp{
		{Name: "vol-b", Kind: "snapshot", Volume: "vol-b", Size: 10, Status: "READY", Increments: `["b1"]`},
		{Name: "vol-a", Kind: "snapshot", Volume: "vol-a", Size: 20, Status: "RUNNING", Stamp: "a3", Increments: `["a1","a2"]`},
		{Name: "restore-vol-x", Kind: "restore", Volume: "vol-a", Status: "RUNNING"},
		{Name: "vol-flat", Kind: "snapshot", Volume: "vol-flat", Status: "READY", Stamp: "f1"},
	}

	var ids []string
	for _, s := range listEntries(recs, "", "") {
		ids = append(ids, s.GetSnapshotId())
		if !s.GetReadyToUse() {
			t.Errorf("%s not ready", s.GetSnapshotId())
		}
	}
	if got, want := strings.Join(ids, ","), "vol-a@a1,vol-a@a2,vol-b@b1,vol-flat@f1"; got != want {
		t.Errorf("entries = %s, want %s", got, want)
	}

	byVol := listEntries(recs, "vol-a", "")
	if len(byVol) != 2 || byVol[0].GetSizeBytes() != 20*bytesPerMB {
		t.Errorf("source_volume_id filter: %v", byVol)
	}
	one := listEntries(recs, "", "a2")
	if len(one) != 1 || one[0].GetSnapshotId() != "vol-a@a2" {
		t.Errorf("stamp filter: %v", one)
	}
}

func TestPaginateSnapshots(t *testing.T) {
	var entries []*csi.Snapshot
	for _, id := range []string{"r@1", "r@2", "r@3"} {
		entries = append(entries, &csi.Snapshot{SnapshotId: id})
	}

	p1, err := paginateSnapshots(entries, "", 2)
	if err != nil || len(p1.GetEntries()) != 2 || p1.GetNextToken() != "2" {
		t.Fatalf("page 1: %v %v", p1, err)
	}
	p2, err := paginateSnapshots(entries, p1.GetNextToken(), 2)
	if err != nil || len(p2.GetEntries()) != 1 || p2.GetNextToken() != "" {
		t.Fatalf("page 2: %v %v", p2, err)
	}
	all, _ := paginateSnapshots(entries, "", 0)
	if len(all.GetEntries()) != 3 {
		t.Errorf("max 0 should return all, got %d", len(all.GetEntries()))
	}
	if _, err := paginateSnapshots(entries, "bogus", 0); err == nil {
		t.Errorf("invalid token should error")
	}
}

func TestHeldByOtherStamp(t *testing.T) {
	cases := []struct {
		status, stamp string
		want          bool
	}{
		{"PENDING", "s3", true},
		{"RUNNING", "s3", true},
		{"REWINDING", "s3", true},
		{"STOPPING", "s3", true},
		{"STOPPED", "s3", true},
		{"FAILED", "s3", true},
		{"DELETING", "s3", true},
		{"READY", "s3", false},
		{"DELETED", "s3", false},
		{"PENDING", "s4", false}, // our own stamp
		{"FAILED", "s4", false},
	}
	for _, tc := range cases {
		rec := &util.SnapshotResp{Status: tc.status, Stamp: tc.stamp}
		if got := heldByOtherStamp(rec, "s4"); got != tc.want {
			t.Errorf("%s on %s: got %v, want %v", tc.status, tc.stamp, got, tc.want)
		}
	}
}

func TestArmRestorePoint(t *testing.T) {
	cases := []struct {
		name    string
		rec     map[string]any // snapshot_show reply; nil = not found
		wantAdd bool
	}{
		{"new record", nil, true},
		{"settled on older point", map[string]any{"status": "READY", "stamp": "s3", "increments": `["s3"]`}, true},
		{"another point pending", map[string]any{"status": "PENDING", "stamp": "s3", "increments": `["s1"]`}, false},
		{"another point running", map[string]any{"status": "RUNNING", "stamp": "s3", "increments": `["s1"]`}, false},
		{"another point failed", map[string]any{"status": "FAILED", "stamp": "s3", "increments": `["s1"]`}, false},
		{"ours armed", map[string]any{"status": "PENDING", "stamp": "s4", "increments": `["s1"]`}, false},
		{"ours failed -> re-arm", map[string]any{"status": "FAILED", "stamp": "s4", "increments": `["s1"]`}, true},
		{"ours committed, newer running", map[string]any{"status": "RUNNING", "stamp": "s5", "increments": `["s4"]`}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, g := newFakeClient(t, func(op string, _ map[string]any) (any, string) {
				if op == opSnapshotShow {
					if tc.rec == nil {
						return nil, "Snapshot not found"
					}
					return tc.rec, ""
				}
				return okReply, ""
			})
			req := &csi.CreateSnapshotRequest{SourceVolumeId: "vol-1", Name: "s4"}
			if _, err := armRestorePoint(client, nil, req, "vol-1", "s4", "", &util.LvolResp{}); err != nil && tc.rec != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if add := g.call("snapshot_add"); (add != nil) != tc.wantAdd {
				t.Errorf("snapshot_add sent = %v, want %v (ops %v)", add != nil, tc.wantAdd, g.ops())
			} else if add != nil && add.data["stamp"] != "s4" {
				t.Errorf("snapshot_add stamp = %v, want s4", add.data["stamp"])
			}
		})
	}
}
