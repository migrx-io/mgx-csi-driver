package mgx

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	testPV       = "pvc-1"
	testVolumeID = "vol-1"
	testNS       = "ns"
	testPod      = "app"
)

// fakeIdler records which volumes the reconciler stopped and started.
type fakeIdler struct {
	idled   []string
	unidled []string
}

func (f *fakeIdler) IdleVolume(volumeID string) error {
	f.idled = append(f.idled, volumeID)
	return nil
}

func (f *fakeIdler) UnIdleVolume(volumeID string) error {
	f.unidled = append(f.unidled, volumeID)
	return nil
}

func newTestPV(annotations map[string]string) *corev1.PersistentVolume {
	if annotations == nil {
		annotations = map[string]string{}
	}
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: testPV, Annotations: annotations},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: testNS, Name: "data"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: "csi.migrx.io", VolumeHandle: testVolumeID},
			},
		},
	}
}

func newTestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: testPod},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
			},
		}}},
	}
}

func ago(d time.Duration) string {
	return time.Now().Add(-d).UTC().Format(time.RFC3339)
}

type reconcilerFixture struct {
	t      *testing.T
	r      *VolumeReconciler
	client *fake.Clientset
	idler  *fakeIdler
}

func newFixture(t *testing.T, pv *corev1.PersistentVolume, withPod bool) *reconcilerFixture {
	objs := []runtime.Object{pv}
	if withPod {
		objs = append(objs, newTestPod())
	}
	client := fake.NewSimpleClientset(objs...)
	idler := &fakeIdler{}
	return &reconcilerFixture{
		t:      t,
		r:      &VolumeReconciler{kubeClient: client, idle: 10 * time.Minute, timeout: 2, cs: idler},
		client: client,
		idler:  idler,
	}
}

func (f *reconcilerFixture) reconcile() {
	f.r.reconcile(context.Background())
}

func (f *reconcilerFixture) annotations() map[string]string {
	f.t.Helper()
	pv, err := f.client.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	if err != nil {
		f.t.Fatalf("get PV: %v", err)
	}
	return pv.Annotations
}

// patches counts PV patch calls; updates counts full-object PV updates (must stay 0).
func (f *reconcilerFixture) writes() (patches, updates int) {
	for _, a := range f.client.Actions() {
		if a.GetResource().Resource != "persistentvolumes" {
			continue
		}
		switch a.GetVerb() {
		case "patch":
			patches++
		case "update":
			updates++
		}
	}
	return patches, updates
}

func (f *reconcilerFixture) wantWrites(patches int) {
	f.t.Helper()
	p, u := f.writes()
	if p != patches || u != 0 {
		f.t.Fatalf("want %d PV patches and 0 updates, got %d patches and %d updates", patches, p, u)
	}
}

func (f *reconcilerFixture) wantUnusedSinceRecent() {
	f.t.Helper()
	v, ok := f.annotations()[unusedSinceKey]
	if !ok {
		f.t.Fatalf("%s not set", unusedSinceKey)
	}
	ts, err := time.Parse(time.RFC3339, v)
	if err != nil || time.Since(ts) > time.Minute {
		f.t.Fatalf("%s = %q, want about now", unusedSinceKey, v)
	}
}

func (f *reconcilerFixture) setPod(present bool) {
	f.t.Helper()
	ctx := context.Background()
	var err error
	if present {
		_, err = f.client.CoreV1().Pods(testNS).Create(ctx, newTestPod(), metav1.CreateOptions{})
	} else {
		err = f.client.CoreV1().Pods(testNS).Delete(ctx, testPod, metav1.DeleteOptions{})
	}
	if err != nil {
		f.t.Fatalf("set pod present=%v: %v", present, err)
	}
}

// Steady state in use: no PV writes at all, however many passes.
func TestReconcileInUseNoWrites(t *testing.T) {
	f := newFixture(t, newTestPV(nil), true)

	for range 3 {
		f.reconcile()
	}

	f.wantWrites(0)
	if len(f.idler.unidled) != 3 || len(f.idler.idled) != 0 {
		t.Fatalf("want 3 unidle and 0 idle, got unidle=%v idle=%v", f.idler.unidled, f.idler.idled)
	}
}

// The case that broke a pod restart: a volume in use for hours whose pod goes
// away must not be stopped on the next pass; idle time counts from that pass.
func TestReconcilePodRestartGapDoesNotStop(t *testing.T) {
	f := newFixture(t, newTestPV(map[string]string{legacyLastUsedKey: ago(3 * time.Hour)}), true)

	f.reconcile() // in use: stale legacy annotation removed
	f.wantWrites(1)
	if a := f.annotations(); len(a) != 0 {
		t.Fatalf("annotations should be cleared while in use, got %v", a)
	}

	f.setPod(false)
	f.reconcile() // just became unused: start counting
	f.wantWrites(2)
	f.wantUnusedSinceRecent()
	if len(f.idler.idled) != 0 {
		t.Fatalf("volume stopped right after its pod went away: %v", f.idler.idled)
	}

	f.setPod(true)
	f.reconcile() // back in use: clear
	f.wantWrites(3)
	if _, ok := f.annotations()[unusedSinceKey]; ok {
		t.Fatalf("%s should be cleared once the volume is in use again", unusedSinceKey)
	}
	if len(f.idler.idled) != 0 {
		t.Fatalf("volume should never have been stopped: %v", f.idler.idled)
	}
}

func TestReconcileUnusedStartsCounting(t *testing.T) {
	f := newFixture(t, newTestPV(nil), false)

	f.reconcile()
	f.reconcile() // already counting: no second write

	f.wantWrites(1)
	f.wantUnusedSinceRecent()
	if len(f.idler.idled) != 0 {
		t.Fatalf("volume stopped as soon as it became unused: %v", f.idler.idled)
	}
}

func TestReconcileUnusedShorterThanIdleKept(t *testing.T) {
	f := newFixture(t, newTestPV(map[string]string{unusedSinceKey: ago(5 * time.Minute)}), false)

	f.reconcile()

	f.wantWrites(0)
	if len(f.idler.idled) != 0 {
		t.Fatalf("volume unused for 5m stopped with 10m idle: %v", f.idler.idled)
	}
}

func TestReconcileUnusedLongerThanIdleStopped(t *testing.T) {
	since := ago(11 * time.Minute)
	f := newFixture(t, newTestPV(map[string]string{unusedSinceKey: since}), false)

	f.reconcile()

	if len(f.idler.idled) != 1 || f.idler.idled[0] != testVolumeID {
		t.Fatalf("want %s stopped, got %v", testVolumeID, f.idler.idled)
	}
	// still unused: unused-since is kept, nothing written
	f.wantWrites(0)
	if got := f.annotations()[unusedSinceKey]; got != since {
		t.Fatalf("%s changed on stop: %q -> %q", unusedSinceKey, since, got)
	}
}

// A stale legacy last-used on an unused PV must not count as idle time.
func TestReconcileUnusedLegacyAnnotationRestartsCount(t *testing.T) {
	f := newFixture(t, newTestPV(map[string]string{legacyLastUsedKey: ago(3 * time.Hour)}), false)

	f.reconcile()

	f.wantWrites(1)
	f.wantUnusedSinceRecent()
	if _, ok := f.annotations()[legacyLastUsedKey]; ok {
		t.Fatalf("%s should be removed", legacyLastUsedKey)
	}
	if len(f.idler.idled) != 0 {
		t.Fatalf("stale legacy annotation caused a stop: %v", f.idler.idled)
	}
}

func TestReconcileUnusedBadValueRestartsCount(t *testing.T) {
	f := newFixture(t, newTestPV(map[string]string{unusedSinceKey: "garbage"}), false)

	f.reconcile()

	f.wantWrites(1)
	f.wantUnusedSinceRecent()
	if len(f.idler.idled) != 0 {
		t.Fatalf("unreadable %s caused a stop: %v", unusedSinceKey, f.idler.idled)
	}
}
