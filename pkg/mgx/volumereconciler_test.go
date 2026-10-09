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
	lastUsedKey  = "migrx.io/last-used"
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

func newTestPV(lastUsed time.Time) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: testPV, Annotations: map[string]string{}},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "ns", Name: "data"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: "csi.migrx.io", VolumeHandle: testVolumeID},
			},
		},
	}
	if !lastUsed.IsZero() {
		pv.Annotations[lastUsedKey] = lastUsed.Format(time.RFC3339)
	}
	return pv
}

func newTestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "app"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
			},
		}}},
	}
}

func newTestReconciler(idler *fakeIdler, pv *corev1.PersistentVolume, pod *corev1.Pod) (*VolumeReconciler, *fake.Clientset) {
	objs := []runtime.Object{pv}
	if pod != nil {
		objs = append(objs, pod)
	}
	client := fake.NewSimpleClientset(objs...)
	return &VolumeReconciler{kubeClient: client, idle: 10 * time.Minute, timeout: 2, cs: idler}, client
}

func getLastUsed(t *testing.T, client *fake.Clientset) string {
	t.Helper()
	pv, err := client.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PV: %v", err)
	}
	return pv.Annotations[lastUsedKey]
}

// A volume in use for longer than the idle timeout must not be stopped on the
// first pass after its pod goes away (e.g. a pod restart): last-used has to be
// refreshed on every pass while the volume is in use.
func TestReconcileRefreshesLastUsedWhileInUse(t *testing.T) {
	idler := &fakeIdler{}
	firstSeen := time.Now().Add(-3 * time.Hour)
	r, client := newTestReconciler(idler, newTestPV(firstSeen), newTestPod())

	r.reconcile(context.Background())

	got, err := time.Parse(time.RFC3339, getLastUsed(t, client))
	if err != nil {
		t.Fatalf("parse last-used: %v", err)
	}
	if time.Since(got) > time.Minute {
		t.Fatalf("last-used not refreshed while in use: %s", got)
	}
	if len(idler.unidled) != 1 || len(idler.idled) != 0 {
		t.Fatalf("in use: want 1 unidle and 0 idle, got unidle=%v idle=%v", idler.unidled, idler.idled)
	}

	// pod deleted (restart gap): next pass must not stop the volume
	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "app", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	r.reconcile(context.Background())

	if len(idler.idled) != 0 {
		t.Fatalf("volume stopped right after its pod went away: idle=%v", idler.idled)
	}
}

func TestReconcileStopsVolumeUnusedLongerThanIdle(t *testing.T) {
	idler := &fakeIdler{}
	r, client := newTestReconciler(idler, newTestPV(time.Now().Add(-11*time.Minute)), nil)

	r.reconcile(context.Background())

	if len(idler.idled) != 1 || idler.idled[0] != testVolumeID {
		t.Fatalf("want %s stopped, got %v", testVolumeID, idler.idled)
	}
	if v := getLastUsed(t, client); v != "" {
		t.Fatalf("last-used should be cleared after stop, got %q", v)
	}
}

func TestReconcileKeepsVolumeUnusedShorterThanIdle(t *testing.T) {
	idler := &fakeIdler{}
	r, _ := newTestReconciler(idler, newTestPV(time.Now().Add(-5*time.Minute)), nil)

	r.reconcile(context.Background())

	if len(idler.idled) != 0 {
		t.Fatalf("volume unused for 5m stopped with 10m idle: %v", idler.idled)
	}
}
