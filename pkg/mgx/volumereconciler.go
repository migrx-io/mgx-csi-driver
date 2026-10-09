package mgx

import (
	"context"
	"encoding/json"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
)

const (
	// unusedSinceKey marks when the reconciler first saw a PV with no pod using it.
	// It is written only on in-use <-> unused transitions.
	unusedSinceKey = "migrx.io/unused-since"
	// legacyLastUsedKey is the old annotation; removed whenever a PV is patched.
	legacyLastUsedKey = "migrx.io/last-used"
)

// volumeIdler stops/starts backend volumes; implemented by controllerServer.
type volumeIdler interface {
	IdleVolume(volumeID string) error
	UnIdleVolume(volumeID string) error
}

type VolumeReconciler struct {
	kubeClient kubernetes.Interface
	idle       time.Duration
	timeout    int
	cs         volumeIdler
}

// Create reconciler
func NewVolumeReconciler(cs *controllerServer, timeout int, idle time.Duration) (*VolumeReconciler, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &VolumeReconciler{
		kubeClient: clientset,
		idle:       idle,
		timeout:    timeout,
		cs:         cs,
	}, nil
}

func (r *VolumeReconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(r.timeout) * time.Minute)

	for {
		select {
		case <-ticker.C:
			r.reconcile(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (r *VolumeReconciler) reconcile(ctx context.Context) {
	klog.Infof("Volumereconciler scanning for idle volumes")

	if r.timeout == 0 {
		klog.Infof("Volumereconciler is disabled timeout == 0")
		return
	}

	pvList, err := r.kubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("list PVs failed: %v", err)
		return
	}

	attachedPV, err := r.BuildPodVolumeUsageMap(ctx)
	if err != nil {
		klog.Errorf("attachedPVPV failed: %v", err)
		return
	}

	now := time.Now()

	for i := range pvList.Items {
		r.reconcilePV(ctx, &pvList.Items[i], attachedPV, now)
	}
}

// reconcilePV tracks when one PV stopped being used and stops its backend
// volume once it has been unused for longer than the idle timeout.
func (r *VolumeReconciler) reconcilePV(ctx context.Context, pv *corev1.PersistentVolume, attachedPV map[string]bool, now time.Time) {
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != "csi.migrx.io" {
		return
	}

	// Extract claimRef
	if pv.Spec.ClaimRef == nil {
		klog.Infof("PV %s has no ClaimRef → unused", pv.Name)
		return
	}

	pvcKey := pv.Spec.ClaimRef.Namespace + "/" + pv.Spec.ClaimRef.Name
	volumeID := pv.Spec.CSI.VolumeHandle

	_, hasLegacy := pv.Annotations[legacyLastUsedKey]
	unusedSinceStr, hasUnusedSince := pv.Annotations[unusedSinceKey]

	// in use: clear unused-since (only on the transition) and make sure it runs
	if attachedPV[pvcKey] {
		klog.V(5).Infof("VolumeReconciler volume attached: %s", pv.Name)

		if hasUnusedSince || hasLegacy {
			r.patchUnusedSince(ctx, pv.Name, nil)
		}

		if err := r.cs.UnIdleVolume(volumeID); err != nil {
			klog.Errorf("unidle volume failed %s: %v", volumeID, err)
		}

		return
	}

	unusedSince, err := time.Parse(time.RFC3339, unusedSinceStr)

	// just became unused (or the value is unreadable): start counting from now
	if !hasUnusedSince || err != nil {
		klog.V(5).Infof("VolumeReconciler volume became unused: %s", pv.Name)
		r.patchUnusedSince(ctx, pv.Name, &now)
		return
	}

	if now.Sub(unusedSince) > r.idle {
		klog.Infof("Volumereconciler stopping idle volume %s, unused since %s", volumeID, unusedSinceStr)

		// unused-since stays: the volume is still unused, and IdleVolume is a
		// no-op once the backend volume is no longer READY
		if err := r.cs.IdleVolume(volumeID); err != nil {
			klog.Errorf("idle volume failed %s: %v", volumeID, err)
		}
	}
}

// patchUnusedSince sets unused-since to t, or removes it when t is nil. The
// legacy last-used annotation is always removed. A merge patch touches only
// these keys, so it can't conflict with other writers of the PV.
func (r *VolumeReconciler) patchUnusedSince(ctx context.Context, pvName string, t *time.Time) {
	annotations := map[string]any{legacyLastUsedKey: nil, unusedSinceKey: nil}
	if t != nil {
		annotations[unusedSinceKey] = t.UTC().Format(time.RFC3339)
	}

	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotations}})
	if err != nil {
		klog.Errorf("Volumereconciler failed to build patch for PV %s: %v", pvName, err)
		return
	}

	klog.Infof("Volumereconciler patching PV %s annotations: %s", pvName, patch)

	_, err = r.kubeClient.CoreV1().PersistentVolumes().Patch(ctx, pvName, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("Volumereconciler failed to patch PV %s: %v", pvName, err)
	}
}

func (r *VolumeReconciler) BuildPodVolumeUsageMap(ctx context.Context) (map[string]bool, error) {
	// 1. List all pods once
	podList, err := r.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// 2. Collect PVC names used by pods
	pvcUsed := map[string]bool{}
	for i := range podList.Items {
		pod := &podList.Items[i]

		for i := range pod.Spec.Volumes {
			vol := &pod.Spec.Volumes[i]

			if vol.PersistentVolumeClaim != nil {
				pvcName := pod.Namespace + "/" + vol.PersistentVolumeClaim.ClaimName
				pvcUsed[pvcName] = true
			}
		}
	}

	return pvcUsed, nil
}
