package mgx

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
)

type VolumeReconciler struct {
	kubeClient kubernetes.Interface
	idle       time.Duration
	timeout    int
	cs         *controllerServer
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
		pv := &pvList.Items[i]

		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != "csi.migrx.io" {
			continue
		}

		// Extract claimRef
		if pv.Spec.ClaimRef == nil {
			klog.Infof("PV %s has no ClaimRef → unused", pv.Name)
			continue
		}

		pvcKey := pv.Spec.ClaimRef.Namespace + "/" + pv.Spec.ClaimRef.Name
		volumeID := pv.Spec.CSI.VolumeHandle

		// last-used annotation
		lastUsedStr := pv.Annotations["migrx.io/last-used"]
		lastUsed, _ := time.Parse(time.RFC3339, lastUsedStr)

		// If attached → skip
		if attachedPV[pvcKey] {
			klog.V(5).Infof("VolumeReconciler volume attached: %s", pv.Name)

			// check and UNIdle first
			if err := r.cs.UnIdleVolume(volumeID); err != nil {
				klog.Errorf("unidle volume failed %s: %v", volumeID, err)
				continue
			}

			// attached but not tracked
			if lastUsed.IsZero() {
				r.updateLastUsedAnnotation(pv.Name, &now)
			}

			continue
		}

		klog.V(5).Infof("VolumeReconciler volume is not attached: %s", lastUsed)

		// not attached yet and not have date set
		if lastUsed.IsZero() {
			r.updateLastUsedAnnotation(pv.Name, &now)
			continue
		}

		if now.Sub(lastUsed) > r.idle {
			klog.Infof("Volumereconciler stopping idle volume %s", volumeID)

			// init clinet and stop volume
			if err := r.cs.IdleVolume(volumeID); err != nil {
				klog.Errorf("idle volume failed %s: %v", volumeID, err)
				continue
			}
			// clear time
			r.updateLastUsedAnnotation(pv.Name, nil)
		}
	}
}

func (r *VolumeReconciler) updateLastUsedAnnotation(volumeID string, t *time.Time) {
	ctx := context.Background()

	klog.Infof("nodeServer updating last-used annotation for PV: %s", volumeID)

	pv, err := r.kubeClient.CoreV1().PersistentVolumes().Get(ctx, volumeID, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("nodeServer failed to get PV %s: %v", volumeID, err)
		return
	}

	if pv.Annotations == nil {
		pv.Annotations = map[string]string{}
	}

	const key = "migrx.io/last-used"

	if t == nil {
		// Delete annotation if present
		if _, exists := pv.Annotations[key]; exists {
			delete(pv.Annotations, key)
			klog.Infof("removed last-used annotation from PV %s", volumeID)
		} else {
			// nothing to delete
			return
		}
	} else {
		// Set/update annotation
		pv.Annotations[key] = t.Format(time.RFC3339)
	}

	_, err = r.kubeClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("nodeServer failed to update PV %s annotation: %v", volumeID, err)
		return
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
