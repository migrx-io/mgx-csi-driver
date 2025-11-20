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

	pvList, err := r.kubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("list PVs failed: %v", err)
		return
	}

	vaList, err := r.kubeClient.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("list VAs failed: %v", err)
		return
	}

	attachedPV := map[string]bool{}
	for i := range vaList.Items {
		va := &vaList.Items[i]
		if va.Spec.Source.PersistentVolumeName != nil {
			attachedPV[*va.Spec.Source.PersistentVolumeName] = va.Status.Attached
		}
	}

	now := time.Now()

	for i := range pvList.Items {
		pv := &pvList.Items[i]
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != "csi.migrx.io" {
			continue
		}

		volumeID := pv.Spec.CSI.VolumeHandle

		// If attached → skip
		if attachedPV[pv.Name] {
			continue
		}

		// last-used annotation
		lastUsedStr := pv.Annotations["migrx.io/last-used"]
		lastUsed, _ := time.Parse(time.RFC3339, lastUsedStr)

		if lastUsed.IsZero() {
			continue
		}

		if now.Sub(lastUsed) > r.idle {
			klog.Infof("Volumereconciler stopping idle volume %s", volumeID)

			// init clinet and stop volume
			if err := r.cs.IdleVolume(volumeID); err != nil {
				klog.Errorf("stop volume failed %s: %v", volumeID, err)
				continue
			}
		}
	}
}
