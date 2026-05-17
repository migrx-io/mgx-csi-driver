package mgx

import (
	"context"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog"

	csicommon "github.com/migrx-io/mgx-csi-driver/pkg/csi-common"
	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

func Run(conf *util.Config) {
	var (
		cd  *csicommon.CSIDriver
		ids *identityServer
		cs  *controllerServer
		ns  *nodeServer

		controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			// PUBLISH_UNPUBLISH_VOLUME makes external-attacher create a
			// VolumeAttachment per (PV, node) pair, which serializes
			// ControllerPublishVolume on the new node behind
			// ControllerUnpublishVolume on the old node. That's where the
			// volume_clean READY-wait lives, so the cross-node Publish-vs-clean
			// race is closed at the attacher layer.
			csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
			csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
			// csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
			csi.ControllerServiceCapability_RPC_GET_VOLUME,
			// csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
			csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
		}
		volumeModes = []csi.VolumeCapability_AccessMode_Mode{
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		}
	)

	cd = csicommon.NewCSIDriver(conf.DriverName, conf.DriverVersion, conf.NodeID)
	if cd == nil {
		klog.Fatalln("Failed to initialize CSI Driver.")
	}
	if conf.IsControllerServer {
		cd.AddControllerServiceCapabilities(controllerCaps)
		cd.AddVolumeCapabilityAccessModes(volumeModes)
	}

	ids = newIdentityServer(cd, conf)

	if conf.IsNodeServer {
		ns = newNodeServer(cd, conf)
	}

	if conf.IsControllerServer {
		cs = newControllerServer(cd, conf)

		// Start volume reconciler
		rec, err := NewVolumeReconciler(cs, conf.Timeout, time.Duration(conf.IdleVolumeMin)*time.Minute)
		if err != nil {
			klog.Fatalf("reconciler init error: %v", err)
		}
		go rec.Run(context.Background())
	}

	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(conf.Endpoint, ids, cs, ns)
	s.Wait()
}
