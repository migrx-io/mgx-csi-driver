package mgx

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"

	csicommon "github.com/migrx-io/mgx-csi-driver/pkg/csi-common"
)

type identityServer struct {
	csi.UnimplementedIdentityServer
	driver  *csicommon.CSIDriver
	name    string
	version string
}

func newIdentityServer(d *csicommon.CSIDriver, name, version string) *identityServer {
	return &identityServer{
		UnimplementedIdentityServer: csi.UnimplementedIdentityServer{},
		driver:                      d,
		name:                        name,
		version:                     version,
	}
}

func (*identityServer) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

func (*identityServer) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	// You can add readiness checks here if needed.
	// For now, return OK to satisfy sidecars
	return &csi.ProbeResponse{}, nil
}

func (ids *identityServer) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          ids.name,
		VendorVersion: ids.version,
	}, nil
}
