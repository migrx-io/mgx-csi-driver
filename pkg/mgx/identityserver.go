package mgx

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"

	csicommon "github.com/migrx-io/mgx-csi-driver/pkg/csi-common"
)

type identityServer struct {
	csi.UnimplementedIdentityServer
	driver *csicommon.CSIDriver
}

func newIdentityServer(d *csicommon.CSIDriver) *identityServer {
	return &identityServer{
		UnimplementedIdentityServer: csi.UnimplementedIdentityServer{},
		driver:                      d,
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
