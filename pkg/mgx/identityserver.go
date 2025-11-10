package mgx

import (
	csicommon "github.com/migrx-io/mgx-csi-driver/pkg/csi-common"
)

type identityServer struct {
	*csicommon.DefaultIdentityServer
	name    string
	version string
}

func newIdentityServer(d *csicommon.CSIDriver, name, version string) *identityServer {
	return &identityServer{
		DefaultIdentityServer: csicommon.NewDefaultIdentityServer(d),
		name:                  name,
		version:               version,
	}
}
