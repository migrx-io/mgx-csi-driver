package mgx

import (
	csicommon "github.com/migrx-io/mgx-csi-driver/pkg/csi-common"
	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

type identityServer struct {
	*csicommon.DefaultIdentityServer
	name    string
	version string
}

func newIdentityServer(d *csicommon.CSIDriver, conf *util.Config) *identityServer {
	return &identityServer{
		DefaultIdentityServer: csicommon.NewDefaultIdentityServer(d),
		name:                  conf.DriverName,
		version:               conf.DriverVersion,
	}
}
