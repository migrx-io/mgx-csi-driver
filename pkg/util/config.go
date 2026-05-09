package util

const (
	cfgRPCTimeoutSeconds = 60
)

// Config stores parsed command line parameters
type Config struct {
	DriverName    string
	DriverVersion string
	Endpoint      string
	NodeID        string

	NrIoQueues int
	QueueSize  int

	// NVMe-oF connection timeouts (seconds), passed to `nvme connect`.
	ReconnectDelay int
	CtrlLossTmo    int
	FastIOFailTmo  int

	IsControllerServer bool
	IsNodeServer       bool
	IdleVolumeMin      int
	Timeout            int
}
