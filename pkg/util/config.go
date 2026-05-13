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

	// volume_clean polling: how often to GetVolume while waiting for READY,
	// and the total wall-clock budget after which we give up and fail unpublish.
	VolumeCleanPollIntervalSec int
	VolumeCleanReadyTimeoutSec int

	IsControllerServer bool
	IsNodeServer       bool
	IdleVolumeMin      int
	Timeout            int
}
