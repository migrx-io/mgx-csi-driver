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
	KeepAliveTmo   int

	// How long Disconnect() waits for the kernel to drop the NVMe subsystem
	// entry and for udev to remove the /dev/disk/by-id symlink before
	// returning an error and skipping volume_clean.
	NvmeDisconnectTimeoutSec int

	// Per-invocation deadline for `nvme connect` / `nvme disconnect` shell
	// commands. The process is killed if it has not returned within this
	// window.
	NvmeCmdTimeoutSec int

	// Per-command timeout (seconds) applied to every shell-out that
	// SafeFormatAndMount makes (fsck, mkfs, mount). Guards against a stuck
	// NVMe-oF device wedging NodePublishVolume forever.
	MkfsFsckTimeoutSec int

	// volume_clean polling: how often to GetVolume while waiting for READY,
	// and the total wall-clock budget after which we give up and fail unpublish.
	VolumeCleanPollIntervalSec int
	VolumeCleanReadyTimeoutSec int
	// When false, NodeUnpublishVolume only unmounts and skips the
	// storage.volume_clean RPC + READY wait.
	VolumeCleanEnabled bool

	IsControllerServer bool
	IsNodeServer       bool
	IdleVolumeMin      int
	Timeout            int
}
