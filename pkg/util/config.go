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

	// Single deadline (seconds) covering every NVMe teardown/setup step:
	// the `nvme connect` / `nvme disconnect` shell-out itself, plus the
	// post-disconnect waits for the kernel subsystem entry and the
	// /dev/disk/by-id symlink to disappear. If any step exceeds this,
	// the operation fails and volume_clean is skipped.
	NvmeTimeoutSec int

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
	// fstrim timeout (seconds) forwarded to storage.volume_clean. The backend
	// applies it to the fstrim run on the SPDK node. force is not exposed —
	// the driver always lets the backend gate on running controllers.
	VolumeCleanFstrimTimeoutSec int

	IsControllerServer bool
	IsNodeServer       bool
	IdleVolumeMin      int
	Timeout            int
}
