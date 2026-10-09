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

	// Block-layer queue settings applied to each device after `nvme connect`.
	// Defaults match what udev gives a local NVMe/EBS device, so an mgx volume
	// and a gp3 volume present the same queue to the filesystem above them.
	//
	// IOScheduler: "" leaves udev's choice alone. `none` is what makes the
	// target's per-operation QoS bind on sequential I/O - see tuneBlockQueue.
	//
	// MaxSectorsKB: 0 leaves the kernel default. Values above the device's
	// max_hw_sectors_kb are clamped, since the transport's advertised MDTS is
	// a hard ceiling (nvme-tcp reports 128 KiB, where EBS reports 256).
	//
	// NrRequests: 0 leaves the kernel default. Under IOScheduler `none` the
	// default is already the controller's tag depth and cannot be exceeded,
	// so this only ever lowers it there - NrIoQueues/QueueSize are the knobs
	// that raise it. Under a real scheduler it sizes the scheduler's own
	// request pool and may go deeper. See tuneBlockQueue.
	IOScheduler  string
	MaxSectorsKB int
	NrRequests   int

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
	// Re-arms of a FAILED snapshot or restore copy before giving up; 0 = unlimited.
	MaxCopyRetries int
}
