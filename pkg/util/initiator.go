package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"
)

const (
	DevDiskByID = "/dev/disk/by-id/*%s*"
)

// MGXCsiInitiator defines interface for NVMeoF initiator
//   - Connect initiates target connection and returns local block device filename
//   - Disconnect terminates target connection
//   - Caller(node service) should serialize calls to same initiator
//   - Implementation should be idempotent to duplicated requests
type MGXCsiInitiator interface {
	Connect(nrIoQueues, queueSize int) (string, error)
	Disconnect() error
}

// initiatorNVMf is an implementation of NVMf tcp initiator
type initiatorNVMf struct {
	name           string
	nqn            string
	reconnectDelay int
	ctrlLossTmo    int
	fastIOFailTmo  int
	keepAliveTmo   int
	timeout        int
	ioScheduler    string
	maxSectorsKB   int
}

func NewMGXClient() (*NodeNVMf, error) {
	secretFile := FromEnv("MGX_SECRET", "/etc/csi-secret/secret.json")

	var clusterConfig *ClusterConfig

	err := ParseJSONFile(secretFile, &clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to parse secret file: %w", err)
	}

	if clusterConfig == nil {
		return nil, fmt.Errorf("failed to find secret")
	}

	if len(clusterConfig.Nodes) == 0 || clusterConfig.Username == "" {
		return nil, fmt.Errorf("invalid cluster configuration")
	}

	// Log and return the newly created Simplyblock client.
	klog.Infof("MGX client created for Nodes:%s",
		clusterConfig.Nodes,
	)

	return NewNVMf(clusterConfig), nil
}

func NewMGXCsiInitiator(volumeContext map[string]string, conf *Config) (MGXCsiInitiator, error) {
	klog.Infof("mgx nqn :%s", volumeContext["nqn"])

	return &initiatorNVMf{
		name:           volumeContext["name"],
		nqn:            volumeContext["nqn"],
		reconnectDelay: conf.ReconnectDelay,
		ctrlLossTmo:    conf.CtrlLossTmo,
		fastIOFailTmo:  conf.FastIOFailTmo,
		keepAliveTmo:   conf.KeepAliveTmo,
		timeout:        conf.NvmeTimeoutSec,
		ioScheduler:    conf.IOScheduler,
		maxSectorsKB:   conf.MaxSectorsKB,
	}, nil
}

func (nvmf *initiatorNVMf) Connect(nrIoQueues, queueSize int) (string, error) {
	klog.Info("connect to ", nvmf.nqn)

	connected, live, err := NvmeSubsysStatus(nvmf.nqn)
	if err != nil {
		klog.Errorf("Failed to check existing connections: %v", err)
		return "", err
	}

	// Subsystem present but no controller is in `live` state — the connection
	// is degraded (resetting/connecting/dead). Tear it down so we can rebuild
	// from a clean state instead of inheriting the bad controller.
	if connected && !live {
		klog.Warningf("NQN %s present but no live controller — forcing disconnect before reconnect", nvmf.nqn)
		if derr := nvmf.Disconnect(); derr != nil {
			klog.Warningf("forced disconnect of %s failed: %v (continuing)", nvmf.nqn, derr)
		}
		connected = false
	}

	var (
		mgxClient  *NodeNVMf
		connection *LvolResp
	)

	if !connected {
		mgxClient, err = NewMGXClient()
		if err != nil {
			klog.Errorf("failed to create mgx client: %v", err)
			return "", err
		}

		connection, err = fetchLvolConnection(mgxClient, nvmf.name)
		if err != nil {
			klog.Errorf("Failed to get lvol connection: %v", err)
			return "", err
		}

		cmdLine := []string{
			"nvme", "connect", "-t", "tcp",
			"-a", connection.IP,
			"-s", strconv.Itoa(connection.Port),
			"-n", nvmf.nqn,
			"--nr-io-queues=" + strconv.Itoa(nrIoQueues),
			"--queue-size=" + strconv.Itoa(queueSize),
			"--ctrl-loss-tmo=" + strconv.Itoa(nvmf.ctrlLossTmo),
			"--reconnect-delay=" + strconv.Itoa(nvmf.reconnectDelay),
			"--fast_io_fail_tmo=" + strconv.Itoa(nvmf.fastIOFailTmo),
			"--keep-alive-tmo=" + strconv.Itoa(nvmf.keepAliveTmo),
		}

		err = execWithTimeoutRetry(cmdLine, nvmf.timeout, 1)
		if err != nil {
			// go on checking device status in case caused by duplicated request
			klog.Errorf("command %v failed: %s", cmdLine, err)
			return "", err
		}
	}

	deviceGlob := fmt.Sprintf(DevDiskByID, nvmf.name)
	devicePath, err := waitForDeviceReady(deviceGlob, 20)
	if err != nil {
		return "", err
	}

	// Applied on every Connect, not just when we ran `nvme connect`: the block
	// device is recreated on reattach and comes back with udev's default.
	if serr := tuneBlockQueue(devicePath, nvmf.ioScheduler, nvmf.maxSectorsKB); serr != nil {
		// Not fatal - the volume is fully usable, only QoS accounting and
		// request sizing are left at the kernel's defaults.
		klog.Warningf("could not tune block queue for %s: %v", devicePath, serr)
	}

	return devicePath, nil
}

// tuneBlockQueue applies block-layer queue settings to a freshly attached
// device so that an mgx volume presents the same queue as a local NVMe/EBS one.
//
// scheduler: udev assigns `mq-deadline` to nvme-tcp devices on some distros
// (Amazon Linux 2023 among them), while local NVMe and EBS get `none`.
// mq-deadline merges adjacent requests up to max_sectors_kb, so a sequential 4K
// writer reaches the target as ~128 KiB I/Os. The target's bdev QoS counts
// operations, so qos_rw_ios_per_sec sees ~1/32 of the client's IOPS and never
// engages - measured 27,000 IOPS against a 3,000 IOPS cap, where random I/O
// (unmergeable) capped correctly at ~2,750. `none` restores parity with EBS.
//
// maxSectorsKB: the largest single request the block layer will issue. The
// kernel rejects any value above the device's max_hw_sectors_kb, so the request
// is clamped instead of failing. That ceiling is the MDTS the target
// advertises, and nvme-tcp reports 128 KiB where EBS reports 256 - so asking
// for EBS parity here is satisfied only as far as the transport allows.
//
// devicePath is a /dev/disk/by-id symlink, so it is resolved to the kernel name
// first. Writing a value that is already set is a kernel no-op, which makes
// this idempotent across reconnects. Zero/empty settings are left alone, and
// both attributes are attempted even if one fails.
func tuneBlockQueue(devicePath, scheduler string, maxSectorsKB int) error {
	if scheduler == "" && maxSectorsKB <= 0 {
		return nil
	}

	resolved, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", devicePath, err)
	}

	// Base() of a resolved device node cannot contain a separator, so nothing
	// built from it can escape /sys/block.
	dev := filepath.Base(resolved) // /dev/nvme1n1 -> nvme1n1

	var errs []error

	if scheduler != "" {
		if werr := writeQueueAttr(dev, "scheduler", scheduler); werr != nil {
			errs = append(errs, werr)
		} else {
			klog.Infof("%s: scheduler set to %s", dev, scheduler)
		}
	}

	if maxSectorsKB > 0 {
		want := maxSectorsKB

		hw, herr := readQueueAttrInt(dev, "max_hw_sectors_kb")
		switch {
		case herr != nil:
			errs = append(errs, herr)

			want = 0
		case want > hw:
			klog.Infof("%s: max_sectors_kb %d above hardware limit %d, clamping",
				dev, want, hw)

			want = hw
		}

		if want > 0 {
			if werr := writeQueueAttr(dev, "max_sectors_kb", strconv.Itoa(want)); werr != nil {
				errs = append(errs, werr)
			} else {
				klog.Infof("%s: max_sectors_kb set to %d", dev, want)
			}
		}
	}

	return errors.Join(errs...)
}

// queueAttrPath builds /sys/block/<dev>/queue/<attr>. Sprintf rather than
// filepath.Join because the sysfs layout is fixed, not composed from path
// elements; dev comes from filepath.Base so it cannot contain a separator.
func queueAttrPath(dev, attr string) string {
	return fmt.Sprintf("/sys/block/%s/queue/%s", dev, attr)
}

func writeQueueAttr(dev, attr, value string) error {
	path := queueAttrPath(dev, attr)

	// Mode is inert: sysfs attributes already exist and ignore it.
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	return nil
}

func readQueueAttrInt(dev, attr string) (int, error) {
	path := queueAttrPath(dev, attr)

	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}

	n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}

	return n, nil
}

func (nvmf *initiatorNVMf) Disconnect() error {
	// nvme disconnect -n "nqn"
	cmdLine := []string{"nvme", "disconnect", "-n", nvmf.nqn}
	err := execWithTimeout(cmdLine, nvmf.timeout)
	if err != nil {
		// go on checking device status in case caused by duplicate request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	}

	// Authoritative signal first: wait for the kernel to drop the subsystem
	// entry. While that entry exists the host still has the namespace open,
	// and calling volume_clean against the backend would race the teardown.
	// The by-id symlink wait that follows is the udev-side confirmation.
	if err := waitForSubsysGone(nvmf.nqn, nvmf.timeout); err != nil {
		return err
	}

	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", nvmf.name)
	return waitForDeviceGone(deviceGlob, nvmf.timeout)
}

// when timeout is set as 0, try to find the device file immediately
// otherwise, wait for device file comes up or timeout
func waitForDeviceReady(deviceGlob string, seconds int) (string, error) {
	for i := 0; i <= seconds; i++ {
		matches, err := filepath.Glob(deviceGlob)
		if err != nil {
			return "", err
		}
		// two symbol links under /dev/disk/by-id/ to same device
		if len(matches) >= 1 {
			return matches[0], nil
		}
		time.Sleep(time.Second)
	}
	return "", fmt.Errorf("timed out waiting device ready: %s", deviceGlob)
}

// waitForSubsysGone polls `nvme list-subsys` until the NQN's subsystem
// entry is no longer present, or fails after seconds. Transient probe
// failures are logged and retried — only a persistently-present subsystem
// returns an error, which gates volume_clean in NodeUnpublishVolume.
func waitForSubsysGone(nqn string, seconds int) error {
	for i := 0; i <= seconds; i++ {
		connected, _, err := NvmeSubsysStatus(nqn)
		if err != nil {
			klog.Warningf("nvme list-subsys probe failed (will retry): %v", err)
		} else if !connected {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting for NVMe subsystem to disconnect: %s", nqn)
}

// wait for device file gone or timeout
func waitForDeviceGone(deviceGlob string, seconds int) error {
	for i := 0; i <= seconds; i++ {
		matches, err := filepath.Glob(deviceGlob)
		if err != nil {
			return err
		}
		if len(matches) == 0 {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting device gone: %s", deviceGlob)
}

func execWithTimeout(cmdLine []string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	klog.Infof("running command: %v", cmdLine)
	//nolint:gosec // execWithTimeout assumes valid cmd arguments
	cmd := exec.CommandContext(ctx, cmdLine[0], cmdLine[1:]...)
	output, err := cmd.CombinedOutput()

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errors.New("timed out")
	}
	if output != nil {
		klog.Infof("command returned: %s", output)
	}
	return err
}

// NvmeSubsysStatus invokes `nvme list-subsys -o json` to find a subsystem with
// the given NQN, and reports both whether it is present (connected) and whether
// at least one of its paths is in the `live` state.
//
// Knowing the path state is what lets the caller distinguish a healthy
// connection (skip reconnect) from a degraded one (controller hung in
// `connecting`/`resetting`/`dead`) where we must force a disconnect+reconnect
// instead of inheriting the broken controller. Exported so the node server
// can probe controller health from its staging health check.
func NvmeSubsysStatus(nqn string) (connected, live bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "nvme", "list-subsys", "-o", "json").Output()
	if err != nil {
		return false, false, fmt.Errorf("nvme list-subsys: %w", err)
	}

	subs, err := parseListSubsys(out)
	if err != nil {
		return false, false, fmt.Errorf("parse nvme list-subsys output: %w", err)
	}

	for _, s := range subs {
		if s.NQN != nqn {
			continue
		}
		connected = true
		for _, p := range s.Paths {
			klog.V(5).Infof("nvme path %s state=%s", p.Name, p.State)
			if p.State == "live" {
				return connected, true, nil
			}
		}
		return connected, false, nil
	}
	return false, false, nil
}

type nvmeSubsystem struct {
	Name  string     `json:"Name"`
	NQN   string     `json:"NQN"`
	Paths []nvmePath `json:"Paths"`
}

type nvmePath struct {
	Name      string `json:"Name"`
	Transport string `json:"Transport"`
	Address   string `json:"Address"`
	State     string `json:"State"`
}

// parseListSubsys handles the two known shapes of `nvme list-subsys -o json`:
//
//  1. Newer nvme-cli (host-grouped):
//     [ { "HostNQN": "...", "Subsystems": [ {...} ] } ]
//  2. Older nvme-cli (flat):
//     { "Subsystems": [ {...} ] }
func parseListSubsys(raw []byte) ([]nvmeSubsystem, error) {
	// Try host-grouped form first.
	var hosts []struct {
		Subsystems []nvmeSubsystem `json:"Subsystems"`
	}
	if err := json.Unmarshal(raw, &hosts); err == nil {
		var out []nvmeSubsystem
		for _, h := range hosts {
			out = append(out, h.Subsystems...)
		}
		return out, nil
	}

	// Fall back to flat form.
	var flat struct {
		Subsystems []nvmeSubsystem `json:"Subsystems"`
	}
	if err := json.Unmarshal(raw, &flat); err != nil {
		return nil, err
	}
	return flat.Subsystems, nil
}

func execWithTimeoutRetry(cmdLine []string, timeout, retry int) (err error) {
	for retry > 0 {
		err = execWithTimeout(cmdLine, timeout)
		if err == nil {
			return nil
		}
		retry--
	}
	return err
}

func fetchLvolConnection(mgxClient *NodeNVMf, lvolID string) (*LvolResp, error) {
	volumeInfo, err := mgxClient.GetVolume(lvolID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch connection: %w", err)
	}

	var connection *LvolResp
	respBytes, _ := json.Marshal(volumeInfo)

	if err := json.Unmarshal(respBytes, &connection); err != nil {
		return nil, fmt.Errorf("invalid or empty connection response")
	}
	return connection, nil
}
