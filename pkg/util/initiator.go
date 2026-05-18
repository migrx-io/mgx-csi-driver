package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
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
	name              string
	nqn               string
	reconnectDelay    int
	ctrlLossTmo       int
	fastIOFailTmo     int
	keepAliveTmo      int
	disconnectTimeout int
	cmdTimeout        int
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
		name:              volumeContext["name"],
		nqn:               volumeContext["nqn"],
		reconnectDelay:    conf.ReconnectDelay,
		ctrlLossTmo:       conf.CtrlLossTmo,
		fastIOFailTmo:     conf.FastIOFailTmo,
		keepAliveTmo:      conf.KeepAliveTmo,
		disconnectTimeout: conf.NvmeDisconnectTimeoutSec,
		cmdTimeout:        conf.NvmeCmdTimeoutSec,
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

		err = execWithTimeoutRetry(cmdLine, nvmf.cmdTimeout, 1)
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
	return devicePath, nil
}

func (nvmf *initiatorNVMf) Disconnect() error {
	// nvme disconnect -n "nqn"
	cmdLine := []string{"nvme", "disconnect", "-n", nvmf.nqn}
	err := execWithTimeout(cmdLine, nvmf.cmdTimeout)
	if err != nil {
		// go on checking device status in case caused by duplicate request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	}

	// Authoritative signal first: wait for the kernel to drop the subsystem
	// entry. While that entry exists the host still has the namespace open,
	// and calling volume_clean against the backend would race the teardown.
	// The by-id symlink wait that follows is the udev-side confirmation.
	if err := waitForSubsysGone(nvmf.nqn, nvmf.disconnectTimeout); err != nil {
		return err
	}

	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", nvmf.name)
	return waitForDeviceGone(deviceGlob, nvmf.disconnectTimeout)
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
