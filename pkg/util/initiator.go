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
	}, nil
}

func (nvmf *initiatorNVMf) Connect(nrIoQueues, queueSize int) (string, error) {
	klog.Info("connect to ", nvmf.nqn)

	connected, live, err := nvmeSubsysStatus(nvmf.nqn)
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
		}

		err = execWithTimeoutRetry(cmdLine, 30, 1)
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
	err := execWithTimeout(cmdLine, 40)
	if err != nil {
		// go on checking device status in case caused by duplicate request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	}

	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", nvmf.name)
	return waitForDeviceGone(deviceGlob)
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

// wait for device file gone or timeout
func waitForDeviceGone(deviceGlob string) error {
	for range 21 {
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

// nvmeSubsysStatus walks /sys/class/nvme-subsystem to find a subsystem with the
// given NQN, and reports both whether it is present (connected) and whether at
// least one of its controllers is in the `live` state.
//
// Knowing the controller state is what lets the caller distinguish a healthy
// connection (skip reconnect) from a degraded one (controller hung in
// `connecting`/`resetting`/`dead`) where we must force a disconnect+reconnect
// instead of inheriting the broken controller.
func nvmeSubsysStatus(nqn string) (connected, live bool, err error) {
	subsysDirs, err := filepath.Glob("/sys/class/nvme-subsystem/nvme-subsys*")
	if err != nil {
		return false, false, fmt.Errorf("glob nvme-subsystem: %w", err)
	}
	for _, dir := range subsysDirs {
		nqnBytes, rerr := os.ReadFile(filepath.Join(dir, "subsysnqn"))
		if rerr != nil {
			continue
		}
		if strings.TrimSpace(string(nqnBytes)) != nqn {
			continue
		}
		connected = true

		ctrlDirs, gerr := filepath.Glob(filepath.Join(dir, "nvme*"))
		if gerr != nil {
			return connected, false, fmt.Errorf("glob controllers under %s: %w", dir, gerr)
		}
		for _, ctrl := range ctrlDirs {
			stateBytes, rerr := os.ReadFile(filepath.Join(ctrl, "state"))
			if rerr != nil {
				continue
			}
			state := strings.TrimSpace(string(stateBytes))
			klog.V(5).Infof("nvme controller %s state=%s", ctrl, state)
			if state == "live" {
				return connected, true, nil
			}
		}
		return connected, false, nil
	}
	return false, false, nil
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
