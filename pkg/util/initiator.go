package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"
)

const (
	DevDiskByID = "/dev/disk/by-id/*%s*"

	reconnectDelay = 2
	ctrlLossTmo    = 30
	fastIOFailTmo  = 30
)

// MGXCsiInitiator defines interface for NVMeoF initiator
//   - Connect initiates target connection and returns local block device filename
//   - Disconnect terminates target connection
//   - Caller(node service) should serialize calls to same initiator
//   - Implementation should be idempotent to duplicated requests
type MGXCsiInitiator interface {
	Connect() (string, error)
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

func NewMGXCsiInitiator(volumeContext map[string]string) (MGXCsiInitiator, error) {
	klog.Infof("mgx nqn :%s", volumeContext["nqn"])

	return &initiatorNVMf{
		name:           volumeContext["name"],
		nqn:            volumeContext["nqn"],
		reconnectDelay: reconnectDelay,
		ctrlLossTmo:    ctrlLossTmo,
		fastIOFailTmo:  fastIOFailTmo,
	}, nil
}

func (nvmf *initiatorNVMf) Connect() (string, error) {
	klog.Info("connect to ", nvmf.nqn)

	alreadyConnected, err := isNqnConnected(nvmf.nqn)
	if err != nil {
		klog.Errorf("Failed to check existing connections: %v", err)
		return "", err
	}

	var (
		mgxClient  *NodeNVMf
		connection *LvolResp
	)

	if !alreadyConnected {
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

func isNqnConnected(nqn string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "list-subsys")

	output, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("failed to execute nvme list-subsys: %w", err)
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, nqn) {
			parts := strings.Fields(line)
			if len(parts) > 0 {
				return true, nil
			}
		}
	}
	return false, nil
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
