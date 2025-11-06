package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/klog"
)

// file name in which volume context is stashed.
const volumeContextFileName = "volume-context.json"

func ParseJSONFile(fileName string, result interface{}) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, result)
}

// round up bytes to megabytes
func ToMiB(bytes int64) int64 {
	const mi = 1024 * 1024
	return (bytes + mi - 1) / mi
}

// ${env:-def}
func FromEnv(env, def string) string {
	s := os.Getenv(env)
	if s != "" {
		return s
	}
	return def
}

// a trivial trylock implementation
type TryLock struct {
	locked int32
}

// acquire lock w/o waiting, return true if acquired, false otherwise
func (lock *TryLock) Lock() bool {
	// golang CAS forces sequential consistent memory order
	return atomic.CompareAndSwapInt32(&lock.locked, 0, 1)
}

// release lock
func (lock *TryLock) Unlock() {
	// golang atomic store forces release memory order
	atomic.StoreInt32(&lock.locked, 0)
}

var (
	nvmeReDeviceSysFileName = regexp.MustCompile(`nvme(\d+)n(\d+)|nvme(\d+)c(\d+)n(\d+)`)
	nvmeReDeviceName        = regexp.MustCompile(`c(\d+)`)
)

// getNvmeDeviceName checks the contents of given uuidFilePath for matching with
// nvmeModel. If it matches then returns the appropriate device name like nvme0n1
func getNvmeDeviceName(uuidFilePath, nvmeModel string) (string, error) {
	uuidContent, err := os.ReadFile(uuidFilePath)
	if err != nil {
		// a uuid file could be removed because of Disconnect() operation at the same time when doing ReadFile
		klog.Errorf("open uuid file uuidFilePath (%s) error: %s", uuidFilePath, err)
		return "", err
	}

	if strings.TrimSpace(string(uuidContent)) == nvmeModel {
		// Obtain the part nvme*c*n* or nvme*n* from the file path, eg, nvme0c0n1
		deviceSysFileName := nvmeReDeviceSysFileName.FindString(uuidFilePath)
		// Remove c* from (nvme*c*n*), eg, c0
		return nvmeReDeviceName.ReplaceAllString(deviceSysFileName, ""), nil
	}

	return "", errors.New("does not match")
}

func CheckIfNvmeDeviceExists(nvmeModel string, ignorePaths map[string]struct{}) (string, error) {
	uuidFilePaths, err := filepath.Glob("/sys/bus/pci/devices/*/nvme/nvme*/nvme*n*/uuid")
	if err != nil {
		return "", fmt.Errorf("obtain uuid files error: %w", err)
	}

	// The content of uuid file should be in the form of, eg, "b9e38b18-511e-429d-9660-f665fa7d63d0\n", which is also the volumeId.
	for _, filePath := range uuidFilePaths {
		if ignorePaths != nil {
			if _, visited := ignorePaths[filePath]; visited {
				continue
			}
			ignorePaths[filePath] = struct{}{}
		}
		deviceName, err := getNvmeDeviceName(filePath, nvmeModel)
		if err != nil {
			klog.Infof("Ignoring err: %v", err)
		}
		if deviceName != "" {
			return deviceName, nil
		}
	}
	return "", os.ErrNotExist
}

// detectNvemeDeviceName detects the device name in sysfs for given nvmeModel
func detectNvmeDeviceName(nvmeModel string) (string, error) {
	uuidFilePathsReadFlag := make(map[string]struct{})

	// Set 20 seconds timeout at maximum to try to find the exact device name for SMA Nvme
	for second := 0; second < 20; second++ {
		deviceName, err := CheckIfNvmeDeviceExists(nvmeModel, uuidFilePathsReadFlag)
		if err != nil {
			klog.Infof("detect nvme device '%s': %v", nvmeModel, err)
		} else {
			return deviceName, nil
		}
		// Wait a second before retry
		time.Sleep(time.Second)
	}

	return "", os.ErrDeadlineExceeded
}

// get the Nvme block device
func GetNvmeDeviceName(nvmeModel, bdf string) (string, error) {
	var deviceName string
	var err error
	if bdf != "" {
		var uuidFilePath string
		// find the uuid file path for the nvme device based on the bdf
		uuidFilePath, err = waitForDeviceReady(fmt.Sprintf("/sys/bus/pci/devices/%s/nvme/nvme*/nvme*n*/uuid", bdf), 20)
		if err != nil {
			return "", fmt.Errorf("failed find device at %s: %w", uuidFilePath, err)
		}
		klog.Infof("uuidFilePath is %s", uuidFilePath)
		deviceName, err = getNvmeDeviceName(uuidFilePath, nvmeModel)
	} else {
		deviceName, err = detectNvmeDeviceName(nvmeModel)
	}
	if err != nil {
		return "", fmt.Errorf("failed to find nvme device name: %w", err)
	}

	deviceGlob := "/dev/" + deviceName

	return waitForDeviceReady(deviceGlob, 20)
}


// ConvertInterfaceToMap converts an interface to a map[string]string
func ConvertInterfaceToMap(data interface{}) (map[string]string, error) {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, errors.New("the data is not a map[string]interface{}")
	}

	strMap := make(map[string]string)
	for key, value := range dataMap {
		if strValue, ok := value.(string); ok {
			strMap[key] = strValue
		} else {
			return nil, fmt.Errorf("the value for key %s is not a string", key)
		}
	}

	return strMap, nil
}

func stashContext(data interface{}, folder, fileName string) error {
	encodedBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshall context JSON: %w", err)
	}
	if _, err = os.Stat(folder); os.IsNotExist(err) {
		err = os.MkdirAll(folder, 0o755)
		if err != nil {
			return err
		}
	}
	fPath := filepath.Join(folder, fileName)
	err = os.WriteFile(fPath, encodedBytes, 0o600)
	if err != nil {
		return fmt.Errorf("failed to marshall context JSON at path (%s): %w", fPath, err)
	}
	return nil
}

func lookupContext(folder, fileName string) (interface{}, error) {
	var data interface{}
	fPath := filepath.Join(folder, fileName)
	encodedBytes, err := os.ReadFile(fPath) // #nosec - intended reading from fPath
	if err != nil {
		if !os.IsNotExist(err) {
			return data,
				fmt.Errorf("failed to read stashed context JSON from path (%s): %w", fPath, err)
		}
		return data, errors.New("volume context JSON file not found")
	}
	err = json.Unmarshal(encodedBytes, &data)
	if err != nil {
		return data,
			fmt.Errorf("failed to unmarshall stashed context JSON from path (%s): %w", fPath, err)
	}
	return data, nil
}

func cleanUpContext(folder, fileName string) error {
	fPath := filepath.Join(folder, fileName)
	if err := os.Remove(fPath); err != nil {
		return fmt.Errorf("failed to cleanup volume context stash (%s): %w", fPath, err)
	}
	return nil
}

// StashVolumeContext stashes volume context into the volumeContextFileName at the passed in path, in
// JSON format.
func StashVolumeContext(volumeContext map[string]string, path string) error {
	return stashContext(volumeContext, path, volumeContextFileName)
}

// LookupVolumeContext read and returns stashed volume context at passed in path
func LookupVolumeContext(path string) (map[string]string, error) {
	data, err := lookupContext(path, volumeContextFileName)
	if err != nil {
		return nil, err
	}
	return ConvertInterfaceToMap(data)
}

// CleanUpVolumeContext cleans up any stashed volume context at passed in path.
func CleanUpVolumeContext(path string) error {
	return cleanUpContext(path, volumeContextFileName)
}
