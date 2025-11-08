package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"k8s.io/klog"
)

// CreateLVolData is the data structure for creating a logical volume
type CreateLVolData struct {
	Name         string `json:"name"`
	Size         int    `json:"size"`
	Config       string `json:"config"`
	Labels       string `json:"labels"`
	CacheRCacheSize    int `json:"cache_r_cache_size"`
	CacheRWCacheSize   int `json:"cache_rw_cache_size"`
	QosRMbytesPerSec   int `json:"qos_r_mbytes_per_sec"`
	QosWMbytesPerSec    int `json:"qos_w_mbytes_per_sec"`
	QosRWIosPerSec      int `json:"qos_rw_ios_per_sec"`
	StorageEncryptSecret 	string `json:"storage_encrypt_secret"`
	StorageCompress 		int `json:"storage_compress"`
}


type LvolResp struct {
	Name           string `json:"nqn"`
	Size           int    `json:"size"`
	Nqn            string `json:"name"`
	Port           int    `json:"port"`
	IP             string `json:"ip"`
	Status         string `json:"status"`
	Error          string `json:"error"`
}


type RPCClient struct {
	Protocol      string
	Nodes         []string
	Cluster       string
	Namespace     string
	Username      string
	Password      string
	Token         string
	HTTPClient    *http.Client
}

// clusterConfig represents the Kubernetes secret structure
type ClusterConfig struct {
	Protocol      string   `json:"protocol"`
	Nodes         []string `json:"nodes"`
	Cluster       string   `json:"cluster"`
	Namespace     string   `json:"ns"`
	Username      string   `json:"username"`
	Password      string   `json:"password"`
}


func (client *RPCClient) Authenticate(host, username, password string) error {

	authURL := fmt.Sprintf("%s://%s/api/v1/auth", client.Protocol, host)

	// Step 1: Build JSON payload
	authData := map[string]string{
		"cluster":  client.Cluster,
		"ns":       client.Namespace,
		"username": client.Username,
		"password": client.Password,
	}

	body, err := json.Marshal(authData)
	if err != nil {
		return fmt.Errorf("failed to marshal auth data: %w", err)
	}

	// Step 2: Send POST request
	req, err := http.NewRequest("POST", authURL, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Step 3: Check HTTP status code
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("auth failed: status=%d body=%s", resp.StatusCode, bodyBytes)
	}

	// Step 4: Parse JSON response
	var respJSON map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&respJSON); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Step 5: Extract access_token
	accessToken, ok := respJSON["access_token"].(string)
	if !ok || accessToken == "" {
		return fmt.Errorf("failed to extract access_token")
	}

	client.Token = fmt.Sprintf("JWT %s", accessToken)

	return nil
}


func (client *RPCClient) Call(plugin, op string, data map[string]interface{}) (interface{}, error) {

	klog.Infof("Calling API: op: %s: plugin: %s: data: %s\n", op, plugin, string(data))

  	// Build request body
    req := map[string]interface{}{
        "context": map[string]interface{}{
            "op": op,
        },
        "data": data,
    }

	reqData, err = json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}

	// find avaliable node and call
	for _, n := range client.Nodes {

		if isReachable(n) {

			klog.Infof("Calling API: node: %s\n", n)

			// If there is no Token then request it
			if client.Token == nil {
				err: = client.Authenticate()
				if err != nil {
					return nil, err
				}
			}

			requestURL := fmt.Sprintf("%s://%s/api/v1/cluster/%s/plugins/%s", client.Protocol, client.Cluster, plugin)
			req, err := http.NewRequest(method, requestURL, bytes.NewReader(data))
			if err != nil {
				return nil, fmt.Errorf("op: %s, plugin: %s, err:  %w", op, plugin,  err)
			}

			req.Header.Add("Authorization", client.Token)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.HTTPClient.Do(req)
			if err != nil {
				return nil, fmt.Errorf("op: %s, plugin: %s, err:  %w", op, plugin,  err)
			}

			defer resp.Body.Close()


			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("%v", resp.Body)
			}

			if resp.StatusCode == http.StatusOK {
				// Ensure data is a map

				m, ok := data.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("unexpected response format")
				}

				// status == 200 -> check "error" field in JSON body
				if errVal, exists := m["error"]; exists && errVal != nil {
					return nil, fmt.Errorf("%v", errVal)
				}

				if val, exists := m["data"]; exists {
					return val, nil
				}
			}

			return nil, fmt.Errorf("unexpected response format")

		} else {
			klog.Infof("Calling API: node: %s\n", n)

		}
	}

	return nil, fmt.Errorf("No nodes avaliable") 
}


func (client *RPCClient) createVolume(params *CreateLVolData) (string, error) {

	klog.V(5).Info("createVolume", params)

	_, err := client.Call("storage", "volume_create", &params)
	if err != nil {
		return "", err
	}

	return params.Name, nil
}

func (client *RPCClient) publishVolume(lvolID string) error {

	klog.V(5).Info("publishVolume", lvolID)

	return nil
}


func (client *RPCClient) getVolume(lvolID string) (*LvolResp, error) {

	params := map[string]Interface{}{
		Name: lvolID
	}

	klog.V(5).Info("getVolume", &params)

	out, err := client.Call("storage", "volume_show", &params)
	if err != nil {
		return nil, err
	}

	var result *LvolResp
	respBytes, _ := json.Marshal(out)

	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("invalid or empty response")
	}

	return result, nil

}

func (client *RPCClient) deleteVolume(lvolID string) error {
	_, err := client.CallSBCLI("DELETE", "/lvol/"+lvolID, nil)
	if errorMatches(err, ErrJSONNoSuchDevice) {
		err = ErrJSONNoSuchDevice // may happen in concurrency
	}

	return err
}

// resizeVolume resizes a volume
func (client *RPCClient) resizeVolume(lvolID string, size int64) (bool, error) {
	params := ResizeVolReq{
		LvolID:  lvolID,
		NewSize: size,
	}
	var result bool
	out, err := client.CallSBCLI("PUT", "/lvol/resize/"+lvolID, &params)
	if err != nil {
		return false, err
	}
	result, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("failed to convert the response to bool type. Interface: %v", out)
	}
	return result, nil
}

// cloneSnapshot clones a snapshot
func (client *RPCClient) cloneSnapshot(snapshotID, cloneName, newSize, pvcName string) (string, error) {
	params := struct {
		SnapshotID string `json:"snapshot_id"`
		CloneName  string `json:"clone_name"`
		PVCName    string `json:"pvc_name,omitempty"`
	}{
		SnapshotID: snapshotID,
		CloneName:  cloneName,
		PVCName:    pvcName,
	}

	klog.V(5).Infof("cloned volume size: %s", newSize)

	var lvolID string
	out, err := client.CallSBCLI("POST", "/snapshot/clone", &params)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft // may happen in concurrency
		}
		return "", err
	}

	lvolID, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("failed to convert the response to string type. Interface: %v", out)
	}
	return lvolID, err
}

// listSnapshots returns all snapshots
func (client *RPCClient) listSnapshots() ([]*SnapshotResp, error) {
	var results []*SnapshotResp

	out, err := client.CallSBCLI("GET", "/snapshot", nil)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the response: %w", err)
	}
	err = json.Unmarshal(b, &results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

// snapshot creates a snapshot
func (client *RPCClient) snapshot(lvolID, snapShotName string) (string, error) {
	params := struct {
		LvolName     string `json:"lvol_id"`
		SnapShotName string `json:"snapshot_name"`
	}{
		LvolName:     lvolID,
		SnapShotName: snapShotName,
	}
	var snapshotID string
	out, err := client.CallSBCLI("POST", "/snapshot", &params)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft // may happen in concurrency
		}
		return "", err
	}

	snapshotID, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("failed to convert the response to string type. Interface: %v", out)
	}
	return snapshotID, err
}

// deleteSnapshot deletes a snapshot
func (client *RPCClient) deleteSnapshot(snapshotID string) error {
	_, err := client.CallSBCLI("DELETE", "/snapshot/"+snapshotID, nil)

	if errorMatches(err, ErrJSONNoSuchDevice) {
		err = ErrJSONNoSuchDevice // may happen in concurrency
	}

	return err
}


func isReachable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}



// errorMatches checks if the error message from the full error
func errorMatches(errFull, errJSON error) bool {
	if errFull == nil {
		return false
	}
	strFull := strings.ToLower(errFull.Error())
	strJSON := strings.ToLower(errJSON.Error())
	strJSON = strings.TrimPrefix(strJSON, "json:")
	strJSON = strings.TrimSpace(strJSON)
	return strings.Contains(strFull, strJSON)
}
