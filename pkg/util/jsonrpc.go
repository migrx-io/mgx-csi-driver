package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net"
	"time"
	"io"
	"k8s.io/klog"
)

// CreateLVolData is the data structure for creating a logical volume
type CreateLVolData struct {
	Name                 string `json:"name"`
	Size                 int64  `json:"size"`
	Config               string `json:"config"`
	Labels               string `json:"labels"`
	CacheRCacheSize      int    `json:"cache_r_cache_size"`
	CacheRWCacheSize     int    `json:"cache_rw_cache_size"`
	QosRMbytesPerSec     int    `json:"qos_r_mbytes_per_sec"`
	QosWMbytesPerSec     int    `json:"qos_w_mbytes_per_sec"`
	QosRWIosPerSec       int    `json:"qos_rw_ios_per_sec"`
	StorageEncryptSecret string `json:"storage_encrypt_secret"`
	StorageCompress      int    `json:"storage_compress"`
}

type LvolResp struct {
	Name   string `json:"nqn"`
	Size   int    `json:"size"`
	Nqn    string `json:"name"`
	Port   int    `json:"port"`
	IP     string `json:"ip"`
	Status string `json:"status"`
	Error  string `json:"error"`
}

type SnapshotResp struct {
	Name         string `json:"name"`
	UUID         string `json:"uuid"`
	Size         int64  `json:"size"`
	Pool         string `json:"pool"`
	CreatedAt    string `json:"created_at"`
	SourceVolume struct {
		UUID string `json:"id"`
	} `json:"lvol"`
}

type RPCClient struct {
	Protocol   string
	Nodes      []string
	Cluster    string
	Namespace  string
	Username   string
	Password   string
	Token      string
	HTTPClient *http.Client
}

// clusterConfig represents the Kubernetes secret structure
type ClusterConfig struct {
	Protocol  string   `json:"protocol"`
	Nodes     []string `json:"nodes"`
	Cluster   string   `json:"cluster"`
	Namespace string   `json:"ns"`
	Username  string   `json:"username"`
	Password  string   `json:"password"`
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

	c := &http.Client{}
	resp, err := c.Do(req)
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

func (client *RPCClient) Call(plugin, op string, data interface{}) (interface{}, error) {

	klog.Infof("Calling API: op: %s: plugin: %s: data: %v\n", op, plugin, data)

	// Build request body
	req := map[string]interface{}{
		"context": map[string]interface{}{
			"op": op,
		},
		"data": data,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	// find avaliable node and call
	for _, n := range client.Nodes {

		if isReachable(n) {

			klog.Infof("Calling API: node: %s\n", n)

			// If there is no Token then request it
			if client.Token == "" {
				err = client.Authenticate(n, client.Username, client.Password)
				if err != nil {
					return nil, err
				}
			}

			requestURL := fmt.Sprintf("%s://%s/api/v1/cluster/%s/plugins/%s", client.Protocol, client.Cluster, plugin)
			req, err := http.NewRequest("POST", requestURL, bytes.NewReader(reqData))
			if err != nil {
				return nil, fmt.Errorf("op: %s, plugin: %s, err:  %w", op, plugin, err)
			}

			req.Header.Add("Authorization", client.Token)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.HTTPClient.Do(req)
			if err != nil {
				return nil, fmt.Errorf("op: %s, plugin: %s, err:  %w", op, plugin, err)
			}

			defer resp.Body.Close()


			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("%v", resp.Body)
			}

			if resp.StatusCode == http.StatusOK {
				// Ensure data is a map

				body, _ := io.ReadAll(resp.Body)
				var m map[string]interface{}

				if err := json.Unmarshal(body, &m); err != nil {
					return nil, fmt.Errorf("unmarshal: %w", err)
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

	_, err := client.Call("storage", "volume_create", params)
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

	params := map[string]interface{}{
		"name": lvolID,
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

	params := map[string]interface{}{
		"name": lvolID,
	}

	klog.V(5).Info("deleteVolume", &params)

	_, err := client.Call("storage", "volume_del", &params)
	if err != nil {
		return err
	}

	return err
}

func (client *RPCClient) resizeVolume(lvolID string, size int64) (bool, error) {

	klog.V(5).Info("resizeVolume", lvolID)

	return false, nil

}

func (client *RPCClient) listSnapshots() ([]*SnapshotResp, error) {

	var results []*SnapshotResp

	klog.V(5).Info("listSnapshots")

	return results, nil
}

func (client *RPCClient) createSnapshot(lvolID, snapShotName string) (string, error) {
	var snapshotID string

	klog.V(5).Info("createSnapshot", lvolID, snapShotName)

	return snapshotID, nil
}

func (client *RPCClient) deleteSnapshot(snapshotID string) error {

	klog.V(5).Info("deleteSnapshot", snapshotID)

	return nil
}

func (client *RPCClient) unpublishVolume(lvolID string) error {

	klog.V(5).Info("unpublishVolume", lvolID)

	return nil
}

func isReachable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
