package util

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"k8s.io/klog"
)

var (
	ErrNotFound = errors.New("not found")
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

func (client *RPCClient) Authenticate(host string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	klog.V(5).Infof("Calling Authenticate: host: %s", host)

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
	req, err := http.NewRequestWithContext(ctx, "POST", authURL, bytes.NewBuffer(body))
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

	klog.V(5).Infof("Calling Authenticate: resp.StatusCode: %d", resp.StatusCode)

	// Step 4: Parse JSON response
	var respJSON map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&respJSON); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Step 5: Extract access_token
	accessToken, ok := respJSON["access_token"].(string)
	if !ok || accessToken == "" {
		return fmt.Errorf("failed to extract access_token")
	}

	klog.V(5).Infof("Calling Authenticate: accessToken received..")

	client.Token = fmt.Sprintf("JWT %s", accessToken)

	return nil
}

func (client *RPCClient) Call(plugin, op string, data any) (any, error) {
	klog.V(5).Infof("Calling API: op: %s, plugin: %s, data: %v\n", op, plugin, data)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build request body
	req := map[string]any{
		"context": map[string]any{
			"op": op,
		},
		"data": data,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	// find available node and call
	for _, n := range client.Nodes {
		if !isReachable(n) {
			continue
		}
		klog.V(5).Infof("Calling API: node: %s, data: %s", n, string(reqData))

		// If there is no Token then request it
		if client.Token == "" {
			err = client.Authenticate(n)
			if err != nil {
				return nil, err
			}
		}

		requestURL := fmt.Sprintf("%s://%s/api/v1/cluster/%s/plugins/%s", client.Protocol, n, client.Cluster, plugin)
		req, err := http.NewRequestWithContext(ctx, "PUT", requestURL, bytes.NewReader(reqData))
		if err != nil {
			return nil, fmt.Errorf("op: %s, plugin: %s, err:  %w", op, plugin, err)
		}

		req.Header.Add("Authorization", client.Token)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.HTTPClient.Do(req)
		if err != nil {
			klog.Errorf("failed to call plugin, err: %v", err)
			return nil, fmt.Errorf("op: %s, plugin: %s, err:  %w", op, plugin, err)
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%v", body)
		}

		if resp.StatusCode == http.StatusOK {
			// Ensure data is a map
			var m map[string]any

			if err := json.Unmarshal(body, &m); err != nil {
				klog.Errorf("failed to unmarshal plugin, err: %v", err)
				return nil, fmt.Errorf("unmarshal: %w", err)
			}

			// status == 200 -> check "error" field in JSON body
			if errVal, exists := m["error"]; exists && errVal != nil {
				// check if it's not found error
				if errorMatches(errVal.(string), ErrNotFound) {
					return nil, ErrNotFound
				}

				return nil, fmt.Errorf("%v", errVal)
			}

			if val, exists := m["data"]; exists {
				return val, nil
			}
		}

		return nil, fmt.Errorf("unexpected response format")
	}

	return nil, fmt.Errorf("no nodes available")
}

func (client *RPCClient) createVolume(params *CreateLVolData) error {
	klog.V(5).Info("createVolume", params)

	_, err := client.Call("storage", "volume_create", params)
	if err != nil {
		return err
	}

	return nil
}

func (*RPCClient) publishVolume(lvolID string) error {
	klog.V(5).Info("publishVolume", lvolID)

	return nil
}

func (client *RPCClient) getVolume(lvolID string) (*LvolResp, error) {
	params := map[string]any{
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

	klog.V(5).Info("getVolume result", result)

	return result, nil
}

func (client *RPCClient) deleteVolume(lvolID string) error {
	params := map[string]any{
		"name": lvolID,
	}

	klog.V(5).Info("deleteVolume", &params)

	_, err := client.Call("storage", "volume_del", &params)
	if err != nil {
		return err
	}

	return err
}

func (*RPCClient) resizeVolume(lvolID string, size int64) (bool, error) {
	klog.V(5).Info("resizeVolume", lvolID, size)

	return false, nil
}

func (*RPCClient) listSnapshots() ([]*SnapshotResp, error) {
	var results []*SnapshotResp

	klog.V(5).Info("listSnapshots")

	return results, nil
}

func (*RPCClient) createSnapshot(lvolID, snapShotName string) (string, error) { //nolint:unparam // placeholder for future error handling
	var snapshotID string

	klog.V(5).Info("createSnapshot", lvolID, snapShotName)

	return snapshotID, nil
}

func (*RPCClient) deleteSnapshot(snapshotID string) error {
	klog.V(5).Info("deleteSnapshot", snapshotID)

	return nil
}

func (*RPCClient) unpublishVolume(lvolID string) error {
	klog.V(5).Info("unpublishVolume", lvolID)

	return nil
}

func isReachable(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)

	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func errorMatches(errString string, err error) bool {
	if errString == "" {
		return false
	}
	strErr := strings.ToLower(err.Error())
	return strings.Contains(errString, strErr)
}
