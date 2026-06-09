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
	QosRWMbytesPerSec    int    `json:"qos_rw_mbytes_per_sec"`
	QosRMbytesPerSec     int    `json:"qos_r_mbytes_per_sec"`
	QosWMbytesPerSec     int    `json:"qos_w_mbytes_per_sec"`
	QosRWIosPerSec       int    `json:"qos_rw_ios_per_sec"`
	StorageEncryptSecret string `json:"storage_encrypt_secret"`
	StorageCompress      int    `json:"storage_compress"`
}

type LvolResp struct {
	Name   string `json:"name"`
	Size   int    `json:"size"`
	Nqn    string `json:"nqn"`
	Port   int    `json:"proxy_port"`
	IP     string `json:"ip"`
	Status string `json:"status"`
	SCNode string `json:"sc_node"`
	Error  string `json:"error"`
}

// SnapshotResp mirrors the mgx-plgn-snapshot Snapshot model. A record owns a
// volume's whole backup chain (keyed by Name); each k8s VolumeSnapshot is one
// restore point (Stamp) within it. Restore records (Kind == "restore") reuse
// the same model.
type SnapshotResp struct {
	Name       string `json:"name"`
	Config     string `json:"config"`
	Kind       string `json:"kind"`
	Volume     string `json:"volume"`
	Stamp      string `json:"stamp"`
	Increments any    `json:"increments"`
	Size       int64  `json:"size"`
	Created    string `json:"created"`
	SCNode     string `json:"sc_node"`
	Status     string `json:"status"`
	Error      string `json:"error"`
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

	resp, err := client.HTTPClient.Do(req)
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
			klog.Errorf("failed to call plugin, err: %v, resp: %v", err, resp)
			return nil, fmt.Errorf("op: %s, plugin: %s, err:  %w", op, plugin, err)
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("plugin call code: %d, error: %s", resp.StatusCode, string(body))
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
	klog.V(5).Infof("createVolume, params: %v", params)

	_, err := client.Call("storage", "volume_create", params)
	if err != nil {
		return err
	}

	return nil
}

func (*RPCClient) publishVolume(lvolID string) error {
	klog.V(5).Infof("publishVolume: %s", lvolID)

	return nil
}

// showByName issues a "<plugin>_show"-style RPC that takes a single "name"
// parameter and decodes the result into *T. Shared by volume_show and
// snapshot_show, which differ only in plugin/op and result type.
func showByName[T any](client *RPCClient, plugin, op, name string) (*T, error) {
	params := map[string]any{
		"name": name,
	}

	klog.V(5).Infof("%s, params: %v", op, &params)

	out, err := client.Call(plugin, op, &params)
	if err != nil {
		return nil, err
	}

	var result *T
	respBytes, _ := json.Marshal(out)

	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("invalid or empty response")
	}

	klog.V(5).Infof("%s result: %v", op, result)

	return result, nil
}

func (client *RPCClient) getVolume(lvolID string) (*LvolResp, error) {
	return showByName[LvolResp](client, "storage", "volume_show", lvolID)
}

func (client *RPCClient) deleteVolume(lvolID string) error {
	params := map[string]any{
		"name": lvolID,
	}

	klog.V(5).Infof("deleteVolume: %v", &params)

	_, err := client.Call("storage", "volume_del", &params)
	if err != nil {
		return err
	}

	return err
}

func (client *RPCClient) stopVolume(lvolID string) error {
	params := map[string]any{
		"name": lvolID,
	}

	klog.V(5).Infof("stopVolume: %v", &params)

	_, err := client.Call("storage", "volume_stop", &params)
	if err != nil {
		return err
	}

	return err
}

func (client *RPCClient) startVolume(lvolID string) error {
	params := map[string]any{
		"name": lvolID,
	}

	klog.V(5).Infof("stopVolume: %v", &params)

	_, err := client.Call("storage", "volume_start", &params)
	if err != nil {
		return err
	}

	return err
}

func (client *RPCClient) cleanVolume(lvolID string, fstrimTimeoutSec int) error {
	params := map[string]any{
		"name": lvolID,
	}
	if fstrimTimeoutSec > 0 {
		params["timeout"] = fstrimTimeoutSec
	}

	klog.V(5).Infof("cleanVolume: %v", &params)

	_, err := client.Call("storage", "volume_clean", &params)
	if err != nil {
		return err
	}

	return err
}

func (client *RPCClient) resizeVolume(lvolID string, updatedSize int64) error {
	params := map[string]any{
		"name": lvolID,
		"size": updatedSize,
	}

	klog.V(5).Infof("resizeVolume: %v", &params)

	_, err := client.Call("storage", "volume_resize", &params)
	if err != nil {
		return err
	}

	return err
}

func (client *RPCClient) snapshotShow(name string) (*SnapshotResp, error) {
	return showByName[SnapshotResp](client, "snapshot", "snapshot_show", name)
}

// snapshotAdd arms a backup/restore point. params carries the snapshot_add meta
// fields (name, config, volume, sc_node, stamp, and any overrides). Idempotent
// on the caller-owned stamp.
func (client *RPCClient) snapshotAdd(params map[string]any) error {
	klog.V(5).Infof("snapshotAdd: %v", &params)

	_, err := client.Call("snapshot", "snapshot_add", &params)
	return err
}

// restoreAdd schedules a restore of a backed-up snapshot into a new volume
// prefix. params carries the restore_add meta fields. Idempotent on name.
func (client *RPCClient) restoreAdd(params map[string]any) error {
	klog.V(5).Infof("restoreAdd: %v", &params)

	_, err := client.Call("snapshot", "restore_add", &params)
	return err
}

// snapshotDel removes one restore point (delStamp set) or the whole backup
// record (delStamp empty). Only the oldest stamp may be deleted; deleting the
// last point drops the whole backup.
func (client *RPCClient) snapshotDel(name, delStamp string, purge bool) error {
	params := map[string]any{
		"name": name,
	}
	if delStamp != "" {
		params["del_stamp"] = delStamp
	}
	if purge {
		params["purge"] = "yes"
	}

	klog.V(5).Infof("snapshotDel: %v", &params)

	_, err := client.Call("snapshot", "snapshot_del", &params)
	return err
}

func (client *RPCClient) snapshotList() ([]*SnapshotResp, error) {
	params := map[string]any{}

	klog.V(5).Info("snapshotList")

	out, err := client.Call("snapshot", "snapshot_list", &params)
	if err != nil {
		return nil, err
	}

	var results []*SnapshotResp
	respBytes, _ := json.Marshal(out)

	if err := json.Unmarshal(respBytes, &results); err != nil {
		return nil, fmt.Errorf("invalid or empty response")
	}

	return results, nil
}

func (*RPCClient) unpublishVolume(lvolID string) error {
	klog.V(5).Infof("unpublishVolume: %s", lvolID)

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
