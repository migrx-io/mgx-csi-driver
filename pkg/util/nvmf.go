package util

import (
	"fmt"
	"net/http"
	"time"

	"k8s.io/klog"
)

type NodeNVMf struct {
	Client *RPCClient
}

// NewNVMf creates a new NVMf client
func NewNVMf(config *ClusterConfig) *NodeNVMf {
	client := RPCClient{
		HTTPClient: &http.Client{
			Timeout:   cfgRPCTimeoutSeconds * time.Second,
			Transport: &http.Transport{DisableKeepAlives: true},
		},
		Protocol:  config.Protocol,
		Nodes:     config.Nodes,
		Cluster:   config.Cluster,
		Namespace: config.Namespace,
		Username:  config.Username,
		Password:  config.Password,
	}
	return &NodeNVMf{
		Client: &client,
	}
}

func (node *NodeNVMf) CreateVolume(params *CreateLVolData) error {
	err := node.Client.createVolume(params)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume created: %s", params.Name)
	return nil
}

func (node *NodeNVMf) PublishVolume(lvolID string) error {
	err := node.Client.publishVolume(lvolID)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume published: %s", lvolID)
	return nil
}

func (node *NodeNVMf) GetVolume(lvolID string) (*LvolResp, error) {
	lvol, err := node.Client.getVolume(lvolID)
	if err != nil {
		return nil, err
	}
	return lvol, nil
}

func (node *NodeNVMf) VolumeInfo(lvolID string) (map[string]string, error) {
	lvol, err := node.Client.getVolume(lvolID)
	if err != nil {
		return nil, err
	}

	infoMap := map[string]string{
		"name": lvol.Name,
		"nqn":  lvol.Nqn,
		"size": fmt.Sprintf("%d", lvol.Size),
	}

	return infoMap, nil
}

func (node *NodeNVMf) GetVolumeSize(lvolID string) (int, error) {
	lvol, err := node.Client.getVolume(lvolID)
	if err != nil {
		return 0, err
	}
	return lvol.Size, err
}

func (node *NodeNVMf) DeleteVolume(lvolID string) error {
	err := node.Client.deleteVolume(lvolID)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume deleted: %s", lvolID)
	return nil
}

func (node *NodeNVMf) StopVolume(lvolID string) error {
	err := node.Client.stopVolume(lvolID)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume stop: %s", lvolID)
	return nil
}

func (node *NodeNVMf) StartVolume(lvolID string) error {
	err := node.Client.startVolume(lvolID)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume start: %s", lvolID)
	return nil
}

func (node *NodeNVMf) ResizeVolume(lvolID string, updatedSize int64) error {
	err := node.Client.resizeVolume(lvolID, updatedSize)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume resize: %s", lvolID)
	return nil
}

func (node *NodeNVMf) ListSnapshots() ([]*SnapshotResp, error) {
	return node.Client.listSnapshots()
}

// CreateSnapshot creates a snapshot of a volume
func (node *NodeNVMf) CreateSnapshot(lvolID, snapshotName string) (string, error) {
	snapshotID, err := node.Client.createSnapshot(lvolID, snapshotName)
	if err != nil {
		return "", err
	}
	klog.V(5).Infof("snapshot created: %s", snapshotID)
	return snapshotID, nil
}

func (node *NodeNVMf) DeleteSnapshot(snapshotID string) error {
	err := node.Client.deleteSnapshot(snapshotID)
	if err != nil {
		return err
	}
	klog.V(5).Infof("snapshot deleted: %s", snapshotID)
	return nil
}

func (node *NodeNVMf) UnpublishVolume(lvolID string) error {
	err := node.Client.unpublishVolume(lvolID)
	if err != nil {
		return err
	}

	klog.V(5).Infof("volume unpublished: %s", lvolID)
	return nil
}
