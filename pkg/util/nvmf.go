package util

import (
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
		HTTPClient: &http.Client{Timeout: cfgRPCTimeoutSeconds * time.Second},
		Protocol:   config.Protocol,
		Nodes:      config.Nodes,
		Cluster:    config.Cluster,
		Namespace:  config.Namespace,
		Username:   config.Username,
		Password:   config.Password,
	}
	return &NodeNVMf{
		Client: &client,
	}
}

func (node *NodeNVMf) CreateVolume(params *CreateLVolData) (string, error) {
	lvolID, err := node.Client.createVolume(params)
	if err != nil {
		return "", err
	}
	klog.V(5).Infof("volume created: %s", lvolID)
	return lvolID, nil
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

func (node *NodeNVMf) ResizeVolume(lvolID string, newSize int64) (bool, error) {
	return node.Client.resizeVolume(lvolID, newSize)
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
