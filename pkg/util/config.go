package util

import "encoding/json"

// Config stores parsed command line parameters
type Config struct {
	DriverName    string
	DriverVersion string
	Endpoint      string
	NodeID        string

	IsControllerServer bool
	IsNodeServer       bool
}

// MgxSecrets storage cluster connection nodes with apiKey secrets, see deploy/kubernetes/secrets.yaml
//
//nolint:tagliatelle // not using json:snake case
type MgxSecrets struct {
		Nodes    []string `json:"nodes"`
		APIKey   string   `json:"apiKey"`
}

func NewMgxSecrets(jsonSecrets string) (*MgxSecrets, error) {
	var secs MgxSecrets
	err := json.Unmarshal([]byte(jsonSecrets), &secs)
	if err != nil {
		return nil, err
	}
	return &secs, nil
}
