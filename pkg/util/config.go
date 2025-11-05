/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
