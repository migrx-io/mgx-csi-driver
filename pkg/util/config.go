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
