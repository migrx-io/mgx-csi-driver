package main

import (
	"flag"
	"os"

	"k8s.io/klog"

	"github.com/migrx-io/mgx-csi-driver/pkg/mgx"
	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

const (
	driverName    = "csi.migrx.io"
	driverVersion = "0.1.0"
)

var conf = util.Config{
	DriverVersion: driverVersion,
}

func setupFlags() {
	flag.StringVar(&conf.DriverName, "drivername", driverName, "Name of the driver")
	flag.StringVar(&conf.Endpoint, "endpoint", "unix://tmp/mgxcsi.sock", "CSI endpoint")
	flag.StringVar(&conf.NodeID, "nodeid", "", "node id")
	flag.BoolVar(&conf.IsControllerServer, "controller", false, "Start controller server")
	flag.BoolVar(&conf.IsNodeServer, "node", false, "Start node server")

	klog.InitFlags(nil)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Exitf("failed to set logtostderr flag: %v", err)
	}
	flag.Parse()
}

func main() {
	setupFlags()

	klog.Infof("Starting MGX-CSI driver: %v version: %v", conf.DriverName, driverVersion)

	mgx.Run(&conf)

	os.Exit(0)
}
