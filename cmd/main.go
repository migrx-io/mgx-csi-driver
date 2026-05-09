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
	flag.IntVar(&conf.Timeout, "timeout-volume-check", 600, "Volume reconcile timeout")
	flag.IntVar(&conf.IdleVolumeMin, "idle-volume-min", 10, "Idle volume mins before to stop")
	flag.IntVar(&conf.NrIoQueues, "nr-io-queues", 2, "Number of I/O queues for nvme")
	flag.IntVar(&conf.QueueSize, "queue-size", 8, "Queue size for nvme")
	flag.IntVar(&conf.ReconnectDelay, "reconnect-delay", 2, "Delay (seconds) between NVMe-oF reconnect attempts")
	flag.IntVar(&conf.CtrlLossTmo, "ctrl-loss-tmo", 10, "Time (seconds) to keep retrying NVMe-oF reconnect before removing the controller")
	flag.IntVar(&conf.FastIOFailTmo, "fast-io-fail-tmo", 0, "Time (seconds) to queue I/O on a lost NVMe-oF controller before failing fast with EIO; 0 fails immediately")

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
