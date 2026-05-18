package util

import (
	"context"
	"time"

	utilexec "k8s.io/utils/exec"
)

// NewTimeoutExec wraps an exec.Interface so every command produced by
// Command(...) is killed if it has not finished within timeout. The
// non-context Command(...) path is what mount.SafeFormatAndMount uses
// internally for fsck/mkfs/mount, so without this wrapper a stuck NVMe-oF
// device makes those calls (and the whole NodePublishVolume RPC) hang
// indefinitely. CommandContext(...) is passed through unchanged — if the
// caller supplied a context, they own the deadline.
func NewTimeoutExec(inner utilexec.Interface, timeout time.Duration) utilexec.Interface {
	return &timeoutExec{inner: inner, timeout: timeout}
}

type timeoutExec struct {
	inner   utilexec.Interface
	timeout time.Duration
}

func (e *timeoutExec) Command(cmd string, args ...string) utilexec.Cmd {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	return &timeoutCmd{
		Cmd:    e.inner.CommandContext(ctx, cmd, args...),
		cancel: cancel,
	}
}

func (e *timeoutExec) CommandContext(ctx context.Context, cmd string, args ...string) utilexec.Cmd {
	return e.inner.CommandContext(ctx, cmd, args...)
}

func (e *timeoutExec) LookPath(file string) (string, error) {
	return e.inner.LookPath(file)
}

// timeoutCmd releases the context as soon as the command completes, so a
// short-running command does not leave its timer parked until the deadline.
type timeoutCmd struct {
	utilexec.Cmd
	cancel context.CancelFunc
}

func (c *timeoutCmd) Run() error {
	defer c.cancel()
	return c.Cmd.Run()
}

func (c *timeoutCmd) CombinedOutput() ([]byte, error) {
	defer c.cancel()
	return c.Cmd.CombinedOutput()
}

func (c *timeoutCmd) Output() ([]byte, error) {
	defer c.cancel()
	return c.Cmd.Output()
}

func (c *timeoutCmd) Wait() error {
	defer c.cancel()
	return c.Cmd.Wait()
}
