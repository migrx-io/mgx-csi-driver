// whitebox test of some functions in initiator.go
package util

import (
	"testing"
	"time"
)

func TestExecWithTimeoutPositive(t *testing.T) {
	elapsed, err := runExecWithTimeout([]string{"true"}, 10)
	if err != nil {
		t.Fatal("should succeed")
	}
	if elapsed > 3 {
		t.Fatal("timeout error")
	}
}

func TestExecWithTimeoutNegative(t *testing.T) {
	elapsed, err := runExecWithTimeout([]string{"false"}, 10)
	if err == nil {
		t.Fatal("should fail")
	}
	if elapsed > 3 {
		t.Fatal("timeout error")
	}
}

func TestExecWithTimeoutTimeout(t *testing.T) {
	elapsed, err := runExecWithTimeout([]string{"sleep", "10"}, 1)
	if err == nil {
		t.Fatal("should fail")
	}
	if elapsed > 3 {
		t.Fatal("timeout error")
	}
}

func runExecWithTimeout(cmdLine []string, timeout int) (int, error) {
	start := time.Now()
	err := execWithTimeout(cmdLine, timeout)
	elapsed := int(time.Since(start) / time.Second)
	return elapsed, err
}
