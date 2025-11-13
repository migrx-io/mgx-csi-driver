// blackbox test of util package
package util_test

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/migrx-io/mgx-csi-driver/pkg/util"
)

func TestTryLockSequential(t *testing.T) {
	var tryLock util.TryLock

	// acquire lock
	if !tryLock.Lock() {
		t.Fatalf("failed to acquire lock")
	}
	// acquire a locked lock should fail
	if tryLock.Lock() {
		t.Fatalf("acquired a locked lock")
	}
	// acquire a released lock should succeed
	tryLock.Unlock()
	if !tryLock.Lock() {
		t.Fatal("failed to acquire a release lock")
	}
}

func TestTryLockConcurrent(t *testing.T) {
	var tryLock util.TryLock
	var wg sync.WaitGroup
	var lockCount int32
	const taskCount = 50

	// only one task should acquire the lock
	for range taskCount {
		wg.Add(1)
		go func() {
			if tryLock.Lock() {
				atomic.AddInt32(&lockCount, 1)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if lockCount != 1 {
		t.Fatal("concurrency test failed")
	}
}

func TestVolumeContext(t *testing.T) {
	volumeContextFileName := "volumeContext.json"

	dir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	volumeContext := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	err = util.StashVolumeContext(volumeContext, dir)
	if err != nil {
		t.Fatalf("StashVolumeContext returned error: %v", err)
	}

	returnedContext, err := util.LookupVolumeContext(dir)
	if err != nil {
		t.Fatalf("LookupVolumeContext returned error: %v", err)
	}

	if volumeContext["key1"] != returnedContext["key1"] || volumeContext["key2"] != returnedContext["key2"] {
		t.Fatalf("LookupVolumeContext returned unexpected value: got %v, want %v", returnedContext, volumeContext)
	}

	err = util.CleanUpVolumeContext(dir)
	if err != nil {
		t.Fatalf("CleanUpVolumeContext returned error: %v", err)
	}

	_, err = os.Stat(dir + "/" + volumeContextFileName)
	if !os.IsNotExist(err) {
		t.Fatalf("CleanUpVolumeContext failed to cleanup volume context stash")
	}
}

func TestPvcToVolName(t *testing.T) {
	tests := []struct {
		pvc      string
		expected string
	}{
		{"pvc-1234", "vol-"},
		{"pvc-9a8b7c6d-1234-5678-90ab-cdef12345678", "vol-"},
	}

	seen := map[string]bool{}
	for _, tt := range tests {
		got := util.PvcToVolName(tt.pvc)

		// must start with vol-
		if len(got) < 5 || got[:4] != "vol-" {
			t.Errorf("expected prefix 'vol-', got %q", got)
		}

		// must have exact length of 20 (NVMe limit)
		if len(got) != 20 {
			t.Errorf("expected length 20, got %d for %q", len(got), got)
		}

		// must be deterministic
		got2 := util.PvcToVolName(tt.pvc)
		if got != got2 {
			t.Errorf("non-deterministic: %q vs %q", got, got2)
		}

		// must be unique across different PVCs
		if seen[got] {
			t.Errorf("duplicate hash found for %q: %q", tt.pvc, got)
		}
		seen[got] = true
	}
}
