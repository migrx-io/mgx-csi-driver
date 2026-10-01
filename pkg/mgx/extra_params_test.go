package mgx

import (
	"strings"
	"testing"
)

// arg list produced when no parameter is set
const defaultExtraParams = "--nbd-flag=--threads=32 " +
	"--nbd-param=cache-purge-on-stop=true " +
	"--nbd-param=cache-flush-threads=10 " +
	"--nbd-param=cache-flush-interval=300 " +
	"--nbd-param=cache-flush-blocks=5 " +
	"--nbd-param=cache-flush-max-age=3000 " +
	"--nbd-param=cache-write-throttle-ms=50 " +
	"--nbd-param=cache-min-block-size=4096 " +
	"--nbd-param=cache-high-threshold=95 " +
	"--nbd-param=cache-low-threshold=85 " +
	"--nbd-param=cache-reclaim-scan-blocks=12800 " +
	"--nbd-param=cache-reclaim-scan-tries=20 " +
	"--nbd-param=cache-stats-interval=500 " +
	"--nbd-param=cache-lru-percent=50 " +
	"--nbd-param=cache-reclaim-high-count=2 " +
	"--nbd-param=cache-reclaim-max-count=64 " +
	"--nbd-param=cache-max-overflow-percent=5 " +
	"--nbd-param=cache-fill-threshold=100 " +
	"--nbd-param=cache-readahead-trigger=3 " +
	"--nbd-param=cache-readahead-blocks=32 " +
	"--nbd-param=cache-readahead-batch=4 " +
	"--nbd-param=cache-readahead-threads=8 " +
	"--nbd-param=cache-sync-interval=300 " +
	"--nbd-param=cache-persist-interval=1000 " +
	"--cacheFlushThreads=30 " +
	"--blockReadThreads=32 " +
	"--blockCacheSize=300 " +
	"--blockCacheThreads=30 " +
	"--listBlocksThreads=30 " +
	"--blockCacheWriteDelay=0 " +
	"--initialRetryPause=300 " +
	"--maxRetryPause=3000"

func TestBuildExtraParamsDefaults(t *testing.T) {
	got, err := buildExtraParams(map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != defaultExtraParams {
		t.Fatalf("defaults mismatch:\n got: %s\nwant: %s", got, defaultExtraParams)
	}
}

func TestBuildExtraParamsOverrides(t *testing.T) {
	got, err := buildExtraParams(map[string]string{
		"cache_flush_threads": " 16 ",
		"cache_purge_on_stop": "False",
		"block_cache_size":    "",
		"extra_params":        "--foo=1\n  --bar",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, want := range []string{
		"--nbd-param=cache-flush-threads=16",
		"--nbd-param=cache-purge-on-stop=false",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if strings.Contains(got, "--blockCacheSize=") {
		t.Errorf("empty block_cache_size should drop the flag: %q", got)
	}
	if !strings.HasSuffix(got, " --foo=1 --bar") {
		t.Errorf("raw extra_params should be appended last: %q", got)
	}
}

func TestBuildExtraParamsInvalid(t *testing.T) {
	cases := map[string]map[string]string{
		"not an int":        {"cache_flush_threads": "ten"},
		"negative":          {"cache_flush_interval": "-1"},
		"below min":         {"nbd_threads": "0"},
		"above percent":     {"cache_lru_percent": "101"},
		"not a bool":        {"cache_purge_on_stop": "maybe"},
		"low above high":    {"cache_high_threshold": "80", "cache_low_threshold": "90"},
		"low above default": {"cache_low_threshold": "96"},
		"low equals high":   {"cache_high_threshold": "90", "cache_low_threshold": "90"},
		"zero lru":          {"cache_lru_percent": "0"},
		"not power of 2":    {"cache_min_block_size": "6144"},
		"batch too large":   {"cache_readahead_batch": "65"},
		"retry pause order": {"initial_retry_pause": "5000"},
	}

	for name, params := range cases {
		if _, err := buildExtraParams(params); err == nil {
			t.Errorf("%s: expected an error for %v", name, params)
		}
	}
}
