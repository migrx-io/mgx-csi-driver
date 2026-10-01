package mgx

import (
	"fmt"
	"strconv"
	"strings"
)

// extraParam maps a StorageClass parameter to an nbdkit plugin flag.
type extraParam struct {
	key    string // StorageClass parameter name
	flag   string // flag prefix, the value is appended as is
	def    string // used when the parameter is not set; "" = leave flag out
	isBool bool   // value is a bool, otherwise a non-negative int
	min    int    // lowest allowed int value
	max    int    // highest allowed int value, 0 = unbounded
	pow2   bool   // int value must be a power of 2
}

// extraParamDefs lists the StorageClass tunables in command line order.
// A parameter set to "" drops the flag.
var extraParamDefs = []extraParam{
	{key: "nbd_threads", flag: "--nbd-flag=--threads=", def: "32", min: 1},
	{key: "cache_purge_on_stop", flag: "--nbd-param=cache-purge-on-stop=", def: "true", isBool: true},
	{key: "cache_flush_threads", flag: "--nbd-param=cache-flush-threads=", def: "10", min: 1},
	{key: "cache_flush_interval", flag: "--nbd-param=cache-flush-interval=", def: "300"},
	{key: "cache_flush_blocks", flag: "--nbd-param=cache-flush-blocks=", def: "5", min: 1},
	{key: "cache_flush_max_age", flag: "--nbd-param=cache-flush-max-age=", def: "3000"},
	{key: "cache_write_throttle_ms", flag: "--nbd-param=cache-write-throttle-ms=", def: "50", min: 1, max: 60000},
	{key: "cache_min_block_size", flag: "--nbd-param=cache-min-block-size=", def: "4096", min: 4096, pow2: true},
	{key: "cache_high_threshold", flag: "--nbd-param=cache-high-threshold=", def: "95", min: 1, max: 100},
	{key: "cache_low_threshold", flag: "--nbd-param=cache-low-threshold=", def: "85", min: 1, max: 100},
	{key: "cache_reclaim_scan_blocks", flag: "--nbd-param=cache-reclaim-scan-blocks=", def: "12800", min: 1},
	{key: "cache_reclaim_scan_tries", flag: "--nbd-param=cache-reclaim-scan-tries=", def: "20"},
	{key: "cache_stats_interval", flag: "--nbd-param=cache-stats-interval=", def: "500"},
	{key: "cache_lru_percent", flag: "--nbd-param=cache-lru-percent=", def: "50", min: 1, max: 100},
	{key: "cache_reclaim_high_count", flag: "--nbd-param=cache-reclaim-high-count=", def: "2", min: 1},
	{key: "cache_reclaim_max_count", flag: "--nbd-param=cache-reclaim-max-count=", def: "64", min: 1},
	{key: "cache_max_overflow_percent", flag: "--nbd-param=cache-max-overflow-percent=", def: "5", max: 100},
	{key: "cache_fill_threshold", flag: "--nbd-param=cache-fill-threshold=", def: "100", max: 100},
	{key: "cache_readahead_trigger", flag: "--nbd-param=cache-readahead-trigger=", def: "3"},
	{key: "cache_readahead_blocks", flag: "--nbd-param=cache-readahead-blocks=", def: "32", min: 1, max: 4096},
	{key: "cache_readahead_batch", flag: "--nbd-param=cache-readahead-batch=", def: "4", min: 1, max: 64},
	{key: "cache_readahead_threads", flag: "--nbd-param=cache-readahead-threads=", def: "8", min: 1, max: 256},
	{key: "cache_sync_interval", flag: "--nbd-param=cache-sync-interval=", def: "300", max: 600000},
	{key: "cache_persist_interval", flag: "--nbd-param=cache-persist-interval=", def: "1000", max: 600000},
	{key: "block_cache_flush_threads", flag: "--cacheFlushThreads=", def: "30", min: 1},
	{key: "block_read_threads", flag: "--blockReadThreads=", def: "32"},
	{key: "block_cache_size", flag: "--blockCacheSize=", def: "300"},
	{key: "block_cache_threads", flag: "--blockCacheThreads=", def: "30", min: 1},
	{key: "list_blocks_threads", flag: "--listBlocksThreads=", def: "30", min: 1},
	{key: "block_cache_write_delay", flag: "--blockCacheWriteDelay=", def: "0"},
	{key: "initial_retry_pause", flag: "--initialRetryPause=", def: "300"},
	{key: "max_retry_pause", flag: "--maxRetryPause=", def: "3000"},
}

// buildExtraParams validates the tunables and joins them into one space
// separated arg list, followed by the raw extra_params parameter.
func buildExtraParams(params map[string]string) (string, error) {
	args := make([]string, 0, len(extraParamDefs))
	ints := map[string]int{}

	for _, p := range extraParamDefs {
		value, exists := params[p.key]
		if !exists {
			value = p.def
		}
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		if p.isBool {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return "", fmt.Errorf("invalid %s %q: expected true or false", p.key, value)
			}
			value = strconv.FormatBool(b)
		} else {
			n, err := strconv.Atoi(value)
			if err != nil {
				return "", fmt.Errorf("invalid %s %q: expected an integer", p.key, value)
			}
			if n < p.min {
				return "", fmt.Errorf("invalid %s %d: must be >= %d", p.key, n, p.min)
			}
			if p.max > 0 && n > p.max {
				return "", fmt.Errorf("invalid %s %d: must be <= %d", p.key, n, p.max)
			}
			if p.pow2 && n&(n-1) != 0 {
				return "", fmt.Errorf("invalid %s %d: must be a power of 2", p.key, n)
			}
			ints[p.key] = n
			value = strconv.Itoa(n)
		}

		args = append(args, p.flag+value)
	}

	if err := checkOrder(ints, "cache_low_threshold", "cache_high_threshold", false); err != nil {
		return "", err
	}
	if err := checkOrder(ints, "initial_retry_pause", "max_retry_pause", true); err != nil {
		return "", err
	}

	// collapse newlines/indent so a multi-line value is stored as a single
	// space separated arg list
	if raw := strings.Fields(params["extra_params"]); len(raw) > 0 {
		args = append(args, raw...)
	}

	return strings.Join(args, " "), nil
}

// checkOrder requires lowKey < highKey (or <= when equal is allowed) when both
// are set.
func checkOrder(ints map[string]int, lowKey, highKey string, equal bool) error {
	low, hasLow := ints[lowKey]
	high, hasHigh := ints[highKey]
	if !hasLow || !hasHigh {
		return nil
	}
	if low < high || (equal && low == high) {
		return nil
	}
	op := "<"
	if equal {
		op = "<="
	}
	return fmt.Errorf("invalid %s %d: must be %s %s %d", lowKey, low, op, highKey, high)
}
