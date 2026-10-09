package mgx

import "sync"

// copyRetries caps how many times a FAILED backup copy (a snapshot restore
// point or a restore) is re-armed. Each re-arm re-runs the whole copy, so a
// copy that keeps failing would otherwise loop forever. Counts live in memory:
// a controller restart starts them over. A nil *copyRetries allows unlimited
// re-arms.
type copyRetries struct {
	mu     sync.Mutex
	max    int               // re-arms allowed per key; 0 = unlimited
	n      map[string]int    // re-arms made per key
	gaveUp map[string]string // keys given up on -> the copy's last error
}

func newCopyRetries(maxRetries int) *copyRetries {
	return &copyRetries{max: maxRetries, n: map[string]int{}, gaveUp: map[string]string{}}
}

// retry reports whether key's FAILED copy may be re-armed again, counting the
// re-arm when it may. attempts is the re-arms made so far, including this one.
func (c *copyRetries) retry(key string) (attempts int, ok bool) {
	if c == nil {
		return 0, true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.n[key]
	if c.max > 0 && n >= c.max {
		return n, false
	}
	c.n[key] = n + 1
	return n + 1, true
}

// giveUp marks key as given up on, keeping the copy's last error to report.
func (c *copyRetries) giveUp(key, lastErr string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gaveUp[key] = lastErr
}

// givenUp reports whether key was given up on, with its re-arms and last error.
func (c *copyRetries) givenUp(key string) (attempts int, lastErr string, ok bool) {
	if c == nil {
		return 0, "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lastErr, ok = c.gaveUp[key]
	return c.n[key], lastErr, ok
}

// reset forgets key's re-arms (the copy succeeded or its object was deleted).
func (c *copyRetries) reset(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.n, key)
	delete(c.gaveUp, key)
}
