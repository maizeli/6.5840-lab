package util

import (
	"sync"
	"time"
)

func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
		return false
	case <-time.After(timeout):
		return true
	}
}
