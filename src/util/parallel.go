package util

import (
	"sync"
	"sync/atomic"
	"time"
)

func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration, cnt *int32) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	tCh := time.After(timeout)
	for {
		select {
		case <-done:
			return false
		case <-tCh:
			return true
		default:
			if cnt != nil && atomic.LoadInt32(cnt) <= 0 {
				return false
			}
		}
	}
}
