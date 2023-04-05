package pstest

import (
	"sync/atomic"
	"testing"
	"time"
)

func mockServerClock(t *testing.T, svr *Server) (advance func(duration time.Duration), restore func()) {
	oldFunc := svr.GServer.timeNowFunc

	clockPtr := atomic.Pointer[time.Time]{}
	var clock time.Time
	advance = func(d time.Duration) {
		clock = clock.Add(d) // start 1s after
		cp := clock
		clockPtr.Store(&cp)
	}
	advance(time.Second) // start at 1s past epoch, so it doesn't look like the 0 time.

	svr.SetTimeNowFunc(func() time.Time {
		t.Helper()
		ts := *clockPtr.Load()
		t.Log(ts)
		return ts
	})
	restore = func() {
		svr.SetTimeNowFunc(oldFunc)
	}
	return advance, restore
}
