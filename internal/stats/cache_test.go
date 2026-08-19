package stats_test

import (
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestCacheRecordConcurrent verifies that concurrent Record() calls don't
// race. Run with `go test -race` to detect the data race in Record().
func TestCacheRecordConcurrent(t *testing.T) {
	c := stats.NewCache()

	const goroutines = 50
	const perGoroutine = 100
	done := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func() {
			for i := 0; i < perGoroutine; i++ {
				c.Record("acc_shared", 1)
			}
			done <- struct{}{}
		}()
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}

	got := c.Get("acc_shared")
	want := int64(goroutines * perGoroutine)
	if got.CallCount != want {
		t.Fatalf("CallCount = %d, want %d", got.CallCount, want)
	}
	if got.TotalDurationSec != want {
		t.Fatalf("TotalDurationSec = %d, want %d", got.TotalDurationSec, want)
	}
}
