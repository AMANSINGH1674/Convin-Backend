package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// TestRecordingGetsMarkedProcessed verifies that the background goroutine
// actually marks the call's recording as processed. Before the fix: the
// goroutine inherited the HTTP request context which was cancelled immediately
// after the handler returned 200, so MarkRecordingProcessed silently failed.
func TestRecordingGetsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// The recording processing takes ~50ms. Wait up to 2s for it to finish.
	deadline := time.Now().Add(2 * time.Second)
	for {
		var processed bool
		row := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			return // success
		}
		if time.Now().After(deadline) {
			t.Fatal("recording_processed still false after 2s — context bug?")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDuplicateDeliveryDoesNotDoubleCountStats verifies that redelivering the
// same event_id does not inflate account_stats — the symptom that ops reported.
// Before the fix: the check-then-insert race allowed duplicates to increment
// account_stats multiple times.
func TestDuplicateDeliveryDoesNotDoubleCountStats(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if stats.CallCount != 1 {
		t.Fatalf("call_count = %d after 3 deliveries of same event, want 1", stats.CallCount)
	}
	if stats.TotalDurationSec != 143 {
		t.Fatalf("total_duration_sec = %d, want 143", stats.TotalDurationSec)
	}
}
