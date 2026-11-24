package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestReadyEndpoint: a running node's /ready turns 200 once it has
// joined and serves SQL, while /status is 200 throughout (liveness), and
// /ready answers 503 as soon as the node drains for a stop.
func TestReadyEndpoint(t *testing.T) {
	tc := startWithHTTP(t, 1)
	base := "http://" + tc.Nodes[0].HTTPAddr()
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, ctype, body := httpGet(t, base+"/ready")
		if code == 200 {
			if !strings.Contains(ctype, "application/json") || !strings.Contains(body, `"ready":true`) {
				t.Fatalf("/ready: %s %s", ctype, body)
			}
			break
		}
		if code != 503 || time.Now().After(deadline) {
			t.Fatalf("/ready: %d %s", code, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if code, _, _ := httpGet(t, base+"/status"); code != 200 {
		t.Fatalf("/status: %d", code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tc.Nodes[0].Drain(ctx)
	code, _, body := httpGet(t, base+"/ready")
	if code != 503 || !strings.Contains(body, "draining") {
		t.Fatalf("/ready while draining: %d %s", code, body)
	}
}
