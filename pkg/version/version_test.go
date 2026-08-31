package version

import "testing"

func TestWindow(t *testing.T) {
	if MinSupported > Current {
		t.Fatalf("window inverted: [%d, %d]", MinSupported, Current)
	}
	if !Supported(Current) || !Supported(MinSupported) {
		t.Fatalf("window endpoints must be supported")
	}
	if Supported(MinSupported - 1) {
		t.Fatalf("below-window version reported supported")
	}
	if Supported(Current + 1) {
		t.Fatalf("above-window version reported supported")
	}
	if got := V2.String(); got != "v2" {
		t.Fatalf("String: got %q", got)
	}
}
