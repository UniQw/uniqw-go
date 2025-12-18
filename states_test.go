package uniqw

import (
	"testing"
)

func TestState_StringAndParse(t *testing.T) {
	// String()
	if StatePending.String() != "pending" || StateActive.String() != "active" || StateDelayed.String() != "delayed" || StateSucceeded.String() != "succeeded" || StateDead.String() != "dead" {
		t.Fatal("unexpected state string values")
	}
	// Parse valid
	for _, s := range []string{"pending", "active", "delayed", "succeeded", "dead"} {
		if _, err := ParseState(s); err != nil {
			t.Fatalf("parse valid state %q failed: %v", s, err)
		}
	}
	// Parse invalid
	if _, err := ParseState("weird"); err == nil {
		t.Fatal("expected error for invalid state")
	} else if err != ErrUnknownState {
		t.Fatalf("expected ErrUnknownState, got %v", err)
	}
}
