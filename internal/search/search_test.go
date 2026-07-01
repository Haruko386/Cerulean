package search

import "testing"

func TestRRFusion(t *testing.T) {
	fusion := NewRRFusion(60)
	got := fusion.Fuse(2,
		[]Result{{ChunkID: "a", Score: 10}, {ChunkID: "b", Score: 8}},
		[]Result{{ChunkID: "b", Score: 0.9}, {ChunkID: "c", Score: 0.7}},
	)
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d", len(got))
	}
	if got[0].ChunkID != "b" {
		t.Fatalf("expected b to win because it appears in both result sets, got %s", got[0].ChunkID)
	}
}
