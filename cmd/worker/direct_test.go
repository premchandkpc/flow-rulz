package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDirectRulesCompilesExampleChain(t *testing.T) {
	revisions, err := loadDirectRules("../../examples/direct-worker/rules")
	if err != nil {
		t.Fatal(err)
	}
	if len(revisions) != 2 || revisions[0].RuleID != "a.created" || revisions[1].RuleID != "a.processed" {
		t.Fatalf("unexpected revisions: %+v", revisions)
	}
}

func TestLoadDirectRulesRejectsMissingMalformedAndOversizedInput(t *testing.T) {
	for _, source := range [][]byte{nil, []byte(`{"rule_set":`), make([]byte, 256*1024+1)} {
		dir := t.TempDir()
		if source != nil {
			if err := os.WriteFile(filepath.Join(dir, "rule.json"), source, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := loadDirectRules(dir); err == nil {
			t.Fatal("accepted invalid rule directory")
		}
	}
}
