package main

import "testing"

func TestParseGenerationOptions(t *testing.T) {
	options, err := parseOptions([]string{"-contract", "order.created", "-version", "2", "-target", "java"})
	if err != nil {
		t.Fatal(err)
	}
	if options.contract != "order.created" || options.version != "2" || options.target != "java" {
		t.Fatalf("parsed options lost flag values: %+v", options)
	}
}

func TestParseGenerationDefaults(t *testing.T) {
	options, err := parseOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if options.target != "go" || options.migrations != "migrations" {
		t.Fatalf("unexpected defaults: %+v", options)
	}
}

func TestParseGenerationRejectsUnknownFlags(t *testing.T) {
	if _, err := parseOptions([]string{"-unknown"}); err == nil {
		t.Fatal("unknown flags must fail")
	}
}
