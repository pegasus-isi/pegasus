package main

import (
	"reflect"
	"testing"
)

func TestParseArgsPermanent(t *testing.T) {
	a, err := parseArgs([]string{"-p"})
	if err != nil {
		t.Fatal(err)
	}
	if !a.permanent {
		t.Fatal("expected permanent=true")
	}
}

func TestParseArgsCollectsUntilNextFlag(t *testing.T) {
	a, err := parseArgs([]string{"-c", "coll-1", "coll-2", "-e", "ep-1", "-p"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.collections, []string{"coll-1", "coll-2"}) {
		t.Fatalf("collections = %v", a.collections)
	}
	if !reflect.DeepEqual(a.endpoints, []string{"ep-1"}) {
		t.Fatalf("endpoints = %v", a.endpoints)
	}
	if !a.permanent {
		t.Fatal("expected permanent=true")
	}
}

func TestParseArgsEmptyNargsList(t *testing.T) {
	// A flag with nothing following it (or immediately followed by another
	// flag) collects zero values, matching argparse's nargs="*" default of [].
	a, err := parseArgs([]string{"-e", "-d", "example.edu"})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.endpoints) != 0 {
		t.Fatalf("endpoints = %v, want empty", a.endpoints)
	}
	if !reflect.DeepEqual(a.domains, []string{"example.edu"}) {
		t.Fatalf("domains = %v", a.domains)
	}
}

func TestParseArgsUnrecognized(t *testing.T) {
	if _, err := parseArgs([]string{"--bogus"}); err == nil {
		t.Fatal("expected an error for an unrecognized flag")
	}
}

func TestParseArgsLongForms(t *testing.T) {
	a, err := parseArgs([]string{"--permanent", "--collections", "c1", "--endpoints", "e1", "--domains", "d1"})
	if err != nil {
		t.Fatal(err)
	}
	if !a.permanent || len(a.collections) != 1 || len(a.endpoints) != 1 || len(a.domains) != 1 {
		t.Fatalf("got %+v", a)
	}
}
