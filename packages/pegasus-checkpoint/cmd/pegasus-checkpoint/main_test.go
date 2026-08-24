package main

import "testing"

func TestParseArgsPatternsRepeatable(t *testing.T) {
	cfg, err := parseArgs([]string{"-p", "foo.*", "--pattern", "bar.*"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.patterns) != 2 || cfg.patterns[0] != "foo.*" || cfg.patterns[1] != "bar.*" {
		t.Fatalf("got %v", cfg.patterns)
	}
}

func TestParseArgsIntervalNotGiven(t *testing.T) {
	cfg, err := parseArgs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.intervalOK {
		t.Fatal("expected no periodic notifier when -i is not given")
	}
}

func TestParseArgsIntervalZeroIsFalsy(t *testing.T) {
	// Matches Python's `if args.interval:` treating an explicit 0 as falsy:
	// 0 is a valid (nonnegative) interval value, but no periodic notifier
	// gets started for it.
	cfg, err := parseArgs([]string{"-i", "0"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.intervalOK {
		t.Fatal("expected interval=0 to not start a periodic notifier")
	}
}

func TestParseArgsIntervalPositive(t *testing.T) {
	cfg, err := parseArgs([]string{"--interval", "30"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.intervalOK || cfg.interval != 30 {
		t.Fatalf("got interval=%d intervalOK=%v", cfg.interval, cfg.intervalOK)
	}
}

func TestParseArgsIntervalNegativeRejected(t *testing.T) {
	if _, err := parseArgs([]string{"-i", "-5"}); err == nil {
		t.Fatal("expected error for negative interval")
	}
}

func TestParseArgsDebugAndLogToFile(t *testing.T) {
	cfg, err := parseArgs([]string{"-d", "-l"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.debug || !cfg.logToFile {
		t.Fatalf("got debug=%v logToFile=%v", cfg.debug, cfg.logToFile)
	}
}
