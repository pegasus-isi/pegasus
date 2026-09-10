package model

import "testing"

func TestParseJSON_Transfer(t *testing.T) {
	data := []byte(`[
	 { "type": "transfer",
	   "id": 1,
	   "src_urls": [ { "site_label": "Workflow", "url": "scp://bamboo@workflow.isi.edu/local-scratch/a.txt" } ],
	   "dest_urls": [ { "site_label": "AmazonS3", "url": "s3://test@amazon/bucket/a.txt" } ]
	 }
	]`)
	entries, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	tr, ok := entries[0].(*Transfer)
	if !ok {
		t.Fatalf("expected *Transfer, got %T", entries[0])
	}
	if tr.SrcProto() != "scp" || tr.DstProto() != "s3" {
		t.Errorf("got src proto %q dst proto %q", tr.SrcProto(), tr.DstProto())
	}
	// "id" is an unknown key to the transfer schema and must be silently
	// ignored, matching json_object_decoder.
}

func TestParseJSON_TransferWithAttributesAndFlags(t *testing.T) {
	data := []byte(`[
	 { "type": "transfer",
	   "lfn": "a.txt",
	   "linkage": "input",
	   "generate_checksum": true,
	   "verify_checksum_remote": false,
	   "attributes": {"foo": "bar"},
	   "src_urls": [ { "site_label": "local", "url": "file:///tmp/a.txt", "priority": "5" } ],
	   "dest_urls": [ { "site_label": "local", "url": "file:///tmp/b.txt" } ]
	 }
	]`)
	entries, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	tr := entries[0].(*Transfer)
	if tr.LFN != "a.txt" || tr.Linkage != "input" {
		t.Errorf("lfn/linkage not parsed: %+v", tr)
	}
	if !tr.GenerateChecksum || tr.VerifyChecksumRemote {
		t.Errorf("checksum flags not parsed: %+v", tr)
	}
	if tr.SrcURLs[0].Priority != 5 {
		t.Errorf("string priority not parsed: %d", tr.SrcURLs[0].Priority)
	}
}

func TestParseJSON_Mkdir(t *testing.T) {
	data := []byte(`[{"type": "mkdir", "target": {"site_label": "local", "url": "file:///tmp/x"}}]`)
	entries, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	m := entries[0].(*Mkdir)
	if m.Path() != "/tmp/x" {
		t.Errorf("got path %q", m.Path())
	}
}

func TestParseJSON_RemoveRecursiveAsPythonStringTrue(t *testing.T) {
	// RemoveDirectory.java emits "recursive": "True" as a JSON string, not a
	// bool. This must be honored exactly like Python's truthiness check.
	data := []byte(`[{"type": "remove", "target": {"site_label": "local", "url": "file:///tmp/x", "recursive": "True"}}]`)
	entries, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	r := entries[0].(*Remove)
	if !r.Recursive {
		t.Errorf("expected recursive=true from string \"True\"")
	}
}

func TestParseJSON_RemoveRecursiveAsPythonStringFalse(t *testing.T) {
	data := []byte(`[{"type": "remove", "target": {"site_label": "local", "url": "file:///tmp/x", "recursive": "False"}}]`)
	entries, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	r := entries[0].(*Remove)
	if r.Recursive {
		t.Errorf("expected recursive=false from string \"False\"")
	}
}

func TestParseJSON_RemoveRecursiveDefaultFalse(t *testing.T) {
	data := []byte(`[{"type": "remove", "target": {"site_label": "local", "url": "file:///tmp/x"}}]`)
	entries, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	r := entries[0].(*Remove)
	if r.Recursive {
		t.Errorf("expected recursive=false by default")
	}
}

func TestParseJSON_RemoveRecursiveAsBool(t *testing.T) {
	data := []byte(`[{"type": "remove", "target": {"site_label": "local", "url": "file:///tmp/x", "recursive": true}}]`)
	entries, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	r := entries[0].(*Remove)
	if !r.Recursive {
		t.Errorf("expected recursive=true from JSON bool")
	}
}

func TestParseJSON_UnknownType(t *testing.T) {
	data := []byte(`[{"type": "frobnicate"}]`)
	if _, err := ParseJSON(data); err == nil {
		t.Errorf("expected error for unknown entry type")
	}
}
