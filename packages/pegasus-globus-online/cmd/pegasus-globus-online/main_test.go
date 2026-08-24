package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/pegasus-isi/pegasus/packages/pegasus-globus-online/internal/globusapi"
)

func TestPosixSplit(t *testing.T) {
	cases := []struct{ in, head, tail string }{
		{"/a/b/c", "/a/b", "c"},
		{"/a/b/", "/a/b", ""},
		{"/", "/", ""},
		{"noSlash", "", "noSlash"},
		{"//", "//", ""},
	}
	for _, c := range cases {
		head, tail := posixSplit(c.in)
		if head != c.head || tail != c.tail {
			t.Errorf("posixSplit(%q) = (%q, %q), want (%q, %q)", c.in, head, tail, c.head, c.tail)
		}
	}
}

// fakeClient is a minimal in-process globusClient for testing runMkdir/
// runTransfer/runRemove and findMkdirBase without an HTTP server — the real
// REST wire format is covered by internal/globusapi's own tests.
type fakeClient struct {
	existingDirs map[string]bool // endpoint+":"+path -> exists
	mkdirCalls   []string
	mkdirErr     map[string]error // path -> error to return from OperationMkdir

	transferTaskID, deleteTaskID string
	submitErr                    error
	waitErr                      error
	canceled                     bool

	gotSrcEndpoint, gotDstEndpoint, gotLabel string
	gotFiles                                 []globusapi.TransferFilePair
	gotDeleteFiles                           []string
	gotRecursive                             bool
}

func (f *fakeClient) OperationLs(ctx context.Context, endpointID, path string, limit int) error {
	if f.existingDirs[endpointID+":"+path] {
		return nil
	}
	return &globusapi.APIError{Code: "ClientError.NotFound", Message: "no such directory", StatusCode: 404}
}

func (f *fakeClient) OperationMkdir(ctx context.Context, endpointID, path string) error {
	f.mkdirCalls = append(f.mkdirCalls, path)
	if err, ok := f.mkdirErr[path]; ok {
		return err
	}
	return nil
}

func (f *fakeClient) SubmitTransfer(ctx context.Context, srcEndpoint, dstEndpoint, label string, files []globusapi.TransferFilePair) (string, error) {
	f.gotSrcEndpoint, f.gotDstEndpoint, f.gotLabel, f.gotFiles = srcEndpoint, dstEndpoint, label, files
	if f.submitErr != nil {
		return "", f.submitErr
	}
	return f.transferTaskID, nil
}

func (f *fakeClient) SubmitDelete(ctx context.Context, endpoint, label string, recursive bool, files []string) (string, error) {
	f.gotLabel, f.gotRecursive, f.gotDeleteFiles = label, recursive, files
	if f.submitErr != nil {
		return "", f.submitErr
	}
	return f.deleteTaskID, nil
}

func (f *fakeClient) CancelTask(ctx context.Context, taskID string) error {
	f.canceled = true
	return nil
}

func (f *fakeClient) WaitForTask(ctx context.Context, taskID string, logf func(format string, args ...any)) error {
	return f.waitErr
}

func TestFindMkdirBaseParentExists(t *testing.T) {
	// mkdir requests name a directory to create, not a file within one —
	// so even when the leaf's parent already exists, the leaf itself is
	// still always one of the returned childDirs (mkdir() always attempts
	// to create it; an "already exists" response for the leaf is a
	// separate, non-fatal case runMkdir handles, not something
	// findMkdirBase itself special-cases).
	client := &fakeClient{existingDirs: map[string]bool{"ep:/a/b": true}}
	base, children := findMkdirBase(context.Background(), client, "ep", "/a/b/c", logger{})
	if base != "/a/b" {
		t.Fatalf("base = %q", base)
	}
	if !reflect.DeepEqual(children, []string{"c"}) {
		t.Fatalf("children = %v, want [c]", children)
	}
}

func TestFindMkdirBaseWalksUpToExistingAncestor(t *testing.T) {
	// Only /a exists; /a/b and /a/b/c must be created, in that order.
	client := &fakeClient{existingDirs: map[string]bool{"ep:/a": true}}
	base, children := findMkdirBase(context.Background(), client, "ep", "/a/b/c", logger{})
	if base != "/a" {
		t.Fatalf("base = %q", base)
	}
	if !reflect.DeepEqual(children, []string{"b", "c"}) {
		t.Fatalf("children = %v, want [b c] (creation order, shallowest first)", children)
	}
}

func TestFindMkdirBaseFallsBackToRoot(t *testing.T) {
	client := &fakeClient{existingDirs: map[string]bool{}} // nothing exists except "/" by loop termination
	base, children := findMkdirBase(context.Background(), client, "ep", "/x/y", logger{})
	if base != "/" {
		t.Fatalf("base = %q, want /", base)
	}
	if !reflect.DeepEqual(children, []string{"x", "y"}) {
		t.Fatalf("children = %v", children)
	}
}

func TestRunMkdirCreatesMissingDirs(t *testing.T) {
	client := &fakeClient{existingDirs: map[string]bool{"ep-1:/a": true}}
	raw := rawRequest{Endpoint: "ep-1", Files: mustJSON(t, []string{"/a/b/c"})}

	runMkdirWithClient(context.Background(), client, raw, logger{})

	if !reflect.DeepEqual(client.mkdirCalls, []string{"/a/b", "/a/b/c"}) {
		t.Fatalf("mkdirCalls = %v", client.mkdirCalls)
	}
}

func TestRunMkdirTreatsAlreadyExistsAsNonFatal(t *testing.T) {
	client := &fakeClient{
		existingDirs: map[string]bool{"ep-1:/a": true},
		mkdirErr: map[string]error{
			"/a/b": &globusapi.APIError{Code: "ExternalError.MkdirFailed.Exists"},
		},
	}
	raw := rawRequest{Endpoint: "ep-1", Files: mustJSON(t, []string{"/a/b/c"})}

	// Should not panic/exit; both mkdir calls still attempted despite the
	// first "already exists" response.
	runMkdirWithClient(context.Background(), client, raw, logger{})

	if !reflect.DeepEqual(client.mkdirCalls, []string{"/a/b", "/a/b/c"}) {
		t.Fatalf("mkdirCalls = %v", client.mkdirCalls)
	}
}

func TestTransferLabelBothEnvVarsSet(t *testing.T) {
	t.Setenv("PEGASUS_WF_UUID", "wf-1")
	t.Setenv("PEGASUS_DAG_JOB_ID", "job-1")
	if got := transferLabel(); got != "wf-1 - job-1" {
		t.Fatalf("got %q", got)
	}
}

func TestTransferLabelMissingEnvVars(t *testing.T) {
	if got := transferLabel(); got != "" {
		t.Fatalf("got %q, want empty when env vars unset", got)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestRunTransferWithClientSubmitsAndWaits(t *testing.T) {
	client := &fakeClient{transferTaskID: "task-1"}
	raw := rawRequest{
		SrcEndpoint: "src", DstEndpoint: "dst",
		Files: mustJSON(t, []transferFileSpec{{Src: "/a", Dst: "/b"}}),
	}

	runTransferWithClient(context.Background(), client, raw, logger{})

	if client.gotSrcEndpoint != "src" || client.gotDstEndpoint != "dst" {
		t.Fatalf("endpoints = %s -> %s", client.gotSrcEndpoint, client.gotDstEndpoint)
	}
	if len(client.gotFiles) != 1 || client.gotFiles[0].Src != "/a" || client.gotFiles[0].Dst != "/b" {
		t.Fatalf("files = %v", client.gotFiles)
	}
}

func TestRunRemoveWithClientSubmitsAndWaits(t *testing.T) {
	client := &fakeClient{deleteTaskID: "task-2"}
	raw := rawRequest{
		Endpoint:  "ep",
		Recursive: true,
		Files:     mustJSON(t, []string{"/a", "/b"}),
	}

	runRemoveWithClient(context.Background(), client, raw, logger{})

	if !client.gotRecursive {
		t.Fatal("expected recursive=true to reach SubmitDelete")
	}
	if !reflect.DeepEqual(client.gotDeleteFiles, []string{"/a", "/b"}) {
		t.Fatalf("files = %v", client.gotDeleteFiles)
	}
}

func TestRunTransferWithClientLabelFromEnv(t *testing.T) {
	t.Setenv("PEGASUS_WF_UUID", "wf-9")
	t.Setenv("PEGASUS_DAG_JOB_ID", "job-9")
	client := &fakeClient{transferTaskID: "task-3"}
	raw := rawRequest{SrcEndpoint: "s", DstEndpoint: "d", Files: mustJSON(t, []transferFileSpec{})}

	runTransferWithClient(context.Background(), client, raw, logger{})

	if client.gotLabel != "wf-9 - job-9" {
		t.Fatalf("label = %q", client.gotLabel)
	}
}

func TestErrorsAsAPIError(t *testing.T) {
	// Sanity check that errors.As unwraps the way runMkdir relies on.
	var target *globusapi.APIError
	err := error(&globusapi.APIError{Code: "X"})
	if !errors.As(err, &target) || target.Code != "X" {
		t.Fatalf("errors.As failed: %v", target)
	}
}
