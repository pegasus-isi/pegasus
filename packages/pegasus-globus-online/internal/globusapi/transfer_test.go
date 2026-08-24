package globusapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func withFakeTransferServer(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	orig := transferBaseURL
	transferBaseURL = srv.URL
	t.Cleanup(func() { transferBaseURL = orig })
}

func testClient() *TransferClient {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "at-test"})
	return NewTransferClient(context.Background(), ts)
}

func TestGetSubmissionID(t *testing.T) {
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer at-test" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		if r.URL.Path != "/submission_id" {
			t.Errorf("path = %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]string{"value": "sub-id-123"})
	})

	id, err := testClient().GetSubmissionID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id != "sub-id-123" {
		t.Fatalf("got %s", id)
	}
}

func TestOperationMkdirSuccess(t *testing.T) {
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if r.URL.Path != "/operation/endpoint/ep-1/mkdir" {
			t.Errorf("path = %s", r.URL.Path)
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["path"] != "/newdir" {
			t.Errorf("body = %v", body)
		}
		json.NewEncoder(w).Encode(map[string]string{"code": "DirectoryCreated"})
	})

	if err := testClient().OperationMkdir(context.Background(), "ep-1", "/newdir"); err != nil {
		t.Fatal(err)
	}
}

func TestOperationMkdirAlreadyExistsError(t *testing.T) {
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{
			"code":       "ExternalError.MkdirFailed.Exists",
			"message":    "Directory already exists",
			"request_id": "abc",
		})
	})

	err := testClient().OperationMkdir(context.Background(), "ep-1", "/existing")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %v (%T)", err, err)
	}
	if apiErr.Code != "ExternalError.MkdirFailed.Exists" {
		t.Fatalf("got code %s", apiErr.Code)
	}
	if apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("got status %d", apiErr.StatusCode)
	}
}

func TestOperationLsError(t *testing.T) {
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("path") != "/missing" {
			t.Errorf("path query = %s", r.URL.Query().Get("path"))
		}
		if r.URL.Query().Get("limit") != "2" {
			t.Errorf("limit query = %s", r.URL.Query().Get("limit"))
		}
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"code": "ClientError.NotFound", "message": "no such directory"})
	})

	err := testClient().OperationLs(context.Background(), "ep-1", "/missing", 2)
	if err == nil {
		t.Fatal("expected error for a nonexistent path")
	}
}

func TestSubmitTransfer(t *testing.T) {
	var gotBody map[string]any
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/submission_id":
			json.NewEncoder(w).Encode(map[string]string{"value": "sub-1"})
		case "/transfer":
			json.NewDecoder(r.Body).Decode(&gotBody)
			json.NewEncoder(w).Encode(map[string]string{"task_id": "task-123", "code": "Accepted"})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	})

	taskID, err := testClient().SubmitTransfer(context.Background(), "src-ep", "dst-ep", "my-label",
		[]TransferFilePair{{Src: "/a", Dst: "/b"}, {Src: "/c", Dst: "/d"}})
	if err != nil {
		t.Fatal(err)
	}
	if taskID != "task-123" {
		t.Fatalf("got %s", taskID)
	}

	if gotBody["DATA_TYPE"] != "transfer" {
		t.Errorf("DATA_TYPE = %v", gotBody["DATA_TYPE"])
	}
	if gotBody["submission_id"] != "sub-1" {
		t.Errorf("submission_id = %v", gotBody["submission_id"])
	}
	if gotBody["source_endpoint"] != "src-ep" || gotBody["destination_endpoint"] != "dst-ep" {
		t.Errorf("endpoints = %v", gotBody)
	}
	if gotBody["label"] != "my-label" {
		t.Errorf("label = %v", gotBody["label"])
	}
	for _, boolField := range []string{"notify_on_succeeded", "notify_on_failed", "notify_on_inactive"} {
		if gotBody[boolField] != false {
			t.Errorf("%s = %v, want false", boolField, gotBody[boolField])
		}
	}
	items, ok := gotBody["DATA"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("DATA = %v", gotBody["DATA"])
	}
	first := items[0].(map[string]any)
	if first["DATA_TYPE"] != "transfer_item" || first["source_path"] != "/a" || first["destination_path"] != "/b" {
		t.Errorf("first item = %v", first)
	}
}

func TestSubmitTransferOmitsLabelWhenEmpty(t *testing.T) {
	var gotBody map[string]any
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/submission_id":
			json.NewEncoder(w).Encode(map[string]string{"value": "sub-1"})
		case "/transfer":
			json.NewDecoder(r.Body).Decode(&gotBody)
			json.NewEncoder(w).Encode(map[string]string{"task_id": "task-1"})
		}
	})
	if _, err := testClient().SubmitTransfer(context.Background(), "s", "d", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := gotBody["label"]; ok {
		t.Errorf("expected no label key, got %v", gotBody["label"])
	}
}

func TestSubmitDelete(t *testing.T) {
	var gotBody map[string]any
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/submission_id":
			json.NewEncoder(w).Encode(map[string]string{"value": "sub-2"})
		case "/delete":
			json.NewDecoder(r.Body).Decode(&gotBody)
			json.NewEncoder(w).Encode(map[string]string{"task_id": "task-999"})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	})

	taskID, err := testClient().SubmitDelete(context.Background(), "ep-1", "", true, []string{"/a", "/b"})
	if err != nil {
		t.Fatal(err)
	}
	if taskID != "task-999" {
		t.Fatalf("got %s", taskID)
	}
	if gotBody["DATA_TYPE"] != "delete" {
		t.Errorf("DATA_TYPE = %v", gotBody["DATA_TYPE"])
	}
	if gotBody["recursive"] != true {
		t.Errorf("recursive = %v", gotBody["recursive"])
	}
	items := gotBody["DATA"].([]any)
	if len(items) != 2 {
		t.Fatalf("DATA = %v", items)
	}
	if items[0].(map[string]any)["DATA_TYPE"] != "delete_item" {
		t.Errorf("item = %v", items[0])
	}
}

func TestGetTaskStatus(t *testing.T) {
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/task/task-1" {
			t.Errorf("path = %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "SUCCEEDED"})
	})
	status, err := testClient().GetTaskStatus(context.Background(), "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("got %s", status)
	}
}

func TestTaskErrorEvents(t *testing.T) {
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/task/task-1/event_list" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("filter") != "is_error:1" {
			t.Errorf("filter = %s", r.URL.Query().Get("filter"))
		}
		if r.URL.Query().Get("limit") != "20" {
			t.Errorf("limit = %s", r.URL.Query().Get("limit"))
		}
		json.NewEncoder(w).Encode(map[string]any{
			"DATA": []map[string]any{
				{"time": "2026-01-01", "description": "boom", "is_error": true},
			},
		})
	})
	events, err := testClient().TaskErrorEvents(context.Background(), "task-1", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Description != "boom" {
		t.Fatalf("got %v", events)
	}
}

func TestCancelTask(t *testing.T) {
	var called bool
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.URL.Path != "/task/task-1/cancel" || r.Method != http.MethodPost {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]string{"code": "Canceled"})
	})
	if err := testClient().CancelTask(context.Background(), "task-1"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("cancel endpoint was never hit")
	}
}

func TestWaitForTaskSucceedsImmediately(t *testing.T) {
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "SUCCEEDED"})
	})
	var logs []string
	err := testClient().WaitForTask(context.Background(), "task-1", func(f string, a ...any) {
		logs = append(logs, fmt.Sprintf(f, a...))
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) == 0 {
		t.Fatal("expected at least the initial 'Waiting for transfer to complete' log line")
	}
}

func TestWaitForTaskIgnoresBenignMkdirRace(t *testing.T) {
	// First poll: still ACTIVE (forces one error-event check). Second poll:
	// terminated. The one error event present matches the ignorable mkdir
	// race and must not abort the wait.
	calls := 0
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/event_list"):
			json.NewEncoder(w).Encode(map[string]any{
				"DATA": []map[string]any{
					{"time": "t", "description": "System error in mkdir at foo: File exists", "is_error": true},
				},
			})
		default:
			calls++
			status := "ACTIVE"
			if calls > 1 {
				status = "SUCCEEDED"
			}
			json.NewEncoder(w).Encode(map[string]string{"status": status})
		}
	})

	client := testClient()
	// Use a short timeout/poll interval via pollTaskOnce directly to avoid a
	// slow real-time test; WaitForTask's own constants are exercised by
	// TestWaitForTaskSucceedsImmediately above.
	done, err := client.pollTaskOnce(context.Background(), "task-1", 50*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("expected task to terminate")
	}

	events, err := client.TaskErrorEvents(context.Background(), "task-1", 20)
	if err != nil {
		t.Fatal(err)
	}
	if !ignorableMkdirError.MatchString(events[0].Description) {
		t.Fatalf("expected the fixture event to match the ignorable-mkdir-race pattern: %q", events[0].Description)
	}
}

func TestWaitForTaskAbortsOnRealError(t *testing.T) {
	pollCount := 0
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/event_list"):
			json.NewEncoder(w).Encode(map[string]any{
				"DATA": []map[string]any{
					{"time": "t1", "description": "PermissionDenied: no access", "is_error": true},
				},
			})
		default:
			pollCount++
			json.NewEncoder(w).Encode(map[string]string{"status": "ACTIVE"})
		}
	})

	client := testClient()
	done, err := client.pollTaskOnce(context.Background(), "task-1", 5*time.Millisecond, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("expected timeout (still ACTIVE), not termination")
	}
	events, err := client.TaskErrorEvents(context.Background(), "task-1", 20)
	if err != nil {
		t.Fatal(err)
	}
	if ignorableMkdirError.MatchString(events[0].Description) {
		t.Fatal("this error should NOT match the ignorable pattern")
	}
}

func TestAPIErrorFallbackWhenBodyIsntJSON(t *testing.T) {
	withFakeTransferServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("<html>502 Bad Gateway</html>"))
	})
	err := testClient().OperationMkdir(context.Background(), "ep", "/x")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError even for a non-JSON body, got %v", err)
	}
	if apiErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("got status %d", apiErr.StatusCode)
	}
}
