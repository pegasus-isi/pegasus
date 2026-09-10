package globusapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

// TransferClient is a minimal client for the Globus Transfer REST API
// (https://docs.globus.org/api/transfer/), covering only the operations
// GlobusOnlineHandler's mkdir/transfer/remove request specs need.
type TransferClient struct {
	http *http.Client
}

// NewTransferClient wraps ts in an *http.Client that attaches a Bearer
// Authorization header (and transparently refreshes via the token endpoint
// when ts is a refreshing source), matching how the SDK's
// AccessTokenAuthorizer/RefreshTokenAuthorizer inject credentials.
func NewTransferClient(ctx context.Context, ts oauth2.TokenSource) *TransferClient {
	return &TransferClient{http: oauth2.NewClient(ctx, ts)}
}

func (c *TransferClient) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	u := transferBaseURL + "/" + strings.TrimPrefix(path, "/")
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 300 {
		apiErr := &APIError{StatusCode: resp.StatusCode}
		if jsonErr := json.Unmarshal(respBody, apiErr); jsonErr != nil || apiErr.Code == "" {
			apiErr.Code = "Error"
			apiErr.Message = string(respBody)
		}
		return apiErr
	}

	if out != nil && len(respBody) > 0 {
		return json.Unmarshal(respBody, out)
	}
	return nil
}

// deadline reproduces `str(datetime.utcnow() + timedelta(hours=24))` — the
// value transfer.py/pegasus-globus-online.py sets on every submitted
// transfer/delete task, effectively a 24h kill switch that in practice
// never fires (tasks complete or are canceled long before it does).
func deadline() string {
	return time.Now().UTC().Add(24 * time.Hour).Format("2006-01-02 15:04:05.000000")
}

// GetSubmissionID fetches a fresh idempotency token for a transfer/delete
// submission, matching TransferClient.get_submission_id()["value"].
func (c *TransferClient) GetSubmissionID(ctx context.Context) (string, error) {
	var out struct {
		Value string `json:"value"`
	}
	if err := c.do(ctx, http.MethodGet, "submission_id", nil, nil, &out); err != nil {
		return "", err
	}
	return out.Value, nil
}

// OperationLs lists a directory, matching operation_ls(). Only used (by
// Mkdir, below) to test whether a path exists; the listing itself is
// discarded, matching the Python original's use of operation_ls purely as
// an existence probe.
func (c *TransferClient) OperationLs(ctx context.Context, endpointID, path string, limit int) error {
	q := url.Values{}
	if path != "" {
		q.Set("path", path)
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	return c.do(ctx, http.MethodGet, fmt.Sprintf("operation/endpoint/%s/ls", endpointID), q, nil, nil)
}

// OperationMkdir creates one directory (non-recursive — the Transfer API,
// like mkdir(1) without -p, fails if the parent doesn't exist), matching
// operation_mkdir().
func (c *TransferClient) OperationMkdir(ctx context.Context, endpointID, path string) error {
	body := map[string]any{"DATA_TYPE": "mkdir", "path": path}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("operation/endpoint/%s/mkdir", endpointID), nil, body, nil)
}

type transferItem struct {
	DataType        string `json:"DATA_TYPE"`
	SourcePath      string `json:"source_path"`
	DestinationPath string `json:"destination_path"`
}

// TransferFilePair is one src/dst entry of a transfer request, matching a
// {"src":..., "dst":...} entry in the JSON spec pegasus-transfer's
// GlobusOnlineHandler writes.
type TransferFilePair struct{ Src, Dst string }

// SubmitTransfer submits a transfer task, matching transfer.py's use of
// TransferData/add_item/submit_transfer. label follows the same
// $PEGASUS_WF_UUID/$PEGASUS_DAG_JOB_ID convention as the Python original
// (built by the caller, since those env vars are read at the CLI layer).
func (c *TransferClient) SubmitTransfer(ctx context.Context, srcEndpoint, dstEndpoint, label string, files []TransferFilePair) (taskID string, err error) {
	submissionID, err := c.GetSubmissionID(ctx)
	if err != nil {
		return "", err
	}

	items := make([]transferItem, len(files))
	for i, f := range files {
		items[i] = transferItem{DataType: "transfer_item", SourcePath: f.Src, DestinationPath: f.Dst}
	}

	body := map[string]any{
		"DATA_TYPE":            "transfer",
		"submission_id":        submissionID,
		"source_endpoint":      srcEndpoint,
		"destination_endpoint": dstEndpoint,
		"deadline":             deadline(),
		"notify_on_succeeded":  false,
		"notify_on_failed":     false,
		"notify_on_inactive":   false,
		"DATA":                 items,
	}
	if label != "" {
		body["label"] = label
	}

	var out struct {
		TaskID string `json:"task_id"`
	}
	if err := c.do(ctx, http.MethodPost, "transfer", nil, body, &out); err != nil {
		return "", err
	}
	return out.TaskID, nil
}

type deleteItem struct {
	DataType string `json:"DATA_TYPE"`
	Path     string `json:"path"`
}

// SubmitDelete submits a delete task, matching
// pegasus-globus-online.py's remove()/DeleteData/add_item/submit_delete.
// (That function carries a stale docstring claiming deletes are faked via
// zero-byte-file transfers; the actual code has always submitted a real
// DeleteData delete task, which is what this reproduces.)
func (c *TransferClient) SubmitDelete(ctx context.Context, endpoint, label string, recursive bool, files []string) (taskID string, err error) {
	submissionID, err := c.GetSubmissionID(ctx)
	if err != nil {
		return "", err
	}

	items := make([]deleteItem, len(files))
	for i, f := range files {
		items[i] = deleteItem{DataType: "delete_item", Path: f}
	}

	body := map[string]any{
		"DATA_TYPE":           "delete",
		"submission_id":       submissionID,
		"endpoint":            endpoint,
		"deadline":            deadline(),
		"recursive":           recursive,
		"notify_on_succeeded": false,
		"notify_on_failed":    false,
		"notify_on_inactive":  false,
		"DATA":                items,
	}
	if label != "" {
		body["label"] = label
	}

	var out struct {
		TaskID string `json:"task_id"`
	}
	if err := c.do(ctx, http.MethodPost, "delete", nil, body, &out); err != nil {
		return "", err
	}
	return out.TaskID, nil
}

// GetTaskStatus fetches a task's current status field
// (ACTIVE/SUCCEEDED/FAILED/...), matching get_task(task_id)["status"].
func (c *TransferClient) GetTaskStatus(ctx context.Context, taskID string) (string, error) {
	var out struct {
		Status string `json:"status"`
	}
	if err := c.do(ctx, http.MethodGet, "task/"+taskID, nil, nil, &out); err != nil {
		return "", err
	}
	return out.Status, nil
}

// TaskEvent is one entry of a task_event_list() response.
type TaskEvent struct {
	Time        string `json:"time"`
	Description string `json:"description"`
	IsError     bool   `json:"is_error"`
}

// TaskErrorEvents fetches up to limit error events for a task, matching
// task_event_list(task_id=..., limit=20, query_params={"filter": "is_error:1"}).
func (c *TransferClient) TaskErrorEvents(ctx context.Context, taskID string, limit int) ([]TaskEvent, error) {
	q := url.Values{"filter": {"is_error:1"}}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	var out struct {
		Data []TaskEvent `json:"DATA"`
	}
	if err := c.do(ctx, http.MethodGet, "task/"+taskID+"/event_list", q, nil, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// CancelTask cancels a task, matching cancel_task() — errors are the
// caller's concern; the Python original silently swallows them when called
// from a signal handler, which callers here should replicate explicitly if
// that's the context (see cmd/pegasus-globus-online's signal handling).
func (c *TransferClient) CancelTask(ctx context.Context, taskID string) error {
	return c.do(ctx, http.MethodPost, "task/"+taskID+"/cancel", nil, nil, nil)
}

// taskWaitTimeout/taskWaitPollInterval reproduce the two literal constants
// baked into wait_for_task()'s and the SDK's task_wait()'s call in the
// Python original: `wait_timeout = 60` (the outer progress-logging cadence)
// and the SDK task_wait() default `polling_interval=10` (never overridden
// by the caller).
const (
	taskWaitTimeout      = 60 * time.Second
	taskWaitPollInterval = 10 * time.Second
)

// ignorableMkdirError matches the regex wait_for_task() uses to swallow a
// specific benign race: a concurrent/retried transfer already created a
// destination directory kickstart/PegasusLite's mkdir step also tries to
// create.
var ignorableMkdirError = regexp.MustCompile(`System error in mkdir.*File exists`)

// pollTaskOnce reproduces TransferClient.task_wait()'s inner algorithm
// exactly (check-then-sleep, not sleep-then-check, and check timeout before
// sleeping so the total wait never overshoots by one extra interval):
// polls GetTaskStatus every pollInterval until the task leaves ACTIVE or
// timeout elapses. Returns true if the task terminated (any status other
// than ACTIVE), false if it timed out still ACTIVE.
func (c *TransferClient) pollTaskOnce(ctx context.Context, taskID string, timeout, pollInterval time.Duration) (done bool, err error) {
	var waited time.Duration
	for {
		status, err := c.GetTaskStatus(ctx, taskID)
		if err != nil {
			return false, err
		}
		if status != "ACTIVE" {
			return true, nil
		}
		waited += pollInterval
		if waited > timeout {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// WaitForTask reproduces pegasus-globus-online.py's wait_for_task(): block
// until the task terminates, logging progress every taskWaitTimeout and
// checking for fatal errors along the way (the "System error in
// mkdir...File exists" race is logged and ignored; anything else aborts
// with an error identifying the failing event).
func (c *TransferClient) WaitForTask(ctx context.Context, taskID string, logf func(format string, args ...any)) error {
	logf("Waiting for transfer to complete")
	loopCounter := 0
	for {
		done, err := c.pollTaskOnce(ctx, taskID, taskWaitTimeout, taskWaitPollInterval)
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		loopCounter++
		logf("Globus transfer task %s is still running (%d seconds)", taskID, loopCounter*int(taskWaitTimeout.Seconds()))

		events, err := c.TaskErrorEvents(ctx, taskID, 20)
		if err != nil {
			return err
		}
		for _, e := range events {
			details := strings.NewReplacer("\n", " ", "\r", " ").Replace(e.Description)
			if ignorableMkdirError.MatchString(details) {
				logf("Ignoring mkdir error: %s", details)
				continue
			}
			return fmt.Errorf("error on globus transfer task %s at %s: %s", taskID, e.Time, details)
		}
	}
}
