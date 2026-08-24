package globusapi

import "fmt"

// APIError is a Globus Transfer API error response body:
// {"code": "...", "message": "...", "request_id": "..."}.
type APIError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`

	StatusCode int `json:"-"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Globus Transfer API error %d %s: %s (request_id=%s)", e.StatusCode, e.Code, e.Message, e.RequestID)
}
