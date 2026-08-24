package globusapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// withFakeAuthServer points authBaseURL at an httptest server for the
// duration of the test.
func withFakeAuthServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	orig := authBaseURL
	authBaseURL = srv.URL
	t.Cleanup(func() { authBaseURL = orig })
	return srv
}

func TestAuthorizeURLContainsPKCEAndScopes(t *testing.T) {
	flow := NewAuthCodeFlow("client-123", []string{TransferAllScope}, false, "")
	raw := flow.AuthorizeURL()

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()

	if q.Get("client_id") != "client-123" {
		t.Errorf("client_id = %q", q.Get("client_id"))
	}
	if q.Get("scope") != TransferAllScope {
		t.Errorf("scope = %q, want %q", q.Get("scope"), TransferAllScope)
	}
	if q.Get("response_type") != "code" {
		t.Errorf("response_type = %q", q.Get("response_type"))
	}
	if q.Get("code_challenge_method") != "S256" {
		t.Errorf("code_challenge_method = %q", q.Get("code_challenge_method"))
	}
	if q.Get("code_challenge") == "" {
		t.Error("code_challenge is empty")
	}
	if q.Get("access_type") != "online" {
		t.Errorf("access_type = %q, want online (not --permanent)", q.Get("access_type"))
	}
	if q.Get("prompt") != "login" {
		t.Errorf("prompt = %q, want login", q.Get("prompt"))
	}
	if q.Get("state") != "_default" {
		t.Errorf("state = %q, want _default", q.Get("state"))
	}
	if q.Get("session_required_single_domain") != "" {
		t.Errorf("expected no domain param when none given, got %q", q.Get("session_required_single_domain"))
	}
}

func TestAuthorizeURLPermanentRequestsOfflineAccess(t *testing.T) {
	flow := NewAuthCodeFlow("client-123", []string{TransferAllScope}, true, "")
	u, _ := url.Parse(flow.AuthorizeURL())
	if got := u.Query().Get("access_type"); got != "offline" {
		t.Fatalf("access_type = %q, want offline for --permanent", got)
	}
}

func TestAuthorizeURLDomainsPassedThrough(t *testing.T) {
	flow := NewAuthCodeFlow("client-123", []string{TransferAllScope}, false, "a.edu,b.edu")
	u, _ := url.Parse(flow.AuthorizeURL())
	if got := u.Query().Get("session_required_single_domain"); got != "a.edu,b.edu" {
		t.Fatalf("session_required_single_domain = %q", got)
	}
}

func TestExchangeCodePrimaryResourceServer(t *testing.T) {
	withFakeAuthServer(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("grant_type") != "authorization_code" {
			t.Errorf("grant_type = %q", r.Form.Get("grant_type"))
		}
		if r.Form.Get("code") != "the-auth-code" {
			t.Errorf("code = %q", r.Form.Get("code"))
		}
		if r.Form.Get("client_id") != "client-123" {
			t.Errorf("client_id = %q", r.Form.Get("client_id"))
		}
		if r.Form.Get("client_secret") != "" {
			t.Errorf("client_secret should never be sent for a native app, got %q", r.Form.Get("client_secret"))
		}
		if r.Form.Get("code_verifier") == "" {
			t.Error("code_verifier missing")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token":    "at-transfer",
			"refresh_token":   "rt-transfer",
			"expires_in":      172800,
			"resource_server": "transfer.api.globus.org",
			"token_type":      "Bearer",
			"scope":           TransferAllScope,
			"other_tokens":    []any{},
		})
	})

	flow := NewAuthCodeFlow("client-123", []string{TransferAllScope}, true, "")
	tokens, err := flow.ExchangeCode(context.Background(), "the-auth-code")
	if err != nil {
		t.Fatal(err)
	}
	if tokens.AccessToken != "at-transfer" || tokens.RefreshToken != "rt-transfer" {
		t.Fatalf("got %+v", tokens)
	}
	if tokens.ExpiresAt.Before(time.Now().Add(23 * time.Hour)) {
		t.Fatalf("expiry too soon: %v", tokens.ExpiresAt)
	}
}

func TestExchangeCodeFromOtherTokens(t *testing.T) {
	// Matches the case where dependent GCS scopes (on other resource
	// servers) were requested alongside transfer.api.globus.org's scope —
	// the transfer token then arrives buried in "other_tokens" instead of
	// being the top-level token.
	withFakeAuthServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token":    "at-gcs-endpoint",
			"resource_server": "some-gcs-endpoint-uuid",
			"expires_in":      3600,
			"other_tokens": []any{
				map[string]any{
					"access_token":    "at-transfer-2",
					"refresh_token":   "rt-transfer-2",
					"expires_in":      172800,
					"resource_server": "transfer.api.globus.org",
				},
			},
		})
	})

	flow := NewAuthCodeFlow("client-123", []string{TransferAllScope}, false, "")
	tokens, err := flow.ExchangeCode(context.Background(), "code")
	if err != nil {
		t.Fatal(err)
	}
	if tokens.AccessToken != "at-transfer-2" || tokens.RefreshToken != "rt-transfer-2" {
		t.Fatalf("got %+v", tokens)
	}
}

func TestExchangeCodeMissingTransferToken(t *testing.T) {
	withFakeAuthServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token":    "at-unrelated",
			"resource_server": "some-other-service",
			"expires_in":      3600,
		})
	})

	flow := NewAuthCodeFlow("client-123", []string{TransferAllScope}, false, "")
	_, err := flow.ExchangeCode(context.Background(), "code")
	if err == nil || !strings.Contains(err.Error(), "transfer.api.globus.org") {
		t.Fatalf("expected a missing-transfer-token error, got %v", err)
	}
}

func TestTokenSourceNoRefreshTokenIsStatic(t *testing.T) {
	ts := TokenSource(context.Background(), "client-123", "at", "", time.Now().Add(time.Hour))
	tok, err := ts.Token()
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "at" {
		t.Fatalf("got %+v", tok)
	}
	// A static source returns the exact same token indefinitely, even past
	// its nominal expiry — matching AccessTokenAuthorizer's behavior (no
	// renewal capability without a refresh token).
}

func TestTokenSourceRefreshesWhenExpired(t *testing.T) {
	var refreshRequests int
	withFakeAuthServer(t, func(w http.ResponseWriter, r *http.Request) {
		refreshRequests++
		r.ParseForm()
		if r.Form.Get("grant_type") != "refresh_token" {
			t.Errorf("grant_type = %q", r.Form.Get("grant_type"))
		}
		if r.Form.Get("refresh_token") != "rt" {
			t.Errorf("refresh_token = %q", r.Form.Get("refresh_token"))
		}
		if r.Form.Get("client_id") != "client-123" {
			t.Errorf("client_id = %q", r.Form.Get("client_id"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "at-refreshed",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	})

	ts := TokenSource(context.Background(), "client-123", "at-expired", "rt", time.Now().Add(-time.Hour))
	tok, err := ts.Token()
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "at-refreshed" {
		t.Fatalf("got %+v", tok)
	}
	if refreshRequests != 1 {
		t.Fatalf("expected exactly one refresh request, got %d", refreshRequests)
	}
}
