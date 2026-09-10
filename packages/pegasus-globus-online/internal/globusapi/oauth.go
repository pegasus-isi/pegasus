package globusapi

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/oauth2"
)

// nativeAppRedirectURI is the SDK's default redirect_uri for a Native App
// flow: a Globus-hosted page that just displays the code for copy/paste,
// matching GlobusNativeAppFlowManager's default
// (auth_client.base_url + "/v2/web/auth-code").
func nativeAppRedirectURI() string { return authBaseURL + "/v2/web/auth-code" }

// oauthConfig builds the oauth2.Config for the Native App (public client,
// no secret) authorization-code+PKCE flow against Globus Auth.
// AuthStyleInParams is set explicitly so client_id always travels as a form
// parameter and never as HTTP Basic auth — the Globus SDK's NativeAppClient
// uses a NullAuthorizer for exactly this reason (a native app has no secret
// to authenticate with).
func oauthConfig(clientID string, scopes []string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:    clientID,
		RedirectURL: nativeAppRedirectURI(),
		Scopes:      scopes,
		Endpoint: oauth2.Endpoint{
			AuthURL:   authBaseURL + "/v2/oauth2/authorize",
			TokenURL:  authBaseURL + "/v2/oauth2/token",
			AuthStyle: oauth2.AuthStyleInParams,
		},
	}
}

// AuthCodeFlow drives the interactive (copy/paste) half of a Native App
// PKCE flow: it builds the authorize URL. The caller is responsible for
// printing it, collecting the pasted auth code from the user, and calling
// Exchange with the same AuthCodeFlow.
type AuthCodeFlow struct {
	conf     *oauth2.Config
	verifier string
	offline  bool
	domains  string
}

// NewAuthCodeFlow starts a Native App authorization-code+PKCE flow
// requesting the given scopes. permanent requests a refresh token
// (access_type=offline, matching --permanent); domains, if non-empty, is
// passed as session_required_single_domain (comma-joined by the caller via
// JoinDomains).
func NewAuthCodeFlow(clientID string, scopes []string, permanent bool, domains string) *AuthCodeFlow {
	return &AuthCodeFlow{
		conf:     oauthConfig(clientID, scopes),
		verifier: oauth2.GenerateVerifier(),
		offline:  permanent,
		domains:  domains,
	}
}

// AuthorizeURL returns the URL the user should visit to log in and consent,
// matching NativeAppAuthClient.oauth2_get_authorize_url(prompt="login").
func (f *AuthCodeFlow) AuthorizeURL() string {
	accessType := "online"
	if f.offline {
		accessType = "offline"
	}
	opts := []oauth2.AuthCodeOption{
		oauth2.S256ChallengeOption(f.verifier),
		oauth2.SetAuthURLParam("access_type", accessType),
		oauth2.SetAuthURLParam("prompt", "login"),
	}
	if f.domains != "" {
		opts = append(opts, oauth2.SetAuthURLParam("session_required_single_domain", f.domains))
	}
	// "_default" matches oauth2_start_flow's default `state` parameter.
	return f.conf.AuthCodeURL("_default", opts...)
}

// TransferTokens are the transfer.api.globus.org-scoped credentials pulled
// out of a token response, matching what
// token_response.by_resource_server["transfer.api.globus.org"] yields —
// only this resource server's tokens are kept, exactly as
// pegasus-globus-online-init.py does; any dependent GCS scope tokens
// (data_access, manage_collections) granted alongside are intentionally
// discarded, matching current behavior.
type TransferTokens struct {
	AccessToken  string
	RefreshToken string // empty when refresh tokens weren't requested (not --permanent)
	ExpiresAt    time.Time
}

// ExchangeCode exchanges the user-pasted authorization code for tokens,
// matching NativeAppAuthClient.oauth2_exchange_code_for_tokens +
// by_resource_server["transfer.api.globus.org"].
func (f *AuthCodeFlow) ExchangeCode(ctx context.Context, code string) (TransferTokens, error) {
	tok, err := f.conf.Exchange(ctx, code, oauth2.VerifierOption(f.verifier))
	if err != nil {
		return TransferTokens{}, fmt.Errorf("token exchange failed: %w", err)
	}
	return extractTransferTokens(tok)
}

// extractTransferTokens finds the transfer.api.globus.org entry in a token
// response, whether it's the primary token (the common case here, since
// TransferScopes.all is always the first requested scope) or buried in
// "other_tokens" (present when dependent GCS scopes on other resource
// servers were also requested, per BuildRequestedScopes).
func extractTransferTokens(tok *oauth2.Token) (TransferTokens, error) {
	if rs, _ := tok.Extra("resource_server").(string); rs == transferResourceServer {
		return TransferTokens{
			AccessToken:  tok.AccessToken,
			RefreshToken: tok.RefreshToken,
			ExpiresAt:    tok.Expiry,
		}, nil
	}

	others, _ := tok.Extra("other_tokens").([]any)
	for _, raw := range others {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if rs, _ := entry["resource_server"].(string); rs != transferResourceServer {
			continue
		}
		access, _ := entry["access_token"].(string)
		refresh, _ := entry["refresh_token"].(string)
		expiresIn, _ := entry["expires_in"].(float64)
		return TransferTokens{
			AccessToken:  access,
			RefreshToken: refresh,
			ExpiresAt:    time.Now().Add(time.Duration(expiresIn) * time.Second),
		}, nil
	}

	return TransferTokens{}, fmt.Errorf("no %s token in the Globus Auth response", transferResourceServer)
}

// TokenSource builds an oauth2.TokenSource for calling the Transfer API
// with the given (possibly already-cached) credentials, refreshing
// automatically via the token endpoint when a refresh token is present and
// the access token has expired — matching acquire_clients()'s choice
// between AccessTokenAuthorizer (no refresh token: static, unrenewable) and
// RefreshTokenAuthorizer (refresh token present: auto-renews).
func TokenSource(ctx context.Context, clientID, accessToken, refreshToken string, expiresAt time.Time) oauth2.TokenSource {
	base := &oauth2.Token{AccessToken: accessToken, RefreshToken: refreshToken, Expiry: expiresAt}
	if refreshToken == "" {
		return oauth2.StaticTokenSource(base)
	}
	conf := oauthConfig(clientID, nil)
	return oauth2.ReuseTokenSource(base, conf.TokenSource(ctx, base))
}
