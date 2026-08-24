// Package globusapi implements just enough of the Globus Auth and Transfer
// REST APIs (https://docs.globus.org/api/transfer/overview/) for
// pegasus-globus-online and pegasus-globus-online-init, replacing the
// globus-sdk (Python) dependency those tools used to have. It is not a
// general-purpose Globus SDK — only the operations pegasus-transfer's
// GlobusOnlineHandler actually needs (mkdir, transfer, delete, task
// wait/cancel) and the OAuth2/PKCE Native App flow needed to acquire tokens
// for them.
package globusapi

import "strings"

// PegasusClientID is Pegasus's registered Globus Auth Native App client,
// matching pegasus-globus-online-init.py's hardcoded client_id.
const PegasusClientID = "d7382f5a-4ea3-4b69-b094-99c392fc820d"

// authBaseURL and transferBaseURL are package-level vars, not consts, so
// tests can point them at an httptest server instead of the real Globus
// services.
var (
	authBaseURL     = "https://auth.globus.org"
	transferBaseURL = "https://transfer.api.globus.org/v0.10"
)

const transferResourceServer = "transfer.api.globus.org"

// TransferAllScope is the top-level Transfer API scope
// ("urn:globus:auth:scope:transfer.api.globus.org:all"), matching
// globus_sdk.scopes.TransferScopes.all — every scope request pegasus makes
// includes this one.
var TransferAllScope = urnScopeString(transferResourceServer, "all")

// urnScopeString matches ScopeBuilder.urn_scope_string(): the Globus Auth
// URN scope format used for e.g. TransferScopes.all and
// GCSEndpointScopeBuilder.manage_collections.
func urnScopeString(resourceServer, name string) string {
	return "urn:globus:auth:scope:" + resourceServer + ":" + name
}

// urlScopeString matches ScopeBuilder.url_scope_string(): the URL scope
// format used for e.g. GCSCollectionScopeBuilder.data_access.
func urlScopeString(resourceServer, name string) string {
	return authBaseURL + "/scopes/" + resourceServer + "/" + name
}

// CollectionDataAccessScope returns a GCS mapped collection's data_access
// scope, matching GCSCollectionScopeBuilder(collectionID).data_access.
func CollectionDataAccessScope(collectionID string) string {
	return urlScopeString(collectionID, "data_access")
}

// EndpointManageCollectionsScope returns a GCS endpoint's manage_collections
// scope, matching GCSEndpointScopeBuilder(endpointID).manage_collections.
func EndpointManageCollectionsScope(endpointID string) string {
	return urnScopeString(endpointID, "manage_collections")
}

// DependentScope composes a scope string with one optional dependency,
// matching MutableScope.serialize()'s format when given exactly one
// optional child (the only shape pegasus-globus-online-init.py ever
// constructs): parent + "[*" + child + "]".
func DependentScope(parent, child string) string {
	return parent + "[*" + child + "]"
}

// BuildRequestedScopes reproduces pegasus-globus-online-init.py's
// pegasus_scopes list construction exactly, in the same order (order
// matters: it becomes the literal, space-joined `scope` request parameter):
//
//	[TransferScopes.all]
//	for each collection c: [all[*c.data_access], c.data_access]
//	for each endpoint e:   [e.manage_collections]
func BuildRequestedScopes(collections, endpoints []string) []string {
	scopes := []string{TransferAllScope}
	for _, c := range collections {
		access := CollectionDataAccessScope(c)
		scopes = append(scopes, DependentScope(TransferAllScope, access), access)
	}
	for _, e := range endpoints {
		scopes = append(scopes, EndpointManageCollectionsScope(e))
	}
	return scopes
}

// JoinDomains matches utils.commajoin() as used for
// session_required_single_domain: a plain comma join, no escaping.
func JoinDomains(domains []string) string {
	return strings.Join(domains, ",")
}
