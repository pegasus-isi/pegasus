package globusapi

import (
	"reflect"
	"testing"
)

func TestTransferAllScope(t *testing.T) {
	want := "urn:globus:auth:scope:transfer.api.globus.org:all"
	if TransferAllScope != want {
		t.Fatalf("got %s, want %s", TransferAllScope, want)
	}
}

func TestCollectionDataAccessScope(t *testing.T) {
	got := CollectionDataAccessScope("collection-uuid")
	want := "https://auth.globus.org/scopes/collection-uuid/data_access"
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestEndpointManageCollectionsScope(t *testing.T) {
	got := EndpointManageCollectionsScope("endpoint-uuid")
	want := "urn:globus:auth:scope:endpoint-uuid:manage_collections"
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDependentScope(t *testing.T) {
	got := DependentScope(TransferAllScope, "child-scope")
	want := "urn:globus:auth:scope:transfer.api.globus.org:all[*child-scope]"
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestBuildRequestedScopesNoExtras(t *testing.T) {
	got := BuildRequestedScopes(nil, nil)
	want := []string{TransferAllScope}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestBuildRequestedScopesOrderMatchesPython(t *testing.T) {
	// Matches pegasus-globus-online-init.py's pegasus_scopes list
	// construction exactly: [all], then per collection [dependent,
	// standalone], then per endpoint [manage_collections] — order matters
	// since it becomes the literal space-joined `scope` request parameter.
	got := BuildRequestedScopes([]string{"coll-1"}, []string{"ep-1"})
	want := []string{
		TransferAllScope,
		DependentScope(TransferAllScope, CollectionDataAccessScope("coll-1")),
		CollectionDataAccessScope("coll-1"),
		EndpointManageCollectionsScope("ep-1"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestBuildRequestedScopesMultipleCollectionsAndEndpoints(t *testing.T) {
	got := BuildRequestedScopes([]string{"c1", "c2"}, []string{"e1", "e2"})
	want := []string{
		TransferAllScope,
		DependentScope(TransferAllScope, CollectionDataAccessScope("c1")),
		CollectionDataAccessScope("c1"),
		DependentScope(TransferAllScope, CollectionDataAccessScope("c2")),
		CollectionDataAccessScope("c2"),
		EndpointManageCollectionsScope("e1"),
		EndpointManageCollectionsScope("e2"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestJoinDomains(t *testing.T) {
	if got := JoinDomains(nil); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
	if got := JoinDomains([]string{"a.edu", "b.edu"}); got != "a.edu,b.edu" {
		t.Fatalf("got %q", got)
	}
}
