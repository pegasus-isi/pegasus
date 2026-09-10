// Command pegasus-globus-online-init initializes OAuth tokens for Globus
// Transfer authentication, via a manual copy/paste authorization-code+PKCE
// flow: it prints an authorize URL, the user logs in and pastes back the
// resulting code, and the exchanged tokens are written to
// ~/.pegasus/globus.conf for pegasus-globus-online to use.
//
// Go port of pegasus-globus-online-init.py, rebuilt against the Globus
// Auth REST API (via internal/globusapi) instead of the globus-sdk Python
// package — see packages/pegasus-globus-online/CLAUDE.md for the decision
// record. Flags, prompts, and the config file format are unchanged.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pegasus-isi/pegasus/packages/pegasus-globus-online/internal/globusapi"
	"github.com/pegasus-isi/pegasus/packages/pegasus-globus-online/internal/globusconf"
)

type args struct {
	permanent   bool
	endpoints   []string
	collections []string
	domains     []string
}

const usage = `usage: pegasus-globus-online-init [-h] [-p] [-e [ENDPOINTS ...]] [-c [COLLECTIONS ...]] [-d [DOMAINS ...]]

Initialize Globus OAuth Tokens

  -h, --help                  show this help message and exit
  -p, --permanent             request a refreshable token
  -e, --endpoints [E ...]     endpoint uuids to acquire manage_collections consent
  -c, --collections [C ...]   collection uuids to acquire data_access consent
  -d, --domains [D ...]       domain requirements identities under the globus account must satisfy
`

// parseArgs hand-rolls argparse's nargs="*" behavior (a flag followed by
// zero or more non-flag tokens) since Go's flag package has no equivalent:
// -e/-c/-d each consume every following argument up to the next token that
// looks like a flag (starts with '-').
func parseArgs(argv []string) (args, error) {
	var a args
	i := 0
	collect := func() []string {
		i++
		var out []string
		for i < len(argv) && !strings.HasPrefix(argv[i], "-") {
			out = append(out, argv[i])
			i++
		}
		return out
	}
	for i < len(argv) {
		switch argv[i] {
		case "-h", "--help":
			fmt.Print(usage)
			os.Exit(0)
		case "-p", "--permanent":
			a.permanent = true
			i++
		case "-e", "--endpoints":
			a.endpoints = collect()
		case "-c", "--collections":
			a.collections = collect()
		case "-d", "--domains":
			a.domains = collect()
		default:
			return args{}, fmt.Errorf("unrecognized argument: %s", argv[i])
		}
	}
	return a, nil
}

func main() {
	a, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	scopes := globusapi.BuildRequestedScopes(a.collections, a.endpoints)
	domains := globusapi.JoinDomains(a.domains)

	flow := globusapi.NewAuthCodeFlow(globusapi.PegasusClientID, scopes, a.permanent, domains)
	fmt.Printf("Please go to this URL and login: %s\n", flow.AuthorizeURL())

	fmt.Print("Please enter the code you get after login here: ")
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	code := strings.TrimSpace(line)

	tokens, err := flow.ExchangeCode(context.Background(), code)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	path, err := globusconf.Path()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg := globusconf.Config{
		ClientID:      globusapi.PegasusClientID,
		TransferAT:    tokens.AccessToken,
		TransferRT:    tokens.RefreshToken,
		TransferATExp: tokens.ExpiresAt.Unix(),
	}
	if err := globusconf.Save(path, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
