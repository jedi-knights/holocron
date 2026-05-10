package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	cliauth "github.com/jedi-knights/holocron/cli/internal/auth"
)

// runAuth dispatches `auth <subcommand>`.
func runAuth(args []string) error {
	if len(args) == 0 {
		return errors.New("auth: subcommand required (issue | inspect)")
	}
	switch args[0] {
	case "issue":
		return runAuthIssue(args[1:])
	case "inspect":
		return runAuthInspect(args[1:])
	}
	return fmt.Errorf("auth: unknown subcommand %q", args[0])
}

// runAuthIssue signs a JWT with the operator's Ed25519 private key.
// Output goes to stdout (or --output file) so the caller can pipe
// into a credential file the SDK / CLI consumes via
// sdk.LoadCredentialFile or `--credential-file`.
func runAuthIssue(args []string) error {
	fs := flag.NewFlagSet("auth issue", flag.ContinueOnError)
	keyPath := fs.String("key", "", "path to a PEM-encoded PKCS8 Ed25519 private key (required)")
	subject := fs.String("subject", "", "JWT subject — the authenticated identity (required)")
	account := fs.String("account", "", "JWT holocron.account claim (optional; reserved for multi-tenancy)")
	issuer := fs.String("issuer", "", "JWT iss claim (optional; audit-logged by the broker)")
	ttl := fs.Duration("ttl", 24*time.Hour, "token validity window from now")
	output := fs.String("output", "", "write the token to this path instead of stdout")
	allAccess := fs.Bool("all-access", false, "shorthand for --scope produce:* --scope consume:* --scope admin:* (dev/ops convenience; mutually exclusive with --scope)")
	var scopes stringSliceFlag
	fs.Var(&scopes, "scope", "JWT holocron.scopes entry — repeat to add multiple. Grammar: verb:resource[*]; verbs are produce, consume, admin (e.g. --scope produce:events --scope consume:orders.* --scope admin:billing)")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *keyPath == "" {
		return errors.New("auth issue: --key is required")
	}
	if *subject == "" {
		return errors.New("auth issue: --subject is required")
	}
	if *ttl <= 0 {
		return errors.New("auth issue: --ttl must be positive")
	}
	if *allAccess && len(scopes) > 0 {
		return errors.New("auth issue: --all-access and --scope are mutually exclusive")
	}
	if *allAccess {
		scopes = stringSliceFlag{"produce:*", "consume:*", "admin:*"}
	}

	priv, err := cliauth.LoadEd25519PrivateKey(*keyPath)
	if err != nil {
		return err
	}

	now := time.Now()
	claims := cliauth.Claims{
		Subject:  *subject,
		Account:  *account,
		Scopes:   []string(scopes),
		Issuer:   *issuer,
		IssuedAt: now.Unix(),
		Expires:  now.Add(*ttl).Unix(),
	}
	token, err := cliauth.IssueJWT(priv, claims)
	if err != nil {
		return err
	}

	if *output != "" {
		if err := os.WriteFile(*output, append(token, '\n'), 0o600); err != nil {
			return fmt.Errorf("auth issue: write %s: %w", *output, err)
		}
		return nil
	}
	if _, err := os.Stdout.Write(append(token, '\n')); err != nil {
		return err
	}
	return nil
}

// runAuthInspect decodes a JWT (without verifying its signature) and
// prints its header + claims. Useful for an operator to read what's
// in a token a service is presenting — debug, expired-token triage,
// claim auditing.
func runAuthInspect(args []string) error {
	fs := flag.NewFlagSet("auth inspect", flag.ContinueOnError)
	tokenStr := fs.String("token", "", "the JWT to inspect (or pipe via stdin if absent)")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var token []byte
	switch {
	case *tokenStr != "":
		token = []byte(strings.TrimSpace(*tokenStr))
	default:
		// Read from stdin — supports `cat token.jwt | holocronctl auth inspect`.
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("auth inspect: read stdin: %w", err)
		}
		token = []byte(strings.TrimSpace(string(data)))
		if len(token) == 0 {
			return errors.New("auth inspect: no token supplied (use --token or pipe via stdin)")
		}
	}

	claims, header, err := cliauth.DecodeClaimsUnverified(token)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(struct {
			Header cliauth.Header `json:"header"`
			Claims cliauth.Claims `json:"claims"`
		}{Header: header, Claims: claims})
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	fmt.Println("# header")
	if err := enc.Encode(header); err != nil {
		return err
	}
	fmt.Println("# claims")
	if err := enc.Encode(claims); err != nil {
		return err
	}
	if claims.Expires > 0 {
		exp := time.Unix(claims.Expires, 0).UTC()
		remaining := time.Until(exp).Round(time.Second)
		if remaining > 0 {
			fmt.Printf("# exp:  %s (%s remaining)\n", exp.Format(time.RFC3339), remaining)
		} else {
			fmt.Printf("# exp:  %s (EXPIRED %s ago)\n", exp.Format(time.RFC3339), -remaining)
		}
	}
	return nil
}

// stringSliceFlag is a flag.Value that accumulates string values
// across repeated occurrences (e.g. --scope a --scope b).
type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}
