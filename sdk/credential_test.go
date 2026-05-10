package sdk_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

func TestAPIKeyCredential(t *testing.T) {
	// Arrange / Act
	c := sdk.APIKeyCredential("alpha")

	// Assert
	if c.Kind != sdk.CredentialAPIKey {
		t.Errorf("Kind: got %d, want CredentialAPIKey", c.Kind)
	}
	if string(c.Bytes) != "alpha" {
		t.Errorf("Bytes: got %q, want %q", c.Bytes, "alpha")
	}
}

func TestJWTCredential(t *testing.T) {
	// Arrange
	token := []byte("eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJhbGljZSJ9.sig")

	// Act
	c := sdk.JWTCredential(token)

	// Assert
	if c.Kind != sdk.CredentialJWT {
		t.Errorf("Kind: got %d, want CredentialJWT", c.Kind)
	}
	if string(c.Bytes) != string(token) {
		t.Errorf("Bytes: got %q, want %q", c.Bytes, token)
	}
}

func TestCredentialKind_AliasesProtoConstants(t *testing.T) {
	// The numeric values must match proto's wire-stable representation
	// so the SDK can't drift away from what the broker decodes.
	cases := []struct {
		name string
		sdk  sdk.CredentialKind
		wire proto.CredentialKind
	}{
		{"None", sdk.CredentialNone, proto.CredentialNone},
		{"APIKey", sdk.CredentialAPIKey, proto.CredentialAPIKey},
		{"JWT", sdk.CredentialJWT, proto.CredentialJWT},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.sdk != c.wire {
				t.Errorf("got %d, want %d", c.sdk, c.wire)
			}
		})
	}
}

func TestLoadCredentialFile_JWT(t *testing.T) {
	// Arrange: file contains a JWT — three base64 segments separated
	// by dots, with optional trailing whitespace that must be trimmed.
	tokenText := "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJhbGljZSJ9.sig\n"
	path := filepath.Join(t.TempDir(), "creds.jwt")
	if err := os.WriteFile(path, []byte(tokenText), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Act
	c, err := sdk.LoadCredentialFile(path)
	if err != nil {
		t.Fatalf("LoadCredentialFile: %v", err)
	}

	// Assert
	if c.Kind != sdk.CredentialJWT {
		t.Errorf("Kind: got %d, want CredentialJWT", c.Kind)
	}
	if string(c.Bytes) != "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJhbGljZSJ9.sig" {
		t.Errorf("Bytes: got %q, want token without trailing whitespace", c.Bytes)
	}
}

func TestLoadCredentialFile_BadPath(t *testing.T) {
	// Arrange / Act
	_, err := sdk.LoadCredentialFile(filepath.Join(t.TempDir(), "missing"))

	// Assert
	if err == nil {
		t.Fatal("expected error for nonexistent credential file")
	}
}

func TestLoadCredentialFile_EmptyFile(t *testing.T) {
	// Arrange
	path := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(path, []byte(""), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Act
	_, err := sdk.LoadCredentialFile(path)

	// Assert
	if err == nil {
		t.Fatal("expected error for empty credential file")
	}
}

func TestLoadCredentialFile_RejectsObviouslyNonJWT(t *testing.T) {
	// LoadCredentialFile defaults to CredentialJWT but should reject
	// content that can't possibly be a JWT (e.g. no dots at all),
	// since silently passing such bytes to the broker only surfaces
	// the failure later as an obscure signature error.
	path := filepath.Join(t.TempDir(), "garbage")
	if err := os.WriteFile(path, []byte("just-a-bare-token-not-a-jwt"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := sdk.LoadCredentialFile(path); err == nil {
		t.Fatal("expected error for content that is not a JWT")
	}
}
