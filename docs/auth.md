# Authentication

This document covers Holocron's authentication surface — how the broker decides *who* an inbound RPC came from. Authorization (what that identity may do) is a separate Wave-1 follow-on; today every authenticated principal can do everything.

For TLS (encryption + cert verification), see [`tls.md`](tls.md). Auth and TLS compose: TLS protects the wire, auth identifies the caller. Most production deployments want both.

## Trust model

The broker accepts a JWT (JSON Web Token) at handshake, signed with the **operator's Ed25519 private key**. Every connecting client presents its token; the broker verifies the signature against the configured public key and rejects anything that doesn't check out.

The flow:

1. **Operator** generates a long-lived Ed25519 keypair (private key stays with the operator; public key is configured on every broker).
2. **Per user / service**, the operator issues a short-lived JWT (default 24h) signed by the private key, listing the subject, account, and scopes.
3. **Client** presents the JWT at every new connection's handshake.
4. **Broker** verifies the signature, expiry, and denylist. The `Principal` (subject + account + scopes) becomes the connection's identity.

Why JWT specifically? Because it carries claims (subject, account, expiry, scopes) without an additional round-trip to a credential server. Why Ed25519? Because it's the same primitive NATS JetStream uses for NKey-style identities — verification is fast, signatures are small (64 bytes), keys are small (32 bytes), and it's in the Go standard library. No third-party JWT dependency.

## Generating an issuer keypair

OpenSSL produces an Ed25519 keypair in two commands:

```bash
# Private key (kept with the operator; never deployed to brokers)
openssl genpkey -algorithm Ed25519 -out issuer-key.pem

# Public key (deployed to every broker)
openssl pkey -in issuer-key.pem -pubout -out issuer-key.pub.pem
```

The public-key file looks like:

```
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEA[base64-PKIX-encoded-public-key]
-----END PUBLIC KEY-----
```

The broker's `--auth-issuer-key` flag points at this public-key file.

## Configuring the broker

```bash
holocrond \
  --data-dir /var/lib/holocron \
  --listen 0.0.0.0:9092 \
  --tls-cert /etc/holocron/cert.pem \
  --tls-key /etc/holocron/cert-key.pem \
  --auth-issuer-key /etc/holocron/issuer.pub.pem
```

Or via environment:

```bash
export HOLOCRON_AUTH_ISSUER_KEY=/etc/holocron/issuer.pub.pem
```

The startup banner reports the auth scheme alongside the wire scheme:

```
listening on 0.0.0.0:9092 (wire v1, TLS 1.3, auth=jwt)
```

Once auth is enabled the broker rejects every handshake that doesn't carry a valid JWT. **Existing plaintext or API-key clients will fail with `StatusUnauthorized` after the restart** — this is the intended transition behaviour, not silent downgrade. Issue tokens to your services before flipping the flag.

## Issuing a JWT

A JWT is a base64url-encoded `header.payload.signature` triple. The header declares EdDSA, the payload carries the claims:

| Claim | Source | Purpose |
|---|---|---|
| `sub` | required | Subject — the authenticated identity (e.g. `billing-svc-01`) |
| `exp` | required | Expiry — Unix seconds. The broker rejects tokens with no `exp`. |
| `iat` | optional | Issued-at — Unix seconds. Audit-logged. |
| `nbf` | optional | Not-before — Unix seconds. Useful for staged rollouts. |
| `iss` | optional | Issuer name — operator identifier; audit-logged. |
| `holocron.account` | optional | Tenant the subject belongs to. Inert in v1; carried for forward compatibility with multi-tenancy. |
| `holocron.scopes` | optional | List of `verb:resource` permissions (e.g. `produce:events`). Inert in v1 (no authorization yet); carried for forward compatibility. |

### Issuing with `holocronctl auth issue`

The CLI signs a JWT directly:

```bash
holocronctl auth issue \
  --key issuer-key.pem \
  --subject billing-svc-01 \
  --account default \
  --scope produce:events \
  --scope consume:orders \
  --ttl 24h \
  --output /etc/holocron/billing-svc.jwt
```

`--output` writes the token to a file (mode `0600`); omit it to print to stdout (useful for piping into `kubectl create secret`). `--scope` accepts repeated values. `--issuer` sets the optional `iss` claim — the operator identity that signed.

### Inspecting a token

`holocronctl auth inspect` decodes any JWT (no signature verification — it's a debugging tool). Useful for triaging "why was my client rejected" cases:

```bash
$ holocronctl auth inspect --token "$(cat /etc/holocron/billing-svc.jwt)"
# header
{"alg":"EdDSA","typ":"JWT"}
# claims
{"sub":"billing-svc-01","holocron.account":"default","holocron.scopes":["produce:events"],"iat":1747000000,"exp":1747086400}
# exp:  2026-05-13T12:00:00Z (23h59m48s remaining)
```

Pipe a token in via stdin if you'd rather: `cat token.jwt | holocronctl auth inspect`. Add `--json` for machine-readable output.

### Connecting clients

Subcommands accept `--credential-file` (and the matching `HOLOCRON_CREDENTIAL_FILE` env var) to dial a JWT-protected broker:

```bash
holocronctl topic list --addr broker.example.com:9092 \
  --tls-ca /etc/holocron/ca.pem \
  --credential-file /etc/holocron/billing-svc.jwt
```

`--credential-file` and the legacy `--api-key` are mutually exclusive — supply one or the other. The Go SDK has the matching `holocronnet.WithCredential(sdk.LoadCredentialFile(path))`; see [`integration-go.md`](integration-go.md#authentication).

## Denylist (revocation)

JWT revocation is famously hard — once issued, a signed token is valid until its `exp`. Two mitigations:

1. **Short TTLs.** Default 24h, recommended. Limits the blast radius of a leaked token.
2. **Denylist file.** Optional file listing subjects to reject regardless of token validity. Use it when a credential is compromised.

Denylist file format:

```
# /etc/holocron/auth-denylist.txt
# Compromised tokens; remove entries when the corresponding token's
# original `exp` has passed.
billing-svc-old-deploy
ops-tmp-2026-05-09
```

Blank lines and `#` comments are skipped. Configure with:

```bash
holocrond --auth-denylist /etc/holocron/auth-denylist.txt ...
```

### SIGHUP reload

The broker reloads the denylist file on `SIGHUP`. No restart needed:

```bash
echo "compromised-svc" >> /etc/holocron/auth-denylist.txt
kill -HUP $(pgrep holocrond)
```

The reload is atomic — no in-flight handshake observes a half-loaded set. A reload failure (file unreadable, parse error) keeps the existing entries in place and logs the failure to stderr.

`--auth-denylist` requires `--auth-issuer-key` — a denylist has nothing to attach to without a verifier.

## Configuration reference

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--auth-issuer-key` | `HOLOCRON_AUTH_ISSUER_KEY` | — | PEM-encoded PKIX Ed25519 public key. Presence enables JWT-required auth. |
| `--auth-denylist` | `HOLOCRON_AUTH_DENYLIST` | — | Path to a denylist file (one subject per line). Reloaded on `SIGHUP`. Requires `--auth-issuer-key`. |

## Limitations and roadmap

- **No mTLS-CN-as-Principal mapping** — the design includes `--auth-mtls-cn-mapping` so a verified client cert's CN can serve as the Principal without a separate JWT. Carved out to a smaller follow-on PR.
- **No authorization yet.** Every authenticated Principal has full access. Per-topic ACLs and per-account quotas already exist for the legacy API-key path; threading the `Principal.Scopes` field through them is on the Wave-1 list.
- **Single signing key.** Multi-key trust ("accept tokens signed by either the old or new key during rollover") is a planned follow-on. Today, rotating the operator key is a planned restart.
- **Operator key sits on disk.** A KMS adapter (AWS KMS / Vault Transit) is on the radar — the `Signer` interface is shaped for it — but file-on-disk is the v1 default.
