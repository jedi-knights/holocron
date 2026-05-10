# TLS

Holocron's wire-protocol listener can be wrapped in TLS. This document covers the operator-facing surface added in PR 2 of the Wave 1 production-readiness sequence: client TLS for the broker's wire port. Intra-cluster (Raft) TLS is configured separately and lands in PR 5.

## Trust model

The broker presents an X.509 certificate to every connecting client. The client verifies the certificate against a CA bundle it controls. By default the broker does **not** request a client certificate; mTLS is opt-in via `--tls-client-ca` (verify if presented) plus `--tls-require-client-cert` (require + verify).

| Mode | Flags | Server presents | Client presents | Failure mode |
|---|---|---|---|---|
| Plain | none | — | — | — |
| Server TLS | `--tls-cert`, `--tls-key` | server cert | nothing | client without `RootCAs` rejects the handshake |
| Optional mTLS | `+ --tls-client-ca` | server cert | optional cert | invalid client cert rejected; missing client cert allowed |
| Required mTLS | `+ --tls-require-client-cert` | server cert | required cert | missing or invalid client cert rejected |

Holocron negotiates **TLS 1.3** by default. TLS 1.2 is available for legacy intermediaries via `--tls-min-version 1.2`; doing so opts you in to the standard library's default cipher-suite list, which is worth auditing for your threat model.

## Generating a self-signed cert (lab use only)

For local development or tests, OpenSSL produces a usable cert in one command:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout key.pem -out cert.pem \
  -days 30 -nodes \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1"
```

Self-signed certs let you run a TLS broker without a CA, but every client must trust `cert.pem` directly. For anything beyond local experimentation, use a real CA — Let's Encrypt for public-facing brokers, an internal CA (cert-manager, smallstep, or your existing PKI) for private deployments.

## Enabling server TLS

Start the broker with the cert and key:

```bash
holocrond \
  --data-dir /var/lib/holocron \
  --listen 0.0.0.0:9092 \
  --tls-cert /etc/holocron/cert.pem \
  --tls-key /etc/holocron/key.pem
```

Or via environment, which mirrors the existing `HOLOCRON_*` pattern:

```bash
export HOLOCRON_TLS_CERT=/etc/holocron/cert.pem
export HOLOCRON_TLS_KEY=/etc/holocron/key.pem
holocrond
```

The startup banner reports the negotiated scheme:

```
listening on 0.0.0.0:9092 (wire v1, TLS 1.3)
```

## Enabling mTLS

Add the CA bundle that signs your client certs:

```bash
holocrond \
  --tls-cert /etc/holocron/server.pem \
  --tls-key /etc/holocron/server-key.pem \
  --tls-client-ca /etc/holocron/clients-ca.pem \
  --tls-require-client-cert
```

`--tls-client-ca` alone enables **optional mTLS** — clients that present a cert get verified, clients that don't are still accepted. Adding `--tls-require-client-cert` rejects connections without a client cert. Both modes are reported in the startup banner:

```
listening on 0.0.0.0:9092 (wire v1, TLS 1.3 (mTLS required))
```

## Connecting from the SDK

The Go SDK exposes `holocronnet.WithTLS(*tls.Config)` for client-side TLS. In the lab, point its `RootCAs` at the same `cert.pem`:

```go
caPEM, _ := os.ReadFile("cert.pem")
pool := x509.NewCertPool()
pool.AppendCertsFromPEM(caPEM)

tr, err := holocronnet.Dial("broker.example.com:9092", holocronnet.WithTLS(&tls.Config{
    RootCAs:    pool,
    ServerName: "broker.example.com",
    MinVersion: tls.VersionTLS13,
}))
```

For production, omit `RootCAs` entirely so the system trust store is used (assuming the broker uses a publicly-trusted cert).

### Round-trip with the bundled examples

`examples/producer` and `examples/consumer` accept `--tls-ca` (verify the broker's cert against a custom CA) and `--tls-skip-verify` (lab escape hatch). Three terminals reproduce the loop end to end:

```bash
# Terminal 1 — TLS broker
holocrond \
  --memory --listen 127.0.0.1:9092 \
  --tls-cert cert.pem --tls-key key.pem

# Terminal 2 — producer over TLS
go run ./examples/producer --addr 127.0.0.1:9092 --tls-ca cert.pem --count 5

# Terminal 3 — consumer over TLS
go run ./examples/consumer --addr 127.0.0.1:9092 --tls-ca cert.pem
```

`HOLOCRON_TLS_CA` is honoured as the env-var fallback for `--tls-ca` so the same config can be set process-wide.

## Configuration reference

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--tls-cert` | `HOLOCRON_TLS_CERT` | — | PEM cert chain. Presence enables TLS. |
| `--tls-key` | `HOLOCRON_TLS_KEY` | — | PEM private key matching `--tls-cert`. |
| `--tls-client-ca` | `HOLOCRON_TLS_CLIENT_CA` | — | PEM CA bundle for client-cert verification. |
| `--tls-require-client-cert` | `HOLOCRON_TLS_REQUIRE_CLIENT_CERT` | `false` | Reject clients without a verified cert. Requires `--tls-client-ca`. |
| `--tls-min-version` | `HOLOCRON_TLS_MIN_VERSION` | `1.3` | Minimum TLS version: `1.2` or `1.3`. |

## Limitations and roadmap

- **Cert rotation requires a restart.** Cert material is read once at startup. Hot-reload on `SIGHUP` is a planned follow-on after PR 6.
- **Cluster (Raft) traffic is still plaintext** until PR 5 lands. If you run a `--cluster` deployment, make sure the Raft port stays on a trusted network until then.
- **No client cert → identity mapping yet.** Required mTLS confirms the client holds a cert signed by the configured CA but does not yet derive an authenticated principal from it. JWT/account-based auth is the next item in Wave 1 after the TLS sequence completes.
