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

## Cluster (Raft) TLS

Multi-node clusters add a second wire surface: the Raft transport that nodes use to replicate writes and elect leaders. Cluster TLS is **symmetric mTLS** — every node is both a server (accepting peer connections) and a client (dialing peers), so cert, key, and CA are mandatory together. Half-encrypted Raft is not a supported state.

### Trust model

Every node in the cluster must trust the same CA. The simplest production layout is a single internal CA that signs one cert per node, with the node's host or Raft bind address in the cert's SAN. The same CA bundle plays both roles: it verifies inbound peer certs (`ClientCAs`) and outbound peer certs (`RootCAs`).

### Enabling cluster TLS

```bash
holocrond \
  --data-dir /var/lib/holocron \
  --listen 0.0.0.0:9092 \
  --cluster --node-id n1 \
  --raft-listen 0.0.0.0:9192 \
  --peers 'n1=10.0.0.1:9192=10.0.0.1:9092,n2=10.0.0.2:9192=10.0.0.2:9092,n3=10.0.0.3:9192=10.0.0.3:9092' \
  --bootstrap \
  --cluster-tls-cert /etc/holocron/raft-n1.pem \
  --cluster-tls-key /etc/holocron/raft-n1-key.pem \
  --cluster-tls-ca /etc/holocron/raft-ca.pem
```

The startup banner reports the Raft transport scheme alongside the wire scheme:

```
listening on 0.0.0.0:9092 (wire v1, TLS 1.3)
cluster: node n1, raft 0.0.0.0:9192 [TLS 1.3 (mTLS required)], peers=3, bootstrap=true
```

Client and cluster TLS are independent — you can run plaintext clients with TLS-only Raft, or vice versa. In production you almost always want both.

### Server-name override

The default verification expects the peer cert's SAN to include the address being dialed (the peer's `--raft-listen` or its `AdvertiseAddr`). If your CA issues certs with a different identity (e.g., a fixed cluster-wide CN like `holocron-raft`), supply `--cluster-tls-server-name holocron-raft` so verification uses that name instead. For most deployments — where each node's cert lists its actual hostname or IP in the SAN — this flag should stay unset.

## Configuration reference

### Wire (client-facing) TLS

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--tls-cert` | `HOLOCRON_TLS_CERT` | — | PEM cert chain. Presence enables TLS. |
| `--tls-key` | `HOLOCRON_TLS_KEY` | — | PEM private key matching `--tls-cert`. |
| `--tls-client-ca` | `HOLOCRON_TLS_CLIENT_CA` | — | PEM CA bundle for client-cert verification. |
| `--tls-require-client-cert` | `HOLOCRON_TLS_REQUIRE_CLIENT_CERT` | `false` | Reject clients without a verified cert. Requires `--tls-client-ca`. |
| `--tls-min-version` | `HOLOCRON_TLS_MIN_VERSION` | `1.3` | Minimum TLS version: `1.2` or `1.3`. |

### Cluster (Raft) TLS

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--cluster-tls-cert` | `HOLOCRON_CLUSTER_TLS_CERT` | — | PEM cert chain for the Raft transport. Presence enables cluster TLS — mTLS mandatory. |
| `--cluster-tls-key` | `HOLOCRON_CLUSTER_TLS_KEY` | — | PEM private key matching `--cluster-tls-cert`. |
| `--cluster-tls-ca` | `HOLOCRON_CLUSTER_TLS_CA` | — | PEM CA bundle that signs every peer's cert. Required when cert + key are supplied. |
| `--cluster-tls-server-name` | `HOLOCRON_CLUSTER_TLS_SERVER_NAME` | — | Expected SAN on peer certs when dialing. Only needed when peer certs do not carry their bind addresses. |

## Limitations and roadmap

- **Cert rotation requires a restart.** Cert material is read once at startup. Hot-reload on `SIGHUP` is a planned follow-on after PR 6.
- **`holocronctl` does not yet accept TLS flags.** PR 6 closes this — until then, operator commands against a TLS broker need a wrapper that supplies a TLS-aware transport.
- **No client cert → identity mapping yet.** Required mTLS confirms the client holds a cert signed by the configured CA but does not yet derive an authenticated principal from it. JWT/account-based auth is the next item in Wave 1 after the TLS sequence completes.
