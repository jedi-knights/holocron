# Authorization

This document covers Holocron's per-action authorization surface — how the broker decides **what** an authenticated identity may do. Authentication (who they are) is the [auth doc](auth.md); authorization sits on top: every authenticated principal still has to clear a per-op policy check before producing, consuming, or managing topics.

For TLS (encryption + cert verification), see [`tls.md`](tls.md). The three layers compose:

| Layer | Question | Configured by | Lives in |
|---|---|---|---|
| TLS | "Is this connection encrypted, and is the peer's cert trusted?" | `--tls-*` flags | `docs/tls.md` |
| Auth | "Who is this caller?" | `--auth-issuer-key` + JWT scopes claim | `docs/auth.md` |
| **Authz** | **"May this caller perform this action on this resource?"** | **JWT `holocron.scopes` claim** | **this doc** |

Production deployments want all three. They're independent — running with auth + authz but no TLS, or TLS + auth but no authz, are valid (if unusual) shapes.

## Policy source

Holocron's authorization is **claims-based**: the JWT a client presents at handshake carries a `holocron.scopes` claim listing the actions that token holder may perform. The broker enforces those claims at every per-op handler. There is no separate broker-side ACL config to maintain — the JWT *is* the policy document, signed by the operator at issue time.

Why? Two reasons:

1. **Single source of truth.** Credentials and policy travel together. Issuing a token with the right scopes is the only step needed to grant access; revoking is a question of letting the token expire or adding the subject to the [denylist](auth.md#denylist-revocation). There's no broker-side config to drift out of sync with the credential store.
2. **Operationally portable.** A multi-broker deployment doesn't need a shared policy store; each broker independently verifies the same JWT against the same operator key.

## Scope grammar

A scope is a string of the form `verb:resource[*]`:

| Form | Example | Meaning |
|---|---|---|
| `verb:literal` | `produce:events` | Permits `verb` on exactly the resource `literal`. |
| `verb:prefix.*` | `produce:orders.*` | Permits `verb` on any resource whose name starts with `prefix.`. |
| `verb:*` | `produce:*` | Permits `verb` on any resource (including the empty resource). |
| `verb` | `admin` | Bare verb — permits `verb` on the **empty** resource only. Used for cluster-level ops with no topic. |

Strict at parse time:

- No regex, no mid-string globs (`produce:foo*bar` is rejected).
- Only one colon (`produce:foo:bar` is rejected).
- No internal whitespace.
- The verb itself can't be wildcarded (`*:events` is rejected).
- A trailing colon (`produce:`) is rejected — use the bare verb form (`produce`) instead.

A malformed scope in a JWT is **silently skipped** during evaluation — it never grants, but it also doesn't crash the verifier. A principal whose only scopes are malformed is indistinguishable from one with empty scopes: denied.

## Verbs

Three verbs cover the v1 surface:

| Verb | Covers | Resource shape |
|---|---|---|
| `produce` | `Produce`, `ProduceBatch` | topic name |
| `consume` | `Fetch` (the read-side handler that gates `Subscribe`, `Poll`, `Commit`, `JoinGroup` indirectly) | topic name |
| `admin` | `CreateTopic`, `DeleteTopic`, `UpdateTopicConfig` (per-topic); `AddVoter`, `RemoveVoter` (cluster) | topic name OR empty for cluster ops |

There is **no hierarchy**: `admin` does not silently include `produce` or `consume`. Each verb is granted independently. This is a deliberate choice — operators expect the union of granted scopes to be exactly what they wrote.

## Default policy

When the broker is configured with `--auth-issuer-key` (auth-required mode), authorization is **deny by default**:

- A JWT with empty `holocron.scopes` admits at handshake but every produce/consume/admin call returns `StatusForbidden`.
- A JWT scoped to `produce:events` cannot consume from `events`, cannot produce to `orders`, and cannot create or delete any topic.
- An anonymous principal (which only the `AnonymousVerifier` produces, and only when no auth is configured) is **rejected** the moment it reaches an authorizer call site. The deny is unconditional — a configured authorizer never silently grants.

When the broker has **no** auth configured (no `--auth-issuer-key`), the `AllowAllAuthorizer` is in play and every action is admitted regardless of credentials. This preserves the no-auth deployment shape exactly as it behaved pre-auth-wave.

## Worked examples

### A producer service

```bash
holocronctl auth issue \
  --key /etc/holocron/issuer-key.pem \
  --subject orders-producer-01 \
  --scope produce:orders.placed \
  --scope produce:orders.cancelled \
  --ttl 24h \
  --output orders-producer.jwt
```

Token holder may produce to `orders.placed` and `orders.cancelled` — nothing else. Cannot consume, cannot create topics, cannot drop them.

### A consumer / aggregator

```bash
holocronctl auth issue \
  --key /etc/holocron/issuer-key.pem \
  --subject billing-aggregator \
  --scope consume:orders.* \
  --scope produce:billing.invoices \
  --ttl 24h \
  --output billing.jwt
```

Reads any `orders.<anything>` topic; produces to one specific billing topic. The prefix wildcard (`orders.*`) covers `orders.placed`, `orders.cancelled`, `orders.refunded` — anything the orders namespace grows.

### An operator / SRE

```bash
holocronctl auth issue \
  --key /etc/holocron/issuer-key.pem \
  --subject ops-alice \
  --scope admin:* \
  --ttl 8h \
  --output ops.jwt
```

`admin:*` matches per-topic admin (`CreateTopic`, `DeleteTopic`, `UpdateTopicConfig` on any topic) AND the empty resource (`AddVoter`, `RemoveVoter`). Short TTL (8h) is appropriate for human credentials.

### A topic-bootstrap helper

```bash
holocronctl auth issue \
  --key /etc/holocron/issuer-key.pem \
  --subject topic-bootstrap \
  --scope admin:events \
  --scope admin:orders.placed \
  --ttl 1h \
  --output bootstrap.jwt
```

Narrowly scoped to two topics; cannot affect anything else. The right shape for an automation-only credential.

### Convenience: full access

```bash
holocronctl auth issue \
  --key /etc/holocron/issuer-key.pem \
  --subject dev-laptop \
  --all-access \
  --ttl 4h \
  --output dev.jwt
```

`--all-access` expands to `--scope produce:* --scope consume:* --scope admin:*`. Equivalent to a Kafka super-user. Use sparingly — the right shape for a developer's local laptop or a one-off script, not for production services.

## Operational implications

- **Services should not auto-create their own topics.** A produce-scoped service token should not also carry `admin:<topic>`. Provision topics out-of-band (via `holocronctl topic create` from an operator credential) and grant services only the produce/consume scopes they need.
- **A leaked token is a policy compromise, not a credential compromise.** Treat the JWT contents as the security boundary. Short TTLs and the [denylist](auth.md#denylist-revocation) are the two mitigations.
- **No scope inheritance across accounts.** The `holocron.account` claim is carried but inert in v1 (multi-tenancy is the next Wave-1 item). Scopes today are global to the broker; per-account scope namespacing lands with multi-tenancy.

## Common error shapes

| Wire status | What it means | Fix |
|---|---|---|
| `StatusForbidden` on produce | Subject's JWT has no `produce:<topic>` scope matching the topic | Issue a new JWT with the right scope |
| `StatusForbidden` on fetch | Subject's JWT has no `consume:<topic>` scope matching the topic | Issue a new JWT with the right scope |
| `StatusForbidden` on `topic create` | Subject's JWT has no `admin:<topic>` (or `admin:*`) scope | Use an operator credential, or add `admin:<topic>` to the service token |
| `StatusForbidden` on `cluster join` | Subject's JWT has no bare `admin` (or `admin:*`) scope | Use an operator credential carrying `admin` |
| `StatusUnauthorized` (not `Forbidden`) | Authentication failed — token expired, denylisted, or signed by an untrusted key | Renew or reissue the JWT |

## Limitations and roadmap

- **Account claim is inert.** `holocron.account` is parsed and threaded through the audit log but does not gate any operation in v1. Multi-tenancy (per-account quotas + topic visibility) is the next Wave-1 item.
- **No group-scoped consume.** `consume:<topic>` covers the topic regardless of consumer group. Per-group ACLs (e.g., `consume:group/<group-id>`) are reserved for a follow-on if the operational need surfaces.
- **No revoke-without-restart for individual scopes.** Scopes are baked into the JWT; revoking a single scope means rotating the token. Subject-level revocation is the [denylist](auth.md#denylist-revocation).
- **Read-only operations stay open.** `ListTopics`, `ListGroups`, `ClusterStatus`, and similar inspection RPCs aren't gated by ACL today. They reveal cluster topology but don't change state. If your threat model requires gating reads too, the Authorizer interface accommodates additional verbs.
