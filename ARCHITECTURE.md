# Architecture

This document records the technical decisions behind the Wager Transaction
Service. The goal is not completeness of every file, but the invariants that
keep financial results correct under concurrency, retries, and process
restarts.

HTTP and SQS share the same use case. The transport changes; the transaction
boundary does not.

## Layers

```text
cmd/api
  └─ Uber Fx application
       ├─ HTTP handlers + OIDC middleware
       ├─ SQS command consumer
       ├─ Outbox worker
       └─ Pending-reference worker
            └─ usecase
                 └─ domain  (Money, Wallet, WagerTransaction, ledger)
                 └─ repository (PostgreSQL / pgx)
                 └─ event envelopes
```

- `internal/domain` has no HTTP, SQL, SQS, or Fx imports. Money and state
  transitions are enforced there.
- `internal/usecase` owns the SQL transaction, idempotency, wallet lock,
  ledger write, inbox/outbox, and pending-reference retry policy.
- `internal/repository` maps aggregates to PostgreSQL and applies
  `FOR UPDATE` / `SKIP LOCKED` where the use case needs them.
- `internal/http` and `internal/worker` adapt requests into `WagerRequest`
  and map domain errors to transport responses.
- `internal/app` wires configuration, pools, clients, and graceful shutdown.

Multiple API instances can run against the same database and queues. Correctness
comes from PostgreSQL locks and unique constraints, not from a single in-memory
coordinator.

## Money

`Money` stores an `int64` amount in cents and an ISO-4217 currency code.

Reasons:

- IEEE-754 floats cannot represent values such as `0.10` exactly.
- All arithmetic (`Add`, `Subtract`, `Negate`) stays on integers and rejects
  overflow before wrapping.
- Parsing requires a string with exactly two decimal places and rejects
  scientific notation (`e`/`E`).
- Negative balances are illegal on wallets. Ledger amounts are absolute and
  paired with `DEBIT` or `CREDIT`.

The PostgreSQL columns are `BIGINT`. The HTTP/SQS contract still uses decimal
strings (`"10.00"`). Conversion happens at the boundary, never inside the
ledger.

Persistence uses `pgx/v5` with explicit SQL. Repositories receive the
`pgx.Tx` opened by the use case; they do not start nested transactions.
`Money` maps to `(amount BIGINT, currency VARCHAR(3))`. Arithmetic never
uses `float32`/`float64`. Prometheus gauges use `float64` only for
non-financial counters.

`ZeroMoney(currency)` builds a zero value for a currency. `Equal` and
`Compare` reject mixed currencies instead of coercing them.

## Transaction boundary

A processed wager commits, in one PostgreSQL transaction:

1. Advisory locks for `(provider, externalTransactionId)` and
   `(provider, idempotencyKey)`.
2. Inbox row when the request arrived from SQS (`messageId` present).
3. Wallet row locked with `SELECT ... FOR UPDATE`.
4. Currency and player ownership checks.
5. Reference lock and reversal rules, when applicable.
6. Wallet balance and version.
7. `wager_transactions` row, including the historical snapshot
   (`result_balance`, `result_wallet_version`).
8. At most one ledger entry for that wallet/transaction.
9. Outbox events (`WagerTransactionProcessed` or
   `WagerTransactionRejected`, plus `WalletBalanceChanged` when the balance
   moved).

If any step fails, the transaction rolls back. HTTP then returns an error;
SQS leaves the message visible for retry or DLQ. There is no “apply balance
now, publish later” split inside the use case.

Wallet creation with a non-zero opening balance follows the same pattern
(`OPENING` transaction + ledger + outbox). A zero opening balance creates
only the wallet row: no synthetic `OPENING` movement.

Reconciliation reads wallet and ledger under `REPEATABLE READ` / read-only
so the stored balance and the summed ledger share one snapshot.

## Idempotency

Replay is a first-class path, not a cache.

Lookup is bidirectional:

- `(provider_id, external_transaction_id)`
- `(provider_id, idempotency_key)` via the alias table

The canonical payload hash is SHA-256 of the business fields only:

- `providerId`, `externalTransactionId`, `walletId`, `playerId`
- `roundId`, `gameId`, `kind`
- `amountCents`, `currency`
- `referenceExternalTransactionId`

HTTP and SQS hash through the same `hashWagerRequest` helper. The payload
is `encoding/json` over a dedicated struct whose field order is fixed;
amounts enter as `int64` cents so equivalent decimal strings hash the
same. That is the canonical form used by this service. RFC 8785 key
sorting is not applied because both channels share one Go type.

`Idempotency-Key`, `X-Correlation-ID`, `messageId`, and other transport
metadata are excluded. Two keys may therefore replay the same original
request. Reusing a key with a different payload, or pairing the same key
with a different external ID, is `IDEMPOTENCY_CONFLICT`.

On replay the use case returns the persisted snapshot
(`result_balance`, `result_wallet_version`), even if later operations
changed the live wallet. A compatibility hash that still includes the
legacy key is accepted so older rows remain replayable.

## Advisory locks

Unique constraints alone are not enough: two concurrent inserts can both
read “not found”, then one fails with `23505` after work has started.
Before the lookup, the use case takes deterministic transaction-scoped
locks:

```sql
SELECT pg_advisory_xact_lock(hashtextextended($1, 0));
```

The lock keys are length-prefixed (`len(providerID):providerID:external:...`
and `...:idempotency:...`) and sorted before acquisition to avoid
deadlocks. They are released automatically on commit or rollback.

Wallet mutations use row locks (`FOR UPDATE`). Outbox publishing and
pending-reference resolution use `FOR UPDATE SKIP LOCKED` so multiple
instances can share work without waiting on the same row.

## Pending-reference state machine

`REFUND` and `ROLLBACK` may arrive before the referenced `BET`, `WIN`, or
`REFUND`.

```text
PENDING
  ├─ PROCESSED
  ├─ REJECTED / FAILED
  └─ PENDING_REFERENCE
        ├─ PROCESSED          (reference found and valid)
        └─ REJECTED           (invalid, expired, or max attempts)
```

Rules once the reference exists:

- Same provider, player, wallet, currency, and round.
- Exact amount.
- Reference must be `PROCESSED`.
- `REFUND` may reverse only a `BET`.
- `ROLLBACK` may reverse a `BET`, `WIN`, or `REFUND`.
- At most one processed reversal per original transaction, of any kind
  (unique partial index plus application check). A `BET` therefore cannot
  receive both a processed `REFUND` and a processed `ROLLBACK`. Allowing
  both would credit the same debit twice. The second attempt is
  `REFERENCE_ALREADY_REVERSED`.
- A rollback that would overdraw is `ROLLBACK_INSUFFICIENT_BALANCE`, not a
  generic insufficient-balance bet error.
- If the reference exists but is still `PENDING` / `PENDING_REFERENCE`,
  or ended as `REJECTED` / `FAILED`, the reversal is `INVALID_REFERENCE`.
  Only a `PROCESSED` reference can be reversed. The worker does not wait
  for a non-successful reference to become valid.

WIN may include `referenceExternalTransactionId`; it is stored but does
not gate the credit. LOSS requires `"0.00"`, writes no ledger row, and
does not increment the wallet version.

While the reference is missing:

- The row stays `PENDING_REFERENCE`.
- A `WagerTransactionPendingReference` event is written to the outbox.
- The worker retries with exponential backoff from 1s to 1h.
- After 10 attempts or 24 hours the row becomes `REJECTED` with
  `REFERENCE_NOT_FOUND` and a final rejected outbox event.

`OPENING` is an internal wallet-creation kind. External HTTP/SQS requests
with `kind=OPENING` are rejected as unsupported.

## Messaging: inbox and outbox

Commands and events use different FIFO queues:

| Queue                         | Role                                      | Dedup                         |
|-------------------------------|-------------------------------------------|-------------------------------|
| `wager-transactions.fifo`     | inbound commands                          | content-based + inbox table   |
| `wager-transactions-dlq.fifo` | command poison / exhausted receives (5)   | content-based                 |
| `wager-events.fifo`           | outbound domain events                    | `MessageDeduplicationId=eventId` |

The consumer deletes an SQS message only after `ProcessTransaction`
commits. A crash between commit and delete causes redelivery. The inbox
primary key `(consumer_name, message_id)` and the wager idempotency path
make that redelivery a no-op financially.

The outbox worker polls every 500ms, claims a batch with
`SKIP LOCKED`, publishes to the event queue, and marks `PUBLISHED`.
Transient send failures increment `retry_count` and schedule
`next_retry_at` with exponential backoff capped at one minute. If the
process dies after SQS accepts the message and before the row is marked
published, the same `eventId` is used as the FIFO deduplication id, so
subscribers see one logical event.

HTTP never publishes directly to SQS. That keeps “wallet changed” and
“event exists” in the same commit.

Command messages are produced by the betting provider. LocalStack uses
the test endpoint and static `test/test` credentials. In AWS the queue
policy and IAM should restrict `SendMessage` on the command queue to the
broker and `ReceiveMessage`/`DeleteMessage` to this service. The consumer
still applies every domain rule; broker access is not a substitute for
idempotency or wallet locks.

FIFO attributes:

| Path | `MessageGroupId` | `MessageDeduplicationId` |
|------|------------------|--------------------------|
| Outbox → events | aggregate id (wallet or transaction) | outbox/`eventId` |
| Commands | chosen by the producer | content-based on the command queue |

Visibility timeout is 30 seconds. Long polling is 20 seconds. Receive
errors wait 1 second before the next poll. Invalid JSON is left in the
queue until the redrive policy (`maxReceiveCount=5`) moves it to the DLQ.
Terminal business rejections delete the message. Transient errors
(`503`, connection loss) do not delete it.

Permanent versus transient:

- Permanent: validation, currency mismatch, unsupported `OPENING`,
  idempotency conflict, inbox hash mismatch.
- Terminal in-band: `REJECTED` / `PENDING_REFERENCE` responses after a
  successful commit; the message is deleted.
- Transient: context deadline, PostgreSQL class `08`/`53`, SQS send/receive
  failures. These retry or wait for visibility to expire.

`FAILED` exists on the domain state machine for a permanent
infrastructure outcome that must stay auditable. The current runtime
paths either commit a business status or roll back and retry; they do
not persist `FAILED`.

## HTTP contract

| Situation | HTTP | `code` / body |
|-----------|------|----------------|
| Invalid JSON, money, kind, missing fields | 400 | `INVALID_REQUEST` / field codes |
| Missing or invalid bearer token | 401 | `UNAUTHORIZED` |
| Token `providerId` ≠ body/URL | 403 | `PROVIDER_MISMATCH` |
| Duplicate wallet or idempotency conflict | 409 | `WALLET_ALREADY_EXISTS` / `IDEMPOTENCY_CONFLICT` |
| Unknown wallet or transaction for this provider | 404 | `WALLET_NOT_FOUND` / `TRANSACTION_NOT_FOUND` |
| PostgreSQL unavailable | 503 | `SERVICE_UNAVAILABLE` |
| Processed, rejected, or pending wager | 200 | `status` + optional `failureCode` |
| Wallet created | 201 | wallet body |

Wager responses include both `id` and `transactionId` with the same
value. Business outcomes (`INSUFFICIENT_BALANCE`, `PENDING_REFERENCE`,
`REFERENCE_NOT_FOUND`, …) are not HTTP errors; they are committed
transactions.

Stable `failureCode` values: `INSUFFICIENT_BALANCE`,
`ROLLBACK_INSUFFICIENT_BALANCE`, `CURRENCY_MISMATCH`, `INVALID_AMOUNT`,
`LOSS_AMOUNT_MUST_BE_ZERO`, `AMOUNT_OVERFLOW`, `WALLET_VERSION_OVERFLOW`,
`WALLET_PLAYER_MISMATCH`, `UNSUPPORTED_TRANSACTION_KIND`,
`REFERENCE_REQUIRED`, `REFERENCE_NOT_FOUND`, `INVALID_REFERENCE`,
`REFERENCE_AMOUNT_MISMATCH`, `REFERENCE_ALREADY_REVERSED`,
`BUSINESS_RULE_VIOLATION`.

## Authentication and provider isolation

Protected routes require a Bearer JWT from the configured OIDC issuer
(Keycloak in local development). The verifier checks signature, issuer,
and audience (`wager-api`).

Keycloak is the local OIDC IdP because it is in the challenge stack, supports
`client_credentials`, and can mint a `providerId` claim without this
service storing passwords. The API does not issue tokens. The permission
model is tenant isolation: one authenticated `providerId` per request,
enforced in middleware, handlers, SQL, and foreign keys.

Wallet HTTP routes (`POST/GET /wallets`, ledger, reconciliation) are
authenticated as that provider. There is no public debit/credit API;
balance changes happen only inside the wager use case. That is the
interpretation of “wallet operations restricted to the internal
service”: providers cannot move money except by submitting a wager.

The token must carry `providerId` (or legacy `provider_id`). Conflicting
claims are rejected. That value is the only provider the request may use:

- URL `providerId`, body `providerId`, and token claim must match.
- Wallet uniqueness is `(provider_id, player_id, currency)`.
- Transactions reference wallets by the composite
  `(provider_id, wallet_id)`.
- Cross-provider reads look like “not found”, not “forbidden with data”.

`GET /health/*` and `GET /metrics` stay public so orchestrators and
Prometheus can scrape them without a provider token.

## Composition and shutdown

Uber Fx constructs the graph in `internal/app`: config, JSON logger,
Prometheus metrics, `pgxpool`, SQS clients, OIDC middleware, repositories,
use cases, HTTP server, and the three background workers.

On start the HTTP server listens, then the outbox worker, pending-reference
worker, and SQS consumer start on a shared runtime context.

On stop the runtime context is cancelled first. Workers finish the current
cycle (`WaitGroup`), the HTTP server shuts down with the Fx stop context,
and the pool is closed. In-flight SQL transactions either commit or roll
back with the connection; they are not left half-applied in application
memory.

## Ledger immutability

Domain construction already requires
`balance_after = balance_before ± amount`. PostgreSQL repeats that check
and allows only one ledger row per `(wallet_id, transaction_id)`.

A trigger rejects `UPDATE` and `DELETE` on `wallet_ledger_entries` unless
the session sets `app.allow_ledger_maintenance = 'on'` (test cleanup only).
Production code never sets that flag.

## Trade-offs

- **Pessimistic locks over optimistic version retries.** Wallet contention
  is expected on a hot player. Waiting on `FOR UPDATE` is simpler and
  safer than retrying lost updates in a financial path.
- **Transactional outbox over dual-write.** The API may commit a few
  hundred milliseconds before the event is visible. That delay is
  preferable to a committed debit without a corresponding event, or the
  reverse.
- **At-least-once messaging.** Exactly-once delivery is not assumed.
  Idempotency and FIFO deduplication ids make duplicates safe.
- **Pending references instead of immediate reject.** Out-of-order
  provider traffic is common. The cost is a retry worker and an
  expiration policy.
- **Integer cents instead of a decimal library.** The challenge is
  two-decimal fiat. `int64` keeps overflow explicit and avoids a heavier
  dependency.
- **Static AWS credentials in local wiring.** The SQS client is pointed at
  LocalStack. A production deployment should replace that provider with
  the default AWS credential chain and real queue URLs.

These choices optimize for auditability and multi-instance safety, not for
minimum latency on a single box.

## Limitations and accepted interpretations

- Wallet uniqueness is `(provider_id, player_id, currency)`, not a global
  `(player_id, currency)`. Two providers may hold a wallet for the same
  player id.
- `OPENING` rows reuse `wager_transactions` with an internal external id
  derived at creation time, rather than a separate internal-only table.
- Hash canonicalization is the Go JSON encoding of a fixed struct shared
  by HTTP and SQS, not RFC 8785.
- SQS recovery tests drive the consumer with in-process messages against
  real PostgreSQL. They do not boot LocalStack inside `go test`. Queue
  topology is exercised by Docker Compose.
- OIDC tests cover claim extraction and verifier configuration. They do
  not obtain a live Keycloak token inside the suite.
- There is no dedicated Fx `fx.New(...).Start/Stop` test. Lifecycle is
  implemented in `internal/app/lifecycle.go` and covered indirectly by
  worker start/stop tests.
- `FAILED` is modelled and not written by current workers.
- OpenTelemetry tracing and load tests are out of scope.
- `go test -race` needs `CGO_ENABLED=1` and `gcc` on `PATH`.
- Local SQS clients use static LocalStack credentials. Production must
  use the AWS default chain and queue policies.
