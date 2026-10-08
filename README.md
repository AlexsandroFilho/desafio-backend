# Wager Transaction Service

Serviço financeiro de iGaming em Go para processar `BET`, `WIN`, `LOSS`,
`REFUND` e `ROLLBACK` com consistência transacional, idempotência, isolamento
por provider e integração assíncrona via SQS.

## Arquitetura

```mermaid
flowchart LR
    Client[Provider HTTP] --> API[HTTP API]
    Command[SQS command FIFO] --> Consumer[SQS Consumer]
    API --> UC[Use cases]
    Consumer --> UC
    UC --> DB[(PostgreSQL)]
    DB --> Outbox[(Outbox)]
    Pending[Pending-reference worker] --> UC
    OutboxWorker[Outbox worker] --> Outbox
    OutboxWorker --> Event[SQS event FIFO]
    Prometheus[Prometheus] --> Metrics[GET /metrics]
```

O código é separado em:

- `internal/domain`: entidades e regras financeiras sem dependências externas.
- `internal/usecase`: orquestração transacional e idempotência.
- `internal/repository`: persistência PostgreSQL com `pgx`.
- `internal/http`: contratos HTTP, autenticação e handlers.
- `internal/worker`: consumidores SQS, outbox e referências pendentes.
- `internal/event`: envelopes e payloads de eventos.
- `internal/observability`: métricas Prometheus e logs HTTP.
- `internal/app`: configuração, injeção de dependências com Fx e lifecycle.

## Garantias financeiras

- Valores monetários são armazenados em centavos (`BIGINT`), sem ponto
  flutuante.
- A carteira é bloqueada com `SELECT ... FOR UPDATE` durante mutações.
- Saldo, transação, ledger, inbox e outbox são gravados na mesma transação SQL.
- O ledger registra saldo anterior e posterior e possui restrições contábeis.
- External ID e chave de idempotência são protegidos por locks consultivos
  determinísticos e constraints no banco.
- Replays retornam o snapshot financeiro original, mesmo após novas operações.
- Apenas uma reversão processada é permitida para cada transação original.
- Reversões recebidas fora de ordem ficam em `PENDING_REFERENCE` e são
  resolvidas por worker com `FOR UPDATE SKIP LOCKED`.
- Carteiras e transações são isoladas pelo `providerId` autenticado.
- Eventos são publicados pelo padrão transactional outbox, com retry
  exponencial e deduplicação FIFO.

## Requisitos

- Go 1.27 ou compatível com o `go.mod`
- Docker e Docker Compose

## Execução local

Copie a configuração de exemplo:

```bash
cp .env.example .env
```

No PowerShell:

```powershell
Copy-Item .env.example .env
```

Suba PostgreSQL, migrations, LocalStack/SQS e Keycloak:

```bash
docker compose up -d
docker compose ps
```

O container `migrate` e o `sqs-init` terminam após concluírem sua tarefa. Isso é
esperado. Depois, execute a aplicação:

```bash
go run ./cmd/api
```

Serviços locais:

- API: `http://localhost:8081`
- Keycloak: `http://localhost:8080`
- LocalStack: `http://localhost:4566`
- PostgreSQL: `localhost:5432`

## Autenticação

Obtenha um token do provider de demonstração:

```bash
curl -X POST \
  http://localhost:8080/realms/wager/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=demo-provider" \
  -d "client_secret=demo-provider-secret"
```

Use o `access_token` como `Authorization: Bearer <token>`. O token contém
`providerId=demo-provider`; recursos de outro provider são respondidos como não
encontrados.

## Endpoints

Endpoints públicos:

- `GET /health/live`
- `GET /health/ready`
- `GET /metrics`

Endpoints autenticados:

- `POST /wallets`
- `GET /wallets/{walletId}`
- `GET /wallets/{walletId}/ledger?limit=50&cursor=...`
- `POST /wallets/{walletId}/reconciliation`
- `POST /wagering/transactions`
- `GET /wagering/transactions/{transactionId}`

### Criar carteira

```bash
curl -X POST http://localhost:8081/wallets \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "playerId": "player-123",
    "initialBalance": {
      "amount": "100.00",
      "currency": "BRL"
    }
  }'
```

O ID da carteira é gerado pelo servidor.

### Processar transação

```bash
curl -X POST http://localhost:8081/wagering/transactions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: bet-001" \
  -H "X-Correlation-ID: round-001" \
  -H "Content-Type: application/json" \
  -d '{
    "providerId": "demo-provider",
    "externalTransactionId": "bet-001",
    "walletId": "<wallet-id>",
    "playerId": "player-123",
    "roundId": "round-001",
    "gameId": "slot-001",
    "kind": "BET",
    "money": {
      "amount": "10.00",
      "currency": "BRL"
    }
  }'
```

Para `REFUND` ou `ROLLBACK`, envie também:

```json
{
  "referenceExternalTransactionId": "bet-001"
}
```

`LOSS` representa apenas o encerramento sem prêmio e exige valor `0.00`.

## Mensageria

Comandos entram em `wager-transactions.fifo`. Mensagens inválidas ou falhas
transientes permanecem disponíveis e seguem a redrive policy para
`wager-transactions-dlq.fifo`.

Envelope aceito:

```json
{
  "messageId": "command-001",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-10-08T15:00:00Z",
  "data": {
    "providerId": "demo-provider",
    "externalTransactionId": "bet-001",
    "idempotencyKey": "bet-001",
    "walletId": "<wallet-id>",
    "playerId": "player-123",
    "roundId": "round-001",
    "gameId": "slot-001",
    "kind": "BET",
    "money": {
      "amount": "10.00",
      "currency": "BRL"
    }
  }
}
```

Eventos são gravados na outbox e publicados exclusivamente em
`wager-events.fifo`. O envelope contém `eventId`, `eventType`, `aggregateId`,
`correlationId`, `causationId`, `occurredAt`, `version` e `data`.

## Observabilidade

Logs são emitidos em JSON no `stdout`. `LOG_LEVEL` aceita `DEBUG`, `INFO`,
`WARN` ou `ERROR`. Cada requisição recebe `X-Request-ID`; se
`X-Correlation-ID` for enviado, ele é reutilizado.

O endpoint `GET /metrics` expõe métricas Prometheus, incluindo:

- requisições e latência HTTP;
- processamento e latência de transações por tipo e status;
- mensagens SQS processadas, inválidas ou com falha;
- erros de recebimento da fila;
- eventos publicados ou reagendados pela outbox;
- quantidade atual de eventos pendentes;
- ciclos e resoluções de referências pendentes;
- métricas padrão do runtime Go e do processo.

Exemplo:

```bash
curl http://localhost:8081/metrics
```

## Testes

Testes unitários:

```bash
go test ./...
go vet ./...
```

Os testes de integração usam `TEST_DATABASE_URL`. Com o PostgreSQL local:

```bash
TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/wagerdb?sslmode=disable" \
  go test ./... -count=1
```

No PowerShell:

```powershell
$env:TEST_DATABASE_URL = "postgres://postgres:postgres@localhost:5432/wagerdb?sslmode=disable"
go test ./... -count=1
```

A suíte cobre concorrência na mesma carteira, replay idempotente, corridas de
reversão, referências fora de ordem e ausência de divergência entre saldo e
ledger.

## Variáveis de ambiente

- `HTTP_ADDR`: endereço HTTP, padrão `:8081`.
- `DATABASE_URL`: conexão PostgreSQL.
- `AWS_REGION`: região AWS/LocalStack.
- `AWS_ENDPOINT_URL`: endpoint do SQS.
- `SQS_COMMAND_QUEUE_URL`: fila FIFO de comandos.
- `SQS_EVENT_QUEUE_URL`: fila FIFO de eventos.
- `KEYCLOAK_ISSUER_URL`: issuer OIDC.
- `KEYCLOAK_AUDIENCE`: audience obrigatória do token.
- `LOG_LEVEL`: nível mínimo dos logs JSON.

Consulte `.env.example` para os valores locais.
