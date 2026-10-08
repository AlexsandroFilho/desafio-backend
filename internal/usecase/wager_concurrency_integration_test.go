package usecase

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/repository"
	"github.com/jackc/pgx/v5/pgxpool"
)

const concurrencyInitialBalance int64 = 100_000

type concurrencyFixture struct {
	pool       *pgxpool.Pool
	useCases   []*WagerUseCase
	providerID string
	walletID   string
	playerID   string
	roundID    string
}

type concurrentResult struct {
	response *WagerResponse
	err      error
}

func TestConcurrentWalletMutationsKeepBalanceAndLedgerConsistentIntegration(
	t *testing.T,
) {
	fixture := newConcurrencyFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const operationsPerKind = 24
	amount := mustMoney(t, "1.00")
	requests := make([]WagerRequest, 0, operationsPerKind*2)
	for index := 0; index < operationsPerKind; index++ {
		requests = append(
			requests,
			fixture.request("bet", index, domain.TransactionKindBet, amount, ""),
			fixture.request("win", index, domain.TransactionKindWin, amount, ""),
		)
	}

	results := fixture.processConcurrently(ctx, requests)
	for index, result := range results {
		if result.err != nil {
			t.Fatalf("concurrent request %d error = %v", index, result.err)
		}
		if result.response.Status != domain.TransactionStatusProcessed {
			t.Fatalf(
				"concurrent request %d status = %s, want PROCESSED",
				index,
				result.response.Status,
			)
		}
	}

	fixture.assertAccountingIntegrity(
		t,
		ctx,
		concurrencyInitialBalance,
		1+operationsPerKind*2,
		operationsPerKind*2,
	)
	fixture.assertTransactionCounts(
		t,
		ctx,
		operationsPerKind*2,
		operationsPerKind*2,
		0,
		0,
	)
}

func TestConcurrentIdempotentReplayCreatesSingleFinancialMutationIntegration(
	t *testing.T,
) {
	fixture := newConcurrencyFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	const attempts = 24
	request := fixture.request(
		"same-bet",
		0,
		domain.TransactionKindBet,
		mustMoney(t, "10.00"),
		"",
	)
	requests := make([]WagerRequest, attempts)
	for index := range requests {
		requests[index] = request
	}

	results := fixture.processConcurrently(ctx, requests)
	transactionID := ""
	originalResponses := 0
	for index, result := range results {
		if result.err != nil {
			t.Fatalf("idempotent attempt %d error = %v", index, result.err)
		}
		if transactionID == "" {
			transactionID = result.response.TransactionID
		}
		if result.response.TransactionID != transactionID {
			t.Errorf(
				"attempt %d transaction ID = %s, want %s",
				index,
				result.response.TransactionID,
				transactionID,
			)
		}
		if !result.response.IdempotentReplay {
			originalResponses++
		}
	}
	if originalResponses != 1 {
		t.Errorf("non-replay responses = %d, want 1", originalResponses)
	}

	fixture.assertAccountingIntegrity(t, ctx, 99_000, 2, 1)
	fixture.assertTransactionCounts(t, ctx, 1, 1, 0, 0)
}

func TestConcurrentReversalsProcessExactlyOnceIntegration(t *testing.T) {
	fixture := newConcurrencyFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	amount := mustMoney(t, "10.00")
	original := fixture.request(
		"original-bet",
		0,
		domain.TransactionKindBet,
		amount,
		"",
	)
	originalResponse, err := fixture.useCases[0].ProcessTransaction(ctx, original)
	if err != nil {
		t.Fatalf("create original transaction: %v", err)
	}
	if originalResponse.Status != domain.TransactionStatusProcessed {
		t.Fatalf("original status = %s, want PROCESSED", originalResponse.Status)
	}

	const attempts = 20
	requests := make([]WagerRequest, 0, attempts)
	for index := 0; index < attempts; index++ {
		kind := domain.TransactionKindRefund
		if index%2 == 1 {
			kind = domain.TransactionKindRollback
		}
		requests = append(
			requests,
			fixture.request(
				"reversal",
				index,
				kind,
				amount,
				original.ExternalTransactionID,
			),
		)
	}

	results := fixture.processConcurrently(ctx, requests)
	processed := 0
	rejected := 0
	for index, result := range results {
		if result.err != nil {
			t.Fatalf("reversal attempt %d error = %v", index, result.err)
		}
		switch result.response.Status {
		case domain.TransactionStatusProcessed:
			processed++
		case domain.TransactionStatusRejected:
			rejected++
			if result.response.FailureCode != "REFERENCE_ALREADY_REVERSED" {
				t.Errorf(
					"reversal attempt %d failure code = %q, want REFERENCE_ALREADY_REVERSED",
					index,
					result.response.FailureCode,
				)
			}
		default:
			t.Errorf(
				"reversal attempt %d status = %s, want PROCESSED or REJECTED",
				index,
				result.response.Status,
			)
		}
	}
	if processed != 1 || rejected != attempts-1 {
		t.Errorf(
			"reversal outcomes = %d processed, %d rejected; want 1 and %d",
			processed,
			rejected,
			attempts-1,
		)
	}

	fixture.assertAccountingIntegrity(t, ctx, concurrencyInitialBalance, 3, 2)
	fixture.assertTransactionCounts(t, ctx, attempts+1, 2, attempts-1, 0)

	var processedReversals int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE provider_id = $1
		  AND reference_transaction_id = $2
		  AND status = 'PROCESSED'
	`, fixture.providerID, originalResponse.TransactionID).Scan(
		&processedReversals,
	); err != nil {
		t.Fatalf("count processed reversals: %v", err)
	}
	if processedReversals != 1 {
		t.Errorf("processed reversals in database = %d, want 1", processedReversals)
	}
}

func TestConcurrentPendingReferenceWorkersResolveOutOfOrderLoadIntegration(
	t *testing.T,
) {
	fixture := newConcurrencyFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const pairs = 16
	amount := mustMoney(t, "1.00")
	originals := make([]WagerRequest, 0, pairs)
	for index := 0; index < pairs; index++ {
		original := fixture.request(
			"late-bet",
			index,
			domain.TransactionKindBet,
			amount,
			"",
		)
		originals = append(originals, original)

		pending := fixture.request(
			"early-refund",
			index,
			domain.TransactionKindRefund,
			amount,
			original.ExternalTransactionID,
		)
		response, err := fixture.useCases[index%len(fixture.useCases)].
			ProcessTransaction(ctx, pending)
		if err != nil {
			t.Fatalf("create pending reference %d: %v", index, err)
		}
		if response.Status != domain.TransactionStatusPendingReference {
			t.Fatalf(
				"pending reference %d status = %s, want PENDING_REFERENCE",
				index,
				response.Status,
			)
		}
	}

	for index, result := range fixture.processConcurrently(ctx, originals) {
		if result.err != nil {
			t.Fatalf("create late original %d: %v", index, result.err)
		}
		if result.response.Status != domain.TransactionStatusProcessed {
			t.Fatalf(
				"late original %d status = %s, want PROCESSED",
				index,
				result.response.Status,
			)
		}
	}

	workerCounts := make(chan int, len(fixture.useCases))
	workerErrors := make(chan error, len(fixture.useCases))
	var workers sync.WaitGroup
	start := make(chan struct{})
	for _, wagerUseCase := range fixture.useCases {
		workers.Add(1)
		go func(uc *WagerUseCase) {
			defer workers.Done()
			<-start
			processed, err := uc.ProcessPendingReferences(ctx, pairs)
			if err != nil {
				workerErrors <- err
				return
			}
			workerCounts <- processed
		}(wagerUseCase)
	}
	close(start)
	workers.Wait()
	close(workerErrors)
	close(workerCounts)

	for err := range workerErrors {
		t.Errorf("pending reference worker error = %v", err)
	}
	totalProcessed := 0
	for count := range workerCounts {
		totalProcessed += count
	}
	if totalProcessed != pairs {
		t.Errorf("pending references processed = %d, want %d", totalProcessed, pairs)
	}

	fixture.assertAccountingIntegrity(
		t,
		ctx,
		concurrencyInitialBalance,
		1+pairs*2,
		pairs*2,
	)
	fixture.assertTransactionCounts(t, ctx, pairs*2, pairs*2, 0, 0)
}

func newConcurrencyFixture(t *testing.T) *concurrencyFixture {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(connectCtx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		t.Fatalf("ping integration database: %v", err)
	}

	suffix, err := newUUID()
	if err != nil {
		pool.Close()
		t.Fatalf("newUUID() error = %v", err)
	}
	walletID, err := newUUID()
	if err != nil {
		pool.Close()
		t.Fatalf("newUUID() error = %v", err)
	}
	fixture := &concurrencyFixture{
		pool:       pool,
		providerID: "concurrency-provider-" + suffix,
		walletID:   walletID,
		playerID:   "concurrency-player-" + suffix,
		roundID:    "concurrency-round-" + suffix,
	}
	if _, err := pool.Exec(connectCtx, `
		INSERT INTO wallets (id, provider_id, player_id, currency, balance)
		VALUES ($1, $2, $3, 'BRL', $4)
	`, fixture.walletID, fixture.providerID, fixture.playerID, concurrencyInitialBalance); err != nil {
		pool.Close()
		t.Fatalf("insert concurrency wallet fixture: %v", err)
	}

	for index := 0; index < 4; index++ {
		fixture.useCases = append(fixture.useCases, NewWagerUseCase(
			pool,
			repository.NewPostgresWalletRepository(),
			repository.NewPostgresTransactionRepository(),
			repository.NewPostgresLedgerRepository(),
			repository.NewPostgresInboxOutboxRepository(),
		))
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `
			DELETE FROM outbox
			WHERE aggregate_id = $1
			   OR aggregate_id IN (
					SELECT id
					FROM wager_transactions
					WHERE provider_id = $2
			   )
		`, fixture.walletID, fixture.providerID)
		_, _ = pool.Exec(cleanupCtx, `
			DELETE FROM wallet_ledger_entries
			WHERE wallet_id = $1
		`, fixture.walletID)
		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM wager_idempotency_keys WHERE provider_id = $1`,
			fixture.providerID,
		)
		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM wager_transactions WHERE provider_id = $1`,
			fixture.providerID,
		)
		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM wallets WHERE provider_id = $1 AND id = $2`,
			fixture.providerID,
			fixture.walletID,
		)
		pool.Close()
	})

	return fixture
}

func (fixture *concurrencyFixture) request(
	prefix string,
	index int,
	kind domain.TransactionKind,
	amount domain.Money,
	referenceExternalID string,
) WagerRequest {
	identifier := fmt.Sprintf("%s-%02d-%s", prefix, index, fixture.walletID)
	return WagerRequest{
		ProviderID:                     fixture.providerID,
		ExternalTransactionID:          identifier,
		IdempotencyKey:                 "key-" + identifier,
		WalletID:                       fixture.walletID,
		PlayerID:                       fixture.playerID,
		RoundID:                        fixture.roundID,
		GameID:                         "concurrency-game",
		Kind:                           kind,
		Amount:                         amount,
		ReferenceExternalTransactionID: referenceExternalID,
	}
}

func (fixture *concurrencyFixture) processConcurrently(
	ctx context.Context,
	requests []WagerRequest,
) []concurrentResult {
	results := make([]concurrentResult, len(requests))
	start := make(chan struct{})
	var group sync.WaitGroup
	for index, request := range requests {
		group.Add(1)
		go func(index int, request WagerRequest) {
			defer group.Done()
			<-start
			results[index].response, results[index].err =
				fixture.useCases[index%len(fixture.useCases)].
					ProcessTransaction(ctx, request)
		}(index, request)
	}
	close(start)
	group.Wait()
	return results
}

func (fixture *concurrencyFixture) assertAccountingIntegrity(
	t *testing.T,
	ctx context.Context,
	expectedBalance int64,
	expectedVersion int,
	expectedLedgerEntries int,
) {
	t.Helper()

	var (
		balance int64
		version int
	)
	if err := fixture.pool.QueryRow(ctx, `
		SELECT balance, version
		FROM wallets
		WHERE provider_id = $1 AND id = $2
	`, fixture.providerID, fixture.walletID).Scan(&balance, &version); err != nil {
		t.Fatalf("query wallet accounting state: %v", err)
	}
	if balance != expectedBalance || version != expectedVersion {
		t.Errorf(
			"wallet state = balance %d, version %d; want balance %d, version %d",
			balance,
			version,
			expectedBalance,
			expectedVersion,
		)
	}

	rows, err := fixture.pool.Query(ctx, `
		SELECT direction, amount, balance_before, balance_after
		FROM wallet_ledger_entries
		WHERE wallet_id = $1
		ORDER BY created_at, id
	`, fixture.walletID)
	if err != nil {
		t.Fatalf("query ledger integrity: %v", err)
	}
	defer rows.Close()

	ledgerCount := 0
	runningBalance := concurrencyInitialBalance
	for rows.Next() {
		var (
			direction             domain.LedgerDirection
			amount, before, after int64
		)
		if err := rows.Scan(&direction, &amount, &before, &after); err != nil {
			t.Fatalf("scan ledger integrity row: %v", err)
		}
		if before != runningBalance {
			t.Errorf(
				"ledger entry %d starts at %d, previous balance is %d",
				ledgerCount,
				before,
				runningBalance,
			)
		}
		switch direction {
		case domain.LedgerDirectionDebit:
			if after != before-amount {
				t.Errorf("ledger debit %d has inconsistent balances", ledgerCount)
			}
		case domain.LedgerDirectionCredit:
			if after != before+amount {
				t.Errorf("ledger credit %d has inconsistent balances", ledgerCount)
			}
		default:
			t.Errorf("ledger entry %d has invalid direction %q", ledgerCount, direction)
		}
		runningBalance = after
		ledgerCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ledger integrity rows: %v", err)
	}
	if ledgerCount != expectedLedgerEntries {
		t.Errorf("ledger entries = %d, want %d", ledgerCount, expectedLedgerEntries)
	}
	if runningBalance != balance {
		t.Errorf(
			"ledger final balance = %d, wallet balance = %d",
			runningBalance,
			balance,
		)
	}
}

func (fixture *concurrencyFixture) assertTransactionCounts(
	t *testing.T,
	ctx context.Context,
	expectedTotal int,
	expectedProcessed int,
	expectedRejected int,
	expectedPending int,
) {
	t.Helper()

	var total, processed, rejected, pending int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'PROCESSED'),
			COUNT(*) FILTER (WHERE status = 'REJECTED'),
			COUNT(*) FILTER (WHERE status = 'PENDING_REFERENCE')
		FROM wager_transactions
		WHERE provider_id = $1
	`, fixture.providerID).Scan(
		&total,
		&processed,
		&rejected,
		&pending,
	); err != nil {
		t.Fatalf("query transaction counts: %v", err)
	}
	if total != expectedTotal ||
		processed != expectedProcessed ||
		rejected != expectedRejected ||
		pending != expectedPending {
		t.Errorf(
			"transaction counts = total %d, processed %d, rejected %d, pending %d; "+
				"want %d, %d, %d, %d",
			total,
			processed,
			rejected,
			pending,
			expectedTotal,
			expectedProcessed,
			expectedRejected,
			expectedPending,
		)
	}
}
