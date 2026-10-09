package usecase

import (
	"errors"
	"testing"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
)

func TestPendingReferenceBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Second},
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 6, want: 32 * time.Second},
		{attempt: 20, want: time.Hour},
	}
	for _, test := range tests {
		if got := pendingReferenceBackoff(test.attempt); got != test.want {
			t.Errorf(
				"pendingReferenceBackoff(%d) = %s, want %s",
				test.attempt,
				got,
				test.want,
			)
		}
	}
}

func TestApplyWalletOperationLossDoesNotChangeWallet(t *testing.T) {
	wallet := mustWallet(t, "100.00")
	initialVersion := wallet.Version()
	initialBalance := wallet.Balance()

	operation, err := applyWalletOperation(wallet, WagerRequest{
		PlayerID: "player-1",
		Kind:     domain.TransactionKindLoss,
		Amount:   mustMoney(t, "0.00"),
	}, nil)
	if err != nil {
		t.Fatalf("applyWalletOperation() unexpected error: %v", err)
	}
	if operation.balanceChanged {
		t.Error("LOSS must not report a balance change")
	}
	if wallet.Balance() != initialBalance {
		t.Errorf(
			"LOSS balance = %s, want %s",
			wallet.Balance().StringFormat(),
			initialBalance.StringFormat(),
		)
	}
	if wallet.Version() != initialVersion {
		t.Errorf("LOSS version = %d, want %d", wallet.Version(), initialVersion)
	}
}

func TestApplyWalletOperationRejectsNonZeroLoss(t *testing.T) {
	wallet := mustWallet(t, "100.00")
	initialVersion := wallet.Version()

	operation, err := applyWalletOperation(wallet, WagerRequest{
		PlayerID: "player-1",
		Kind:     domain.TransactionKindLoss,
		Amount:   mustMoney(t, "1.00"),
	}, nil)
	if !errors.Is(err, ErrLossAmountMustBeZero) {
		t.Fatalf("applyWalletOperation() error = %v, want %v", err, ErrLossAmountMustBeZero)
	}
	if operation.balanceChanged {
		t.Error("rejected LOSS must not report a balance change")
	}
	if wallet.Balance().AmountCents() != 10000 {
		t.Errorf("rejected LOSS balance = %d, want %d", wallet.Balance().AmountCents(), 10000)
	}
	if wallet.Version() != initialVersion {
		t.Errorf("rejected LOSS version = %d, want %d", wallet.Version(), initialVersion)
	}
}

func TestApplyWalletOperationRollbackReversesOriginalTransaction(t *testing.T) {
	tests := []struct {
		name             string
		referenceKind    domain.TransactionKind
		wantDirection    domain.LedgerDirection
		wantBalanceCents int64
	}{
		{
			name:             "rollback bet credits wallet",
			referenceKind:    domain.TransactionKindBet,
			wantDirection:    domain.LedgerDirectionCredit,
			wantBalanceCents: 12500,
		},
		{
			name:             "rollback win debits wallet",
			referenceKind:    domain.TransactionKindWin,
			wantDirection:    domain.LedgerDirectionDebit,
			wantBalanceCents: 7500,
		},
		{
			name:             "rollback refund debits wallet",
			referenceKind:    domain.TransactionKindRefund,
			wantDirection:    domain.LedgerDirectionDebit,
			wantBalanceCents: 7500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wallet := mustWallet(t, "100.00")
			amount := mustMoney(t, "25.00")
			reference := mustProcessedTransaction(t, tt.referenceKind, amount)

			operation, err := applyWalletOperation(wallet, WagerRequest{
				PlayerID: "player-1",
				Kind:     domain.TransactionKindRollback,
				Amount:   amount,
			}, reference)
			if err != nil {
				t.Fatalf("applyWalletOperation() unexpected error: %v", err)
			}
			if !operation.balanceChanged {
				t.Error("ROLLBACK must report a balance change")
			}
			if operation.direction != tt.wantDirection {
				t.Errorf("ROLLBACK direction = %s, want %s", operation.direction, tt.wantDirection)
			}
			if wallet.Balance().AmountCents() != tt.wantBalanceCents {
				t.Errorf(
					"ROLLBACK balance = %d, want %d",
					wallet.Balance().AmountCents(),
					tt.wantBalanceCents,
				)
			}
			if wallet.Version() != 2 {
				t.Errorf("ROLLBACK version = %d, want %d", wallet.Version(), 2)
			}
		})
	}
}

func TestApplyWalletOperationReportsRollbackInsufficientBalance(t *testing.T) {
	wallet := mustWallet(t, "10.00")
	amount := mustMoney(t, "25.00")
	reference := mustProcessedTransaction(t, domain.TransactionKindWin, amount)

	operation, err := applyWalletOperation(wallet, WagerRequest{
		PlayerID: "player-1",
		Kind:     domain.TransactionKindRollback,
		Amount:   amount,
	}, reference)
	if !errors.Is(err, ErrRollbackInsufficientBalance) {
		t.Fatalf("applyWalletOperation() error = %v, want %v", err, ErrRollbackInsufficientBalance)
	}
	if operation.balanceChanged {
		t.Error("rejected ROLLBACK must not report a balance change")
	}
	if wallet.Balance().AmountCents() != 1000 || wallet.Version() != 1 {
		t.Error("rejected ROLLBACK must not mutate the wallet")
	}
	if got := failureCodeFor(err); got != "ROLLBACK_INSUFFICIENT_BALANCE" {
		t.Errorf("failureCodeFor() = %q, want ROLLBACK_INSUFFICIENT_BALANCE", got)
	}
}

func TestValidateReferenceAcceptsOnlyCompatibleTransactions(t *testing.T) {
	amount := mustMoney(t, "25.00")
	tests := []struct {
		name          string
		requestKind   domain.TransactionKind
		referenceKind domain.TransactionKind
	}{
		{"refund bet", domain.TransactionKindRefund, domain.TransactionKindBet},
		{"rollback bet", domain.TransactionKindRollback, domain.TransactionKindBet},
		{"rollback win", domain.TransactionKindRollback, domain.TransactionKindWin},
		{"rollback refund", domain.TransactionKindRollback, domain.TransactionKindRefund},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validReferenceRequest(tt.requestKind, amount)
			reference := mustProcessedTransaction(t, tt.referenceKind, amount)
			if err := validateReference(req, reference); err != nil {
				t.Fatalf("validateReference() unexpected error: %v", err)
			}
		})
	}
}

func TestValidateReferenceRejectsIdentityAndBusinessMismatches(t *testing.T) {
	amount := mustMoney(t, "25.00")
	tests := []struct {
		name      string
		change    func(*WagerRequest)
		reference *domain.WagerTransaction
		want      error
	}{
		{
			name: "different provider",
			change: func(req *WagerRequest) {
				req.ProviderID = "provider-2"
			},
			reference: mustProcessedTransaction(t, domain.TransactionKindBet, amount),
			want:      ErrInvalidReference,
		},
		{
			name: "different player",
			change: func(req *WagerRequest) {
				req.PlayerID = "player-2"
			},
			reference: mustProcessedTransaction(t, domain.TransactionKindBet, amount),
			want:      ErrInvalidReference,
		},
		{
			name: "different wallet",
			change: func(req *WagerRequest) {
				req.WalletID = "wallet-2"
			},
			reference: mustProcessedTransaction(t, domain.TransactionKindBet, amount),
			want:      ErrInvalidReference,
		},
		{
			name: "different round",
			change: func(req *WagerRequest) {
				req.RoundID = "round-2"
			},
			reference: mustProcessedTransaction(t, domain.TransactionKindBet, amount),
			want:      ErrInvalidReference,
		},
		{
			name: "different amount",
			change: func(req *WagerRequest) {
				req.Amount = mustMoney(t, "24.00")
			},
			reference: mustProcessedTransaction(t, domain.TransactionKindBet, amount),
			want:      ErrReferenceAmountMismatch,
		},
		{
			name:      "refund references win",
			change:    func(*WagerRequest) {},
			reference: mustProcessedTransaction(t, domain.TransactionKindWin, amount),
			want:      ErrInvalidReference,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validReferenceRequest(domain.TransactionKindRefund, amount)
			tt.change(&req)
			if err := validateReference(req, tt.reference); !errors.Is(err, tt.want) {
				t.Fatalf("validateReference() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestHashWagerRequestExcludesIdempotencyKey(t *testing.T) {
	request := WagerRequest{
		ProviderID:            "provider-1",
		ExternalTransactionID: "transaction-1",
		IdempotencyKey:        "key-1",
		WalletID:              "wallet-1",
		PlayerID:              "player-1",
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  domain.TransactionKindBet,
		Amount:                mustMoney(t, "10.00"),
	}

	firstHash, err := hashWagerRequest(request)
	if err != nil {
		t.Fatalf("hashWagerRequest() error = %v", err)
	}
	request.IdempotencyKey = "key-2"
	secondHash, err := hashWagerRequest(request)
	if err != nil {
		t.Fatalf("hashWagerRequest() error = %v", err)
	}
	if firstHash != secondHash {
		t.Error("canonical payload hash must not include the idempotency key")
	}

	request.ExternalTransactionID = "transaction-2"
	changedPayloadHash, err := hashWagerRequest(request)
	if err != nil {
		t.Fatalf("hashWagerRequest() error = %v", err)
	}
	if firstHash == changedPayloadHash {
		t.Error("canonical payload hash must change when business payload changes")
	}
}

func validReferenceRequest(kind domain.TransactionKind, amount domain.Money) WagerRequest {
	return WagerRequest{
		ProviderID: "provider-1",
		WalletID:   "wallet-1",
		PlayerID:   "player-1",
		RoundID:    "round-1",
		Kind:       kind,
		Amount:     amount,
	}
}

func mustWallet(t *testing.T, balance string) *domain.Wallet {
	t.Helper()

	wallet, err := domain.NewWallet("wallet-1", "player-1", mustMoney(t, balance))
	if err != nil {
		t.Fatalf("NewWallet() unexpected error: %v", err)
	}
	return wallet
}

func mustMoney(t *testing.T, amount string) domain.Money {
	t.Helper()

	money, err := domain.NewMoneyFromString(amount, "BRL")
	if err != nil {
		t.Fatalf("NewMoneyFromString() unexpected error: %v", err)
	}
	return money
}

func mustProcessedTransaction(
	t *testing.T,
	kind domain.TransactionKind,
	amount domain.Money,
) *domain.WagerTransaction {
	t.Helper()

	txn, err := domain.NewWagerTransaction(domain.NewWagerTransactionParams{
		ID:                    "reference-transaction",
		ProviderID:            "provider-1",
		ExternalTransactionID: "reference-external",
		IdempotencyKey:        "reference-idempotency",
		WalletID:              "wallet-1",
		PlayerID:              "player-1",
		RoundID:               "round-1",
		Kind:                  kind,
		Money:                 amount,
	})
	if err != nil {
		t.Fatalf("NewWagerTransaction() unexpected error: %v", err)
	}
	if err := txn.MarkProcessed(); err != nil {
		t.Fatalf("MarkProcessed() unexpected error: %v", err)
	}
	return txn
}
