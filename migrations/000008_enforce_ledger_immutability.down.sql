DROP TRIGGER IF EXISTS wallet_ledger_entries_immutable
    ON wallet_ledger_entries;

DROP FUNCTION IF EXISTS reject_wallet_ledger_mutation();
