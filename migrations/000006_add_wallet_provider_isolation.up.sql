ALTER TABLE wallets
    ADD COLUMN provider_id TEXT;

UPDATE wallets AS wallet
SET provider_id = (
    SELECT transaction.provider_id
    FROM wager_transactions AS transaction
    WHERE transaction.wallet_id = wallet.id
    ORDER BY
        CASE WHEN transaction.kind = 'OPENING' THEN 0 ELSE 1 END,
        transaction.created_at,
        transaction.id
    LIMIT 1
);

ALTER TABLE wallets
    ALTER COLUMN provider_id SET NOT NULL,
    DROP CONSTRAINT wallets_player_currency_unique,
    ADD CONSTRAINT wallets_provider_player_currency_unique
        UNIQUE (provider_id, player_id, currency),
    ADD CONSTRAINT wallets_provider_id_unique
        UNIQUE (provider_id, id);

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_provider_wallet_fk
        FOREIGN KEY (provider_id, wallet_id)
        REFERENCES wallets (provider_id, id);
