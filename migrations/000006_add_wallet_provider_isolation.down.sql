ALTER TABLE wager_transactions
    DROP CONSTRAINT IF EXISTS wager_transactions_provider_wallet_fk;

ALTER TABLE wallets
    DROP CONSTRAINT IF EXISTS wallets_provider_id_unique,
    DROP CONSTRAINT IF EXISTS wallets_provider_player_currency_unique,
    ADD CONSTRAINT wallets_player_currency_unique
        UNIQUE (player_id, currency),
    DROP COLUMN IF EXISTS provider_id;
