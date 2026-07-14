-- +goose Up
-- +goose StatementBegin
CREATE TABLE v3_withdrawals (
    id         BIGSERIAL PRIMARY KEY,
    shop_id    BIGINT NOT NULL,
    team_id    BIGINT NOT NULL,
    amount     DOUBLE PRECISION NOT NULL DEFAULT 0,
    funded_at  TIMESTAMP WITH TIME ZONE,
    ref_id     VARCHAR(200) NOT NULL DEFAULT '',
    source     INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_v3_withdrawals_team_id ON v3_withdrawals(team_id);
CREATE INDEX idx_v3_withdrawals_shop_id ON v3_withdrawals(shop_id);
CREATE INDEX idx_v3_withdrawals_deleted_at ON v3_withdrawals(deleted_at);
CREATE UNIQUE INDEX uidx_v3_withdrawal_ref ON v3_withdrawals(team_id, ref_id, source) WHERE deleted_at IS NULL;

CREATE TABLE v3_withdrawal_logs (
    id            BIGSERIAL PRIMARY KEY,
    withdrawal_id BIGINT NOT NULL,
    shop_id       BIGINT NOT NULL,
    team_id       BIGINT NOT NULL,
    change_type   INTEGER NOT NULL DEFAULT 0,
    amount        DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_v3_withdrawal_logs_withdrawal_id ON v3_withdrawal_logs(withdrawal_id);
CREATE INDEX idx_v3_withdrawal_logs_team_id ON v3_withdrawal_logs(team_id);
CREATE INDEX idx_v3_withdrawal_logs_shop_id ON v3_withdrawal_logs(shop_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS v3_withdrawal_logs;
DROP TABLE IF EXISTS v3_withdrawals;
-- +goose StatementEnd
