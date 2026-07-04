-- +goose Up
-- +goose StatementBegin
CREATE TABLE v2_shops (
    id               BIGSERIAL PRIMARY KEY,
    team_id          BIGINT NOT NULL,
    shop_name        VARCHAR(200) NOT NULL DEFAULT '',
    shop_username    VARCHAR(200) NOT NULL DEFAULT '',
    marketplace_type INTEGER NOT NULL DEFAULT 0,
    uri              VARCHAR(500) NOT NULL DEFAULT '',
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at       TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_v2_shops_team_id ON v2_shops(team_id);
CREATE INDEX idx_v2_shops_deleted_at ON v2_shops(deleted_at);
CREATE UNIQUE INDEX uidx_v2_shop_username ON v2_shops(team_id, shop_username) WHERE deleted_at IS NULL;

CREATE TABLE v2_shop_users (
    id         BIGSERIAL PRIMARY KEY,
    shop_id    BIGINT NOT NULL,
    team_id    BIGINT NOT NULL,
    user_id    BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_v2_shop_users_shop_id ON v2_shop_users(shop_id);
CREATE INDEX idx_v2_shop_users_team_id ON v2_shop_users(team_id);
CREATE INDEX idx_v2_shop_users_user_id ON v2_shop_users(user_id);
CREATE INDEX idx_v2_shop_users_deleted_at ON v2_shop_users(deleted_at);
CREATE UNIQUE INDEX uidx_v2_shop_user ON v2_shop_users(shop_id, user_id) WHERE deleted_at IS NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS v2_shop_users;
DROP TABLE IF EXISTS v2_shops;
-- +goose StatementEnd
