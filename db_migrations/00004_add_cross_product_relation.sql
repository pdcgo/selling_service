-- +goose Up
-- +goose StatementBegin
CREATE TABLE team_cross_products (
    id         BIGSERIAL PRIMARY KEY,
    team_id    BIGINT NOT NULL DEFAULT 0,
    product_id BIGINT NOT NULL DEFAULT 0,
    shop_id    BIGINT NOT NULL DEFAULT 0,
    user_id    BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uniq_tcp_team_product_shop_user UNIQUE (team_id, product_id, shop_id, user_id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS team_cross_products;
-- +goose StatementEnd