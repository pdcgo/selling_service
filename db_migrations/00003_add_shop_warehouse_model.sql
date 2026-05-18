-- +goose Up
-- +goose StatementBegin
CREATE TABLE shop_warehouses (
    id            SERIAL PRIMARY KEY,
    shop_id       BIGINT NOT NULL,
    warehouse_id  BIGINT NOT NULL,
    last_order_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_shop_warehouse ON shop_warehouses (shop_id, warehouse_id);
CREATE INDEX idx_shop_warehouses_shop_id ON shop_warehouses (shop_id);
CREATE INDEX idx_shop_warehouses_warehouse_id ON shop_warehouses (warehouse_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS shop_warehouses;
-- +goose StatementEnd