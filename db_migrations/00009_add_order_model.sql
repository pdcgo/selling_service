-- +goose Up
-- +goose StatementBegin
CREATE TABLE v3_orders (
    id                       BIGSERIAL PRIMARY KEY,
    team_id                  BIGINT NOT NULL,
    shop_id                  BIGINT NOT NULL,
    warehouse_id             BIGINT NOT NULL,
    ref_id                   VARCHAR(200) NOT NULL DEFAULT '',
    status                   VARCHAR(32) NOT NULL DEFAULT 'created',
    shipment_status          VARCHAR(32) NOT NULL DEFAULT '',
    canceled_at              TIMESTAMP WITH TIME ZONE,
    problem                  BOOLEAN NOT NULL DEFAULT FALSE,
    problem_note             TEXT NOT NULL DEFAULT '',
    inventory_transaction_id BIGINT NOT NULL DEFAULT 0,
    shipping_fee             DOUBLE PRECISION NOT NULL DEFAULT 0,
    total                    DOUBLE PRECISION NOT NULL DEFAULT 0,
    est_revenue_amount       DOUBLE PRECISION NOT NULL DEFAULT 0,
    warehouse_fee            DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_v3_orders_team_id ON v3_orders(team_id);
CREATE INDEX idx_v3_orders_shop_id ON v3_orders(shop_id);
CREATE INDEX idx_v3_orders_warehouse_id ON v3_orders(warehouse_id);
CREATE INDEX idx_v3_orders_status ON v3_orders(status);
CREATE UNIQUE INDEX uidx_v3_order_ref ON v3_orders(team_id, ref_id);

CREATE TABLE v3_order_items (
    id             BIGSERIAL PRIMARY KEY,
    order_id       BIGINT NOT NULL,
    product_id     BIGINT NOT NULL,
    product_name   VARCHAR(200) NOT NULL DEFAULT '',
    count          BIGINT NOT NULL DEFAULT 0,
    price          DOUBLE PRECISION NOT NULL DEFAULT 0,
    subtotal       DOUBLE PRECISION NOT NULL DEFAULT 0,
    owned          BOOLEAN NOT NULL DEFAULT FALSE,
    owner_team_id  BIGINT NOT NULL DEFAULT 0,
    markup_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
    product_fee    DOUBLE PRECISION NOT NULL DEFAULT 0
);
CREATE INDEX idx_v3_order_items_order_id ON v3_order_items(order_id);

CREATE TABLE v3_order_customers (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL,
    name        VARCHAR(200) NOT NULL DEFAULT '',
    phone       VARCHAR(50) NOT NULL DEFAULT '',
    province    VARCHAR(100) NOT NULL DEFAULT '',
    city        VARCHAR(100) NOT NULL DEFAULT '',
    district    VARCHAR(100) NOT NULL DEFAULT '',
    postal_code VARCHAR(20) NOT NULL DEFAULT '',
    address     VARCHAR(500) NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX uidx_v3_order_customers_order_id ON v3_order_customers(order_id);

CREATE TABLE v3_order_status_logs (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL,
    dimension   VARCHAR(20) NOT NULL DEFAULT '',
    from_status VARCHAR(32) NOT NULL DEFAULT '',
    to_status   VARCHAR(32) NOT NULL DEFAULT '',
    note        TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_v3_order_status_logs_order_id ON v3_order_status_logs(order_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS v3_order_status_logs;
DROP TABLE IF EXISTS v3_order_customers;
DROP TABLE IF EXISTS v3_order_items;
DROP TABLE IF EXISTS v3_orders;
-- +goose StatementEnd
