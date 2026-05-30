-- +goose Up
CREATE TABLE supplier_order_logs (
    id          BIGSERIAL         PRIMARY KEY,
    log_type    INTEGER           NOT NULL,
    supplier_id BIGINT            NOT NULL,
    product_id  BIGINT            NOT NULL,
    order_id    BIGINT            NOT NULL,
    team_id     BIGINT            NOT NULL,
    count       BIGINT            NOT NULL DEFAULT 0,
    amount      DOUBLE PRECISION  NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ       NOT NULL DEFAULT now(),
    event_at    TIMESTAMPTZ       NOT NULL
);

CREATE INDEX idx_supplier_order_logs_supplier_id ON supplier_order_logs (supplier_id);
CREATE INDEX idx_supplier_order_logs_team_id     ON supplier_order_logs (team_id);
CREATE INDEX idx_supplier_order_logs_order_id    ON supplier_order_logs (order_id);
CREATE INDEX idx_supplier_order_logs_event_at    ON supplier_order_logs (event_at);

-- +goose Down
DROP TABLE supplier_order_logs;