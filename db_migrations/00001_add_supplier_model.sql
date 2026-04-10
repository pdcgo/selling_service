-- +goose Up
CREATE TABLE v2_suppliers (
    id BIGSERIAL PRIMARY KEY,
    team_id BIGINT NOT NULL,
    code TEXT,
    name TEXT,
    contact TEXT,
    province TEXT,
    city TEXT,
    description TEXT,
    address TEXT,
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_supplier_v2_team_id ON v2_suppliers(team_id);
CREATE INDEX idx_supplier_v2_deleted_at ON v2_suppliers(deleted_at);
CREATE UNIQUE INDEX uidx_code_active  ON v2_suppliers(code) WHERE deleted_at IS NULL;

CREATE TABLE v2_supplier_marketplaces (
    id BIGSERIAL PRIMARY KEY,
    supplier_id BIGINT NOT NULL,
    mp_type INTEGER NOT NULL,
    shop_name VARCHAR(200) NOT NULL DEFAULT '',
    product_name VARCHAR(250) NOT NULL DEFAULT '',
    uri VARCHAR(500) NOT NULL DEFAULT '',
    description VARCHAR(500) NOT NULL DEFAULT '',
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_supplier_marketplace_v2_supplier_id ON v2_supplier_marketplaces (supplier_id);
CREATE INDEX idx_supplier_marketplace_v2_deleted_at ON v2_supplier_marketplaces (deleted_at);

-- +goose Down
DROP TABLE IF EXISTS v2_suppliers;
DROP TABLE IF EXISTS v2_supplier_marketplaces;


