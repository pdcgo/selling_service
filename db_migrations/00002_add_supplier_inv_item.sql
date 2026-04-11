-- +goose Up
CREATE TABLE v2_supplier_inv_tx_items (
    id BIGSERIAL PRIMARY KEY,
    inv_tx_item_id BIGINT,
    supplier_id BIGINT
);

-- index
CREATE INDEX idx_v2_supplier_inv_tx_items_inv_tx_item_id ON v2_supplier_inv_tx_items(inv_tx_item_id);
CREATE INDEX idx_v2_supplier_inv_tx_items_supplier_id ON v2_supplier_inv_tx_items(supplier_id);

-- foreign key ke inv_tx_items
ALTER TABLE v2_supplier_inv_tx_items ADD CONSTRAINT fk_v2_supplier_inv_tx_items_inv_tx_item FOREIGN KEY (inv_tx_item_id) REFERENCES inv_tx_items(id) ON DELETE CASCADE;
ALTER TABLE v2_supplier_inv_tx_items ADD CONSTRAINT fk_v2_supplier_inv_tx_items_supplier_marketplace FOREIGN KEY (supplier_id) REFERENCES v2_supplier_marketplaces(id) ON DELETE CASCADE;

-- +goose Down
DROP TABLE IF EXISTS v2_supplier_inv_tx_items;
