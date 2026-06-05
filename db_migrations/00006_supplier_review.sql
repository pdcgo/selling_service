-- +goose Up
-- +goose StatementBegin
CREATE TABLE supplier_reviews (
    id          BIGSERIAL PRIMARY KEY,
    supplier_id BIGINT      NOT NULL,
    team_id     BIGINT      NOT NULL,
    user_id     VARCHAR(255) NOT NULL,
    review      TEXT        NOT NULL,
    rating      BIGINT      NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_supplier_reviews_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES v2_suppliers(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_supplier_reviews_supplier_id ON supplier_reviews(supplier_id);
CREATE INDEX idx_supplier_reviews_team_id     ON supplier_reviews(team_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS supplier_reviews;
-- +goose StatementEnd