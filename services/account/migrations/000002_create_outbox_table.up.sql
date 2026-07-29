CREATE TABLE outbox (
    id          BIGSERIAL   PRIMARY KEY,
    subject     TEXT        NOT NULL,
    key         TEXT        NOT NULL,
    payload     JSONB       NOT NULL DEFAULT '{}',
    published   BOOLEAN     NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_published ON outbox (published, created_at);
