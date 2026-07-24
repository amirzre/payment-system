CREATE TABLE IF NOT EXISTS accounts (
    id          UUID        PRIMARY KEY,
    owner_ref   TEXT        NOT NULL,
    currency    CHAR(3)     NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'frozen', 'closed')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
