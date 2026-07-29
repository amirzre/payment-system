CREATE TABLE processed_events (
    id              BIGSERIAL   PRIMARY KEY,
    consumer_name   TEXT        NOT NULL,
    event_key       TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (consumer_name, event_key)
);
