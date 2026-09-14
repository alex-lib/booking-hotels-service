CREATE TABLE IF NOT EXISTS outbox (
    id              UUID PRIMARY KEY,
    topic           VARCHAR(255)  NOT NULL,
    message_key     VARCHAR(255),
    message_type    VARCHAR(255)  NOT NULL,
    payload         BYTEA         NOT NULL,
    headers         TEXT,
    status          VARCHAR(16)   NOT NULL DEFAULT 'PENDING',
    attempts        INT           NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_error      TEXT,
    claimed_by      VARCHAR(255),
    claimed_until   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_claim ON outbox (status, next_attempt_at, created_at);
