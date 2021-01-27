CREATE TABLE auth_tokens
(
    id           BINARY(12)  NOT NULL PRIMARY KEY,
    worker_id    BIGINT      NOT NULL,
    created_at   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP   NULL,
    token_bit    CHAR(4)     NOT NULL,
    description  VARCHAR(64) NOT NULL
);
