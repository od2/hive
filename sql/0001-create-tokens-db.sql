CREATE TABLE auth_tokens (
    id BINARY(12) NOT NULL PRIMARY KEY,
    worker_id BIGINT NOT NULL,
    expires_at TIMESTAMP NOT NULL
);