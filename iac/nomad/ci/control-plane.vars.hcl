# Non-sensitive syntax-validation fixture. Never submit this file to a cluster.
api_image           = "registry.example.invalid/firepaas/api@sha256:0000000000000000000000000000000000000000000000000000000000000000"
postgres_url        = "postgres://fixture:fixture@127.0.0.1:5432/fixture?sslmode=disable"
redis_addr          = "127.0.0.1:6379"
nomad_addr          = "http://127.0.0.1:4646"
api_token           = "ci-validation-placeholder"
secrets_master_key  = "ci-validation-placeholder"
traffic_token_key   = "ci-validation-placeholder"
# TLS variables carry PEM contents (materialized via templates), not paths.
agent_tls_cert      = "-----BEGIN CERTIFICATE-----\nci-fixture\n-----END CERTIFICATE-----\n"
agent_tls_key       = "-----BEGIN PRIVATE KEY-----\nci-fixture\n-----END PRIVATE KEY-----\n"
agent_tls_ca        = "-----BEGIN CERTIFICATE-----\nci-fixture\n-----END CERTIFICATE-----\n"
