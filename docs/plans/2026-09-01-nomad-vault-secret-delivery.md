# Nomad production secret delivery via Vault

## Goal

Remove database credentials, API tokens, signing keys, and TLS private material from Nomad job variables and job specifications while preserving fail-closed startup and independently rotatable API and edge identities.

## Architecture

Nomad workload identities authenticate each task to Vault. The `firepaas-api` and `firepaas-edge` tasks receive only non-secret service addresses and Vault secret paths in the submitted job. Nomad `template` blocks read narrowly scoped KV v2 records and materialize:

- an environment file in `secrets/` for scalar credentials;
- PEM files in `secrets/` for mTLS and ingress identities.

The application continues to consume its existing environment variables and file paths, so no application protocol or storage migration is required. Vault is the source for deployment credentials; PostgreSQL remains authoritative for application secrets managed by FirePaaS.

## Validation

- `bash -n scripts/lab/*.sh`
- Nomad 1.10.5 and the supported Nomad 2.x version: `scripts/lab/validate-nomad.sh`
- `make check`
- Staging `nomad job plan` and `nomad job run` with workload identity enabled
- Assert secret values are absent from `nomad job inspect`, allocation environment metadata, logs, and evidence archives
- Rotate each Vault secret and prove the intended task restart/reload behavior

## Dependencies

| Task | Type | Blocked by | Parallelizable with |
|---|---|---|---|
| Provision Vault auth and policies | HITL | Vault service and Nomad workload identity trust | Secret population |
| Convert control-plane job | AFK | Approved Vault paths/policy names | Edge conversion |
| Convert edge job | AFK | Approved Vault paths/policy names | Control-plane conversion |
| Stage and rotate | HITL | Both job conversions | None |
| Remove legacy secret variables | AFK | Successful staged rotation | None |

## Secret layout and policies

Use separate KV v2 paths and policies so an edge allocation cannot read control-plane material:

```text
secret/data/firepaas/control-plane/runtime
secret/data/firepaas/control-plane/agent-mtls
secret/data/firepaas/edge/runtime
secret/data/firepaas/edge/agent-mtls
secret/data/firepaas/edge/ingress-tls
```

`firepaas-api` policy grants `read` only to the two control-plane paths. `firepaas-edge` policy grants `read` only to the three edge paths. Policies must be bound to Nomad workload identity claims for the exact namespace, job, and task; no shared wildcard role.

## Migration sequence

1. Provision Vault HA, audit logging, sealed-backup procedure, and Nomad workload identity integration outside this repository.
2. Populate versioned secret paths and verify certificate/key pairing and minimum key lengths offline.
3. Add `vault` stanzas and Vault-backed templates to each job while temporarily retaining the legacy variables as an explicit rollback mode. Default to Vault mode and reject a mixed configuration.
4. Deploy edge canaries, verify TLS ingress and edge-to-agent authorization, then promote.
5. Deploy the single active control-plane canary, verify PostgreSQL/Redis/Nomad access, API auth, secret decryption, and traffic-token continuity, then promote.
6. Rotate API token, signing keys, and certificates one class at a time. Record expected restart behavior and verify old credentials are rejected after the overlap window.
7. Remove the legacy secret-value variables and CI fixtures after the staged deployment has remained healthy for the agreed observation window.

## Fail-closed behavior

- Missing Vault identity, denied path, empty required field, malformed PEM, or unavailable Vault during initial rendering prevents task start.
- Templates use `perms = "0400"`; private keys never enter environment variables.
- Scalar environment files use `env = true`, `perms = "0400"`, and contain only the exact required keys.
- Secret template changes signal or restart the task explicitly; no silent partial reload.
- Job plans and CI fixtures contain paths and role names only, never usable credentials.

## Rollback

During the transition only, preserve the prior deployment artifact and a protected legacy variable file outside Git. If Vault rendering causes an outage, roll back the Nomad job version and artifact together. Trigger rollback when canary health fails, a required template is absent, or traffic/auth checks fail. Never copy rendered allocation secrets into command-line `-var` values. Remove the legacy path after the observation window; after removal, rollback means restoring the previous Vault-backed job version, not reverting to plaintext variables.

## Non-goals

- Deploying or operating Vault in this repository.
- Moving tenant application secrets out of PostgreSQL envelope encryption.
- Adding multi-writer control-plane behavior.
