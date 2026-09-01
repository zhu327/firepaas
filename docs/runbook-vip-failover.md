# VIP failover validation

Use two edge nodes with keepalived or equivalent and run the probe from a third client. Set `VIP_ACTIVE_HOST`, `VIP_FAILOVER_COMMAND`, and `VIP_OWNER_COMMAND`; owner command must return one host identifier. `WORKLOAD_URL` must traverse the VIP, not a node address.

Run `bash scripts/lab/e2e-vip-failover.sh`. It first proves the baseline owner and HTTP 200, injects failure, then requires both continued HTTP 200 and a different VIP owner. Restore the failed edge after collecting keepalived and proxy logs.
