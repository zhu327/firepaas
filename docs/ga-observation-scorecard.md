# GA observation scorecard

Status is **NOT ASSESSED** until a complete evidence archive is reviewed. This document does not declare GA readiness.

| Gate | Required evidence | Status |
|---|---|---|
| Two-compute scheduling | scheduler result, placement raw JSON | NOT ASSESSED |
| Compute failover | fencing event, replacement/traffic result | NOT ASSESSED |
| Three-member control quorum | stopped-member event and committed write | NOT ASSESSED |
| Two-edge VIP failover | owner transition and client 200 result | NOT ASSESSED |
| Isolated DR restore | backup URI record, restore validation | NOT ASSESSED |
| 72-hour soak | continuous observations, SLO result | NOT ASSESSED |
| 30-day observation | daily observations, SLO result | NOT ASSESSED |

Reviewers must verify commit/config/topology checksums, fault scope, raw samples, and every result’s explicit `PASS`. Any failed, absent, expired, unreviewed, or non-comparable evidence leaves the status NOT ASSESSED. Record approval in the release process, not by editing this template.
