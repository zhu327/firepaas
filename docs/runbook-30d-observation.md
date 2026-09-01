# 30-day observation gate

This is a calendar-duration gate, not a load-test loop. Start after 72-hour soak passes. Set `DAILY_OBSERVATION_COMMAND` to collect daily JSON observations, `OBSERVATION_DAYS=30`, `OBSERVATION_INTERVAL_SECONDS=86400`, `OBSERVATION_MIN_GAP_SECONDS=82800`, SLO spec, and evidence path.

Run `bash scripts/lab/observe-30d.sh` on a durable monitored runner. The script rejects compressed days, command failures, and insufficient SLO samples. Interruptions must be recorded as a failed/incomplete observation period and restarted; do not backfill measurements.
