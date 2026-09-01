# DR recovery rehearsal

Use a fresh isolated recovery target; never overwrite the source environment. Record immutable backup URI, encryption/key-access approval outside the evidence archive, target topology, and source commit/config. Set `DR_BACKUP_URI`, `DR_RESTORE_COMMAND`, and `DR_VALIDATION_COMMAND`.

Validation command must emit booleans `restore_isolated`, `schema_valid`, `data_integrity_valid`, and `traffic_valid`. Run `bash scripts/lab/dr-rehearsal.sh`. It fails closed on any false/missing field. Measure RTO from restore start; document data timestamp separately. This rehearsal does not create RPO/RTO promises for node-local data.
