# Release records

These records distinguish implemented code from release evidence. Accepted ADRs remain the authority for design decisions; plans preserve scope and entry/exit contracts; the GA scorecard remains `NOT ASSESSED` until a complete archive is independently reviewed.

| Release | Scope contract | Implementation record | Current evidence status |
|---|---|---|---|
| MVP (M0–M5) | [mvp-plan.md](../mvp-plan.md) | [historical milestone records](../archive/milestones/README.md) | Major paths and single-node scenarios exist; complete release evidence is not established here. |
| v1.1 | [v1.1-plan.md](../v1.1-plan.md) | [v1.1 implementation notes](../v1.1-implementation-notes.md) | Work packages implemented; standard HA topology and long-duration gates are not established by the notes. |
| v1.2 | [v1.2-plan.md](../v1.2-plan.md) | [v1.2 implementation notes](../v1.2-implementation-notes.md) | Major paths implemented; notes explicitly mark the release gate unmet. |
| v1.3 | [v1.3-plan.md](../v1.3-plan.md) | [v1.3 implementation notes](../v1.3-implementation-notes.md) | Egress, snapshot and LOCAL_RW paths implemented; CoW remains disabled and version-level gates are not established. |
| v1.4 | [v1.4-plan.md](../v1.4-plan.md) | [v1.4 implementation notes](../v1.4-implementation-notes.md) | Partial implementation; version-level acceptance is not complete. |

Current GA evidence status: [GA observation scorecard](../ga-observation-scorecard.md).
