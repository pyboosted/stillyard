# Codex checks while independent review runs

Target: immutable snapshot o, file-map
b0d7b6cd15f22f7fe9b8f840946364588b516e68688c542399f5895b1784f46d.
These are inspection hypotheses to test, not acceptance results or Opus findings.

1. Manager outbox length(command_json/outcome_json) counts SQLite TEXT characters,
   not bytes. Root now uses CAST AS BLOB and has an unvalidated large UTF-8 response
   control that retains room for an acknowledgement. Snapshot q validates this.
2. Candidate limit query counts all rows except released/canceled, while expired
   candidates stay ready/withdrawn in history. 4096 abandoned unused candidates may
   exhaust the domain's active candidate budget forever. Verify with a focused
   scheduled test; separate retained revision history from current admission count.
3. The external authority registry has one 4 MiB limit for both retained base state
   and transient prepare journal. A release includes cleanup proofs plus a copy of
   the Grant. Determine whether previously admitted rights can exhaust space needed
   to journal their safe release; a limit must block new starts while preserving
   cleanup progress. Do not remove obligations or simply raise a limit without a
   reproducible boundary control.
