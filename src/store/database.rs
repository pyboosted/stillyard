use super::*;

const JOBS_TREE_ORDER_INSERT_TRIGGER: &str = "CREATE TRIGGER jobs_tree_order_insert
AFTER INSERT ON jobs BEGIN
    UPDATE meta SET value = CAST(value AS INTEGER) + 1
    WHERE key = 'tree_order_revision';
END";

const JOBS_TREE_ORDER_UPDATE_TRIGGER: &str = "CREATE TRIGGER jobs_tree_order_update
AFTER UPDATE OF state, outcome, parent_job_id, parent_attempt_id, parent_invocation_id
ON jobs
WHEN OLD.state IS NOT NEW.state
  OR OLD.outcome IS NOT NEW.outcome
  OR OLD.parent_job_id IS NOT NEW.parent_job_id
  OR OLD.parent_attempt_id IS NOT NEW.parent_attempt_id
  OR OLD.parent_invocation_id IS NOT NEW.parent_invocation_id BEGIN
    UPDATE meta SET value = CAST(value AS INTEGER) + 1
    WHERE key = 'tree_order_revision';
END";

pub(super) fn load_host_config(path: &Path) -> StoreResult<HostConfig> {
    match File::open(path) {
        Ok(file) => {
            let config: HostConfig = serde_json::from_reader(file)?;
            config
                .validate()
                .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
            Ok(config)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(HostConfig::default()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn open_lock(path: &Path) -> StoreResult<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?)
}

pub(super) fn configure_database(connection: &Connection) -> StoreResult<()> {
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;
         PRAGMA busy_timeout = 5000;",
    )?;
    Ok(())
}

pub(super) fn schema_is_current(connection: &Connection) -> StoreResult<bool> {
    let meta_exists = match connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta')",
        [],
        |row| row.get::<_, bool>(0),
    ) {
        Ok(exists) => exists,
        Err(error) => return schema_probe_error(error.into()),
    };
    if !meta_exists {
        return Ok(false);
    }
    let meta_columns = match table_columns(connection, "meta") {
        Ok(columns) => columns,
        Err(error) => return schema_probe_error(error),
    };
    if !["key", "value"]
        .iter()
        .all(|column| meta_columns.contains(*column))
    {
        return Ok(false);
    }

    let epoch = match connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_epoch'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
    {
        Ok(epoch) => epoch,
        Err(error) => return schema_probe_error(error.into()),
    };
    if epoch.as_deref() != Some(STORE_SCHEMA_EPOCH) {
        return Ok(false);
    }

    match current_store_uuid(connection) {
        Ok(_) => {}
        Err(error) => return schema_probe_error(error),
    }
    match validate_schema(connection) {
        Ok(()) => Ok(true),
        Err(error) => schema_probe_error(error),
    }
}

pub(super) fn schema_probe_error(error: StoreError) -> StoreResult<bool> {
    match error {
        StoreError::InvalidState(_) => Ok(false),
        StoreError::Sqlite(ref sqlite) if is_database_corruption(sqlite) => Ok(false),
        other => Err(other),
    }
}

pub(super) fn is_database_corruption(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase,
                ..
            },
            _
        )
    )
}

pub(super) fn table_columns(
    connection: &Connection,
    table: &str,
) -> StoreResult<std::collections::HashSet<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    statement
        .query_map([], |row| row.get(1))?
        .collect::<std::result::Result<_, _>>()
        .map_err(Into::into)
}

pub(super) fn current_store_uuid(connection: &Connection) -> StoreResult<Uuid> {
    let value = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'store_uuid'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::InvalidState("current store has no store_uuid".into()))?;
    Uuid::parse_str(&value)
        .map_err(|_| StoreError::InvalidState("current store has an invalid store_uuid".into()))
}

pub(super) fn meta_value(connection: &Connection, key: &str) -> StoreResult<Option<String>> {
    connection
        .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
        .map_err(Into::into)
}

pub(super) fn host_binding_is_acceptable(
    connection: &Connection,
    current_host_id: Option<&HostId>,
) -> StoreResult<bool> {
    let bound_host_id = meta_value(connection, "bound_host_id")?;
    match (bound_host_id.as_deref(), current_host_id) {
        (Some(bound), Some(current)) => Ok(bound == current.0),
        // An unavailable identity is a capability failure, not evidence that the
        // durable store moved. Keep diagnostics available and block admission.
        (Some(_), None) => Ok(true),
        (None, _) => {
            let durable_state_exists: bool = connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM containments)
                    OR EXISTS(SELECT 1 FROM leases WHERE state = 'granted')",
                [],
                |row| row.get(0),
            )?;
            Ok(!durable_state_exists)
        }
    }
}

pub(super) fn bind_unbound_store(
    connection: &Connection,
    current_host_id: Option<&HostId>,
) -> StoreResult<()> {
    let Some(current_host_id) = current_host_id else {
        return Ok(());
    };
    if meta_value(connection, "bound_host_id")?.is_some() {
        return Ok(());
    }
    connection.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> StoreResult<()> {
        let durable_state_exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM containments)
                OR EXISTS(SELECT 1 FROM leases WHERE state = 'granted')",
            [],
            |row| row.get(0),
        )?;
        if durable_state_exists {
            return Err(StoreError::InvalidState(
                "unbound store gained durable containment state while binding".into(),
            ));
        }
        connection.execute(
            "INSERT INTO meta(key, value) VALUES ('bound_host_id', ?1)",
            [&current_host_id.0],
        )?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            connection.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

pub(super) fn reset_database_files(paths: &StorePaths) -> StoreResult<()> {
    for path in [
        sqlite_sidecar_path(&paths.database, "-wal"),
        sqlite_sidecar_path(&paths.database, "-shm"),
        paths.database.clone(),
    ] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(super) fn sqlite_sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

pub(super) fn create_current_schema(
    connection: &Connection,
    store_uuid: Uuid,
    bound_host_id: Option<&HostId>,
) -> StoreResult<()> {
    let bound_host_meta = bound_host_id.map_or_else(String::new, |host_id| {
        format!(
            "INSERT INTO meta(key, value) VALUES ('bound_host_id', '{}');",
            host_id.0.replace('\'', "''")
        )
    });
    connection.execute_batch(&format!(
        "BEGIN IMMEDIATE;
         CREATE TABLE meta(
             key TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         CREATE TABLE submissions(
             id TEXT PRIMARY KEY,
             scope TEXT NOT NULL,
             idempotency_key TEXT NOT NULL,
             payload_hash TEXT NOT NULL,
             state TEXT NOT NULL,
             spec_json TEXT NOT NULL,
             stdin_json TEXT,
             job_id TEXT,
             kind TEXT NOT NULL DEFAULT 'job',
             batch_id TEXT,
             parent_job_id TEXT,
             parent_attempt_id TEXT,
             parent_invocation_id TEXT,
             wait_intent INTEGER NOT NULL DEFAULT 0,
             reject_code TEXT,
             reject_detail TEXT,
             daemon_generation TEXT,
             created_ms INTEGER NOT NULL,
             UNIQUE(scope, idempotency_key)
         );
         CREATE TABLE batches(
             id TEXT PRIMARY KEY,
             state TEXT NOT NULL,
             submission_id TEXT REFERENCES submissions(id),
             accepted_ms INTEGER
         );
         CREATE TABLE jobs(
             id TEXT PRIMARY KEY,
             submission_id TEXT NOT NULL REFERENCES submissions(id),
             batch_id TEXT REFERENCES batches(id),
             batch_member TEXT,
             batch_index INTEGER,
             state TEXT NOT NULL,
             outcome TEXT,
             spec_json TEXT NOT NULL,
             claims_json TEXT NOT NULL DEFAULT '{{}}',
             stdin_hash TEXT,
             stdin_len INTEGER,
             attempt_id TEXT,
             invocation_id TEXT,
             containment_id TEXT,
             root_exit_code INTEGER,
             accepted_ms INTEGER NOT NULL,
             started_ms INTEGER,
             finished_ms INTEGER,
             stdout_len INTEGER NOT NULL DEFAULT 0,
             stderr_len INTEGER NOT NULL DEFAULT 0,
             cancel_requested INTEGER NOT NULL DEFAULT 0,
             retry_not_before_ms INTEGER,
             reservation_not_before_ms INTEGER,
             parent_job_id TEXT,
             parent_attempt_id TEXT,
             parent_invocation_id TEXT,
             resolved_child_policy_json TEXT,
             managed_policy_admission_json TEXT
         );
         CREATE TABLE dependencies(
             predecessor_id TEXT NOT NULL REFERENCES jobs(id),
             successor_id TEXT NOT NULL REFERENCES jobs(id),
             kind TEXT NOT NULL,
             PRIMARY KEY(predecessor_id, successor_id, kind)
         );
         CREATE TABLE attempts(
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL REFERENCES jobs(id),
             state TEXT NOT NULL,
             attempt_index INTEGER NOT NULL,
             verdict TEXT,
             safety_reason TEXT,
             created_ms INTEGER NOT NULL,
             started_ms INTEGER,
             deadline_ms INTEGER,
             finished_ms INTEGER,
             primary_result_json TEXT,
             UNIQUE(job_id, attempt_index)
         );
         CREATE TABLE admissions(
             attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
             admitting_started_ms INTEGER NOT NULL,
             wall_deadline_ms INTEGER NOT NULL,
             quiet_consumed_ms INTEGER NOT NULL DEFAULT 0,
             last_eval_monotonic_ms INTEGER,
             last_eval_generation TEXT,
             quiet_generation TEXT,
             quiet_first_monotonic_ms INTEGER,
             quiet_last_monotonic_ms INTEGER,
             deferral_count INTEGER NOT NULL DEFAULT 0,
             retry_not_before_ms INTEGER,
             last_blockers_json TEXT NOT NULL DEFAULT '[]',
             last_eval_unix_ms INTEGER,
             last_evidence_json TEXT,
             reservation_generation TEXT,
             reservation_evidence_json TEXT,
             release_evidence_json TEXT,
             gpu_uuid TEXT,
             gpu_driver_version TEXT
         );
         CREATE TABLE daemon_generations(
             generation TEXT PRIMARY KEY,
             process_identity_json TEXT NOT NULL,
             started_ms INTEGER NOT NULL
         );
         CREATE TABLE invocations(
             id TEXT PRIMARY KEY,
             attempt_id TEXT NOT NULL REFERENCES attempts(id),
             role TEXT NOT NULL,
             role_index INTEGER NOT NULL DEFAULT 0,
             state TEXT NOT NULL,
             root_pid INTEGER,
             root_host_id TEXT,
             root_boot_id TEXT,
             root_creation_filetime_100ns INTEGER,
             root_exit_code INTEGER,
             exited_ms INTEGER,
             executable_hash TEXT,
             daemon_generation TEXT,
             postcondition_index INTEGER,
             started_ms INTEGER,
             finished_ms INTEGER,
             exit_classification TEXT,
             stdout_tail TEXT NOT NULL DEFAULT '',
             stderr_tail TEXT NOT NULL DEFAULT '',
             UNIQUE(attempt_id, role_index)
         );
         CREATE TABLE containments(
             id TEXT PRIMARY KEY,
             invocation_id TEXT NOT NULL REFERENCES invocations(id),
             state TEXT NOT NULL,
             host_id TEXT,
             boot_id TEXT,
             daemon_generation TEXT,
             strength TEXT,
             version INTEGER NOT NULL DEFAULT 1,
             incident_sequence INTEGER,
             reason_code TEXT,
             detail TEXT,
             opened_ms INTEGER,
             retained_claims_json TEXT,
             resolution TEXT,
             resolved_ms INTEGER,
             last_reconciliation TEXT,
             resolution_audit_json TEXT
         );
         CREATE TABLE conditions(
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL REFERENCES jobs(id),
             state TEXT NOT NULL,
             spec_json TEXT NOT NULL
         );
         CREATE TABLE observations(
             id TEXT PRIMARY KEY,
             condition_id TEXT NOT NULL REFERENCES conditions(id),
             observed_ms INTEGER NOT NULL,
             value_json TEXT NOT NULL
         );
         CREATE TABLE leases(
             id TEXT PRIMARY KEY,
             attempt_id TEXT NOT NULL REFERENCES attempts(id),
             state TEXT NOT NULL,
             claims_json TEXT NOT NULL
         );
         CREATE TABLE reservations(
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
             claims_json TEXT NOT NULL,
             created_ms INTEGER NOT NULL,
             hold_deadline_ms INTEGER NOT NULL,
             CHECK (hold_deadline_ms > created_ms)
         );
         CREATE TABLE events(
             sequence INTEGER PRIMARY KEY AUTOINCREMENT,
             kind TEXT NOT NULL,
             job_id TEXT NOT NULL REFERENCES jobs(id),
             batch_id TEXT REFERENCES batches(id),
             attempt_id TEXT REFERENCES attempts(id),
             invocation_id TEXT REFERENCES invocations(id),
             transition TEXT,
             committed_ms INTEGER NOT NULL,
             CHECK (
                 (kind = 'invocation_changed'
                     AND attempt_id IS NOT NULL
                     AND invocation_id IS NOT NULL
                     AND transition IN ('started', 'exited'))
                 OR
                 (kind != 'invocation_changed'
                     AND attempt_id IS NULL
                     AND invocation_id IS NULL
                     AND transition IS NULL)
             )
         );
         CREATE INDEX events_job_sequence ON events(job_id, sequence);
         CREATE INDEX jobs_parent_accepted ON jobs(parent_job_id, accepted_ms, id);
         CREATE INDEX jobs_state_accepted ON jobs(state, accepted_ms, id);
         CREATE INDEX jobs_accepted_order ON jobs(accepted_ms, id);
         CREATE INDEX reservations_hold_deadline ON reservations(hold_deadline_ms, job_id);
         CREATE TRIGGER events_prune AFTER INSERT ON events BEGIN
             DELETE FROM events WHERE sequence <= NEW.sequence - {MAX_EVENT_ROWS};
         END;
         CREATE TRIGGER jobs_event_insert AFTER INSERT ON jobs BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             VALUES ('job_changed', NEW.id, NEW.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER));
         END;
         CREATE TRIGGER jobs_event_update AFTER UPDATE ON jobs
         WHEN OLD.state IS NOT NEW.state
           OR OLD.outcome IS NOT NEW.outcome
           OR OLD.spec_json IS NOT NEW.spec_json
           OR OLD.claims_json IS NOT NEW.claims_json
           OR OLD.stdin_hash IS NOT NEW.stdin_hash
           OR OLD.stdin_len IS NOT NEW.stdin_len
           OR OLD.attempt_id IS NOT NEW.attempt_id
           OR OLD.invocation_id IS NOT NEW.invocation_id
           OR OLD.containment_id IS NOT NEW.containment_id
           OR OLD.root_exit_code IS NOT NEW.root_exit_code
           OR OLD.started_ms IS NOT NEW.started_ms
           OR OLD.finished_ms IS NOT NEW.finished_ms
           OR OLD.retry_not_before_ms IS NOT NEW.retry_not_before_ms
           OR OLD.reservation_not_before_ms IS NOT NEW.reservation_not_before_ms
           OR OLD.parent_job_id IS NOT NEW.parent_job_id
           OR OLD.parent_attempt_id IS NOT NEW.parent_attempt_id
           OR OLD.parent_invocation_id IS NOT NEW.parent_invocation_id BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             VALUES ('job_changed', NEW.id, NEW.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER));
         END;
         CREATE TRIGGER jobs_reservation_cleanup
         AFTER UPDATE OF state, cancel_requested ON jobs
         WHEN NEW.state != 'pending' OR NEW.cancel_requested != 0 BEGIN
             DELETE FROM reservations WHERE job_id = NEW.id;
         END;
         {JOBS_TREE_ORDER_INSERT_TRIGGER};
         {JOBS_TREE_ORDER_UPDATE_TRIGGER};
         CREATE TRIGGER cancellation_event_update AFTER UPDATE OF cancel_requested ON jobs
         WHEN OLD.cancel_requested IS NOT NEW.cancel_requested BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             VALUES ('cancellation_requested', NEW.id, NEW.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER));
         END;
         CREATE TRIGGER logs_event_update AFTER UPDATE OF stdout_len, stderr_len ON jobs
         WHEN OLD.stdout_len != NEW.stdout_len OR OLD.stderr_len != NEW.stderr_len BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             VALUES ('log_committed', NEW.id, NEW.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER));
         END;
         CREATE TRIGGER attempts_event_insert AFTER INSERT ON attempts BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'attempt_changed', NEW.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM jobs WHERE jobs.id = NEW.job_id;
         END;
         CREATE TRIGGER attempts_event_update AFTER UPDATE ON attempts BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'attempt_changed', NEW.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM jobs WHERE jobs.id = NEW.job_id;
         END;
         CREATE TRIGGER invocations_event_insert AFTER INSERT ON invocations
         WHEN NEW.state IN ('started', 'exited') BEGIN
             INSERT INTO events(
                 kind, job_id, batch_id, attempt_id, invocation_id, transition, committed_ms
             )
             SELECT 'invocation_changed', attempts.job_id, jobs.batch_id,
                 NEW.attempt_id, NEW.id, NEW.state,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM attempts JOIN jobs ON jobs.id = attempts.job_id
             WHERE attempts.id = NEW.attempt_id;
         END;
         CREATE TRIGGER invocations_event_update AFTER UPDATE OF state ON invocations
         WHEN OLD.state IS NOT NEW.state AND NEW.state IN ('started', 'exited') BEGIN
             INSERT INTO events(
                 kind, job_id, batch_id, attempt_id, invocation_id, transition, committed_ms
             )
             SELECT 'invocation_changed', attempts.job_id, jobs.batch_id,
                 NEW.attempt_id, NEW.id, NEW.state,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM attempts JOIN jobs ON jobs.id = attempts.job_id
             WHERE attempts.id = NEW.attempt_id;
         END;
         CREATE TRIGGER containments_event_insert AFTER INSERT ON containments BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'containment_changed', attempts.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM invocations
             JOIN attempts ON attempts.id = invocations.attempt_id
             JOIN jobs ON jobs.id = attempts.job_id
             WHERE invocations.id = NEW.invocation_id;
         END;
         CREATE TRIGGER containments_event_update AFTER UPDATE ON containments BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'containment_changed', attempts.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM invocations
             JOIN attempts ON attempts.id = invocations.attempt_id
             JOIN jobs ON jobs.id = attempts.job_id
             WHERE invocations.id = NEW.invocation_id;
         END;
         CREATE TRIGGER reservations_event_insert AFTER INSERT ON reservations BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'job_changed', NEW.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM jobs WHERE jobs.id = NEW.job_id;
         END;
         CREATE TRIGGER reservations_event_delete AFTER DELETE ON reservations BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'job_changed', OLD.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM jobs WHERE jobs.id = OLD.job_id;
         END;
         INSERT INTO meta(key, value) VALUES ('store_uuid', '{store_uuid}');
         INSERT INTO meta(key, value) VALUES ('schema_epoch', '{STORE_SCHEMA_EPOCH}');
         INSERT INTO meta(key, value) VALUES ('tree_order_revision', '0');
         {bound_host_meta}
         COMMIT;"
    ))?;
    validate_schema(connection)
}

pub(super) fn validate_schema(connection: &Connection) -> StoreResult<()> {
    for table in [
        "meta",
        "submissions",
        "batches",
        "jobs",
        "attempts",
        "admissions",
        "daemon_generations",
        "invocations",
        "containments",
        "conditions",
        "observations",
        "leases",
        "reservations",
        "dependencies",
        "events",
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::InvalidState(format!(
                "current schema is missing table {table}"
            )));
        }
    }
    for (table, columns) in [
        (
            "submissions",
            &[
                "kind",
                "batch_id",
                "stdin_json",
                "parent_job_id",
                "parent_attempt_id",
                "parent_invocation_id",
                "wait_intent",
                "reject_code",
                "reject_detail",
                "daemon_generation",
            ] as &[_],
        ),
        ("batches", &["submission_id", "accepted_ms"] as &[_]),
        (
            "jobs",
            &[
                "batch_id",
                "batch_member",
                "batch_index",
                "claims_json",
                "stdin_hash",
                "stdin_len",
                "cancel_requested",
                "retry_not_before_ms",
                "reservation_not_before_ms",
                "parent_job_id",
                "parent_attempt_id",
                "parent_invocation_id",
                "resolved_child_policy_json",
                "managed_policy_admission_json",
            ] as &[_],
        ),
        (
            "dependencies",
            &["predecessor_id", "successor_id", "kind"] as &[_],
        ),
        (
            "attempts",
            &[
                "attempt_index",
                "safety_reason",
                "created_ms",
                "started_ms",
                "deadline_ms",
                "finished_ms",
                "primary_result_json",
            ] as &[_],
        ),
        (
            "admissions",
            &[
                "attempt_id",
                "admitting_started_ms",
                "wall_deadline_ms",
                "quiet_consumed_ms",
                "last_eval_monotonic_ms",
                "last_eval_generation",
                "quiet_generation",
                "quiet_first_monotonic_ms",
                "quiet_last_monotonic_ms",
                "deferral_count",
                "retry_not_before_ms",
                "last_blockers_json",
                "last_eval_unix_ms",
                "last_evidence_json",
                "reservation_generation",
                "reservation_evidence_json",
                "release_evidence_json",
                "gpu_uuid",
                "gpu_driver_version",
            ] as &[_],
        ),
        (
            "daemon_generations",
            &["generation", "process_identity_json", "started_ms"] as &[_],
        ),
        (
            "invocations",
            &[
                "role_index",
                "postcondition_index",
                "daemon_generation",
                "root_host_id",
                "root_boot_id",
                "root_creation_filetime_100ns",
                "exited_ms",
                "exit_classification",
                "stdout_tail",
                "stderr_tail",
            ] as &[_],
        ),
        (
            "containments",
            &[
                "host_id",
                "boot_id",
                "daemon_generation",
                "strength",
                "version",
                "incident_sequence",
                "reason_code",
                "detail",
                "opened_ms",
                "retained_claims_json",
                "resolution",
                "resolved_ms",
                "last_reconciliation",
                "resolution_audit_json",
            ] as &[_],
        ),
        (
            "reservations",
            &[
                "id",
                "job_id",
                "claims_json",
                "created_ms",
                "hold_deadline_ms",
            ] as &[_],
        ),
        (
            "events",
            &[
                "sequence",
                "kind",
                "job_id",
                "batch_id",
                "attempt_id",
                "invocation_id",
                "transition",
                "committed_ms",
            ] as &[_],
        ),
    ] {
        let present = table_columns(connection, table)?;
        for column in columns {
            if !present.contains(*column) {
                return Err(StoreError::InvalidState(format!(
                    "current schema table {table} is missing column {column}"
                )));
            }
        }
    }
    for trigger in [
        "events_prune",
        "jobs_event_insert",
        "jobs_event_update",
        "jobs_reservation_cleanup",
        "jobs_tree_order_insert",
        "jobs_tree_order_update",
        "cancellation_event_update",
        "logs_event_update",
        "attempts_event_insert",
        "attempts_event_update",
        "invocations_event_insert",
        "invocations_event_update",
        "containments_event_insert",
        "containments_event_update",
        "reservations_event_insert",
        "reservations_event_delete",
    ] {
        let exists = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = ?1)",
            [trigger],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Err(StoreError::InvalidState(format!(
                "current schema is missing trigger {trigger}"
            )));
        }
    }
    for index in [
        "jobs_parent_accepted",
        "jobs_state_accepted",
        "jobs_accepted_order",
        "reservations_hold_deadline",
    ] {
        let exists = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1)",
            [index],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Err(StoreError::InvalidState(format!(
                "current schema is missing index {index}"
            )));
        }
    }
    for (name, expected) in [
        ("jobs_tree_order_insert", JOBS_TREE_ORDER_INSERT_TRIGGER),
        ("jobs_tree_order_update", JOBS_TREE_ORDER_UPDATE_TRIGGER),
    ] {
        let actual: String = connection.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
            [name],
            |row| row.get(0),
        )?;
        if normalize_schema_sql(&actual) != normalize_schema_sql(expected) {
            return Err(StoreError::InvalidState(format!(
                "current schema trigger {name} does not match its canonical definition"
            )));
        }
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
