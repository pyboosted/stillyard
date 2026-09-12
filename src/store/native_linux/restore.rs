//! Restore only empty kernel scaffolding; never repair or reset durable history.
use super::*;
use fs2::FileExt;

fn require_quiescent_sql(connection: &Connection) -> StoreResult<()> {
    let busy: bool = connection.query_row(
        "SELECT
        EXISTS(SELECT 1 FROM submissions WHERE state='received') OR
        EXISTS(SELECT 1 FROM jobs WHERE state!='final') OR
        EXISTS(SELECT 1 FROM attempts WHERE state!='settled') OR
        EXISTS(SELECT 1 FROM invocations WHERE state!='resolved') OR
        EXISTS(SELECT 1 FROM leases WHERE state!='released') OR
        EXISTS(SELECT 1 FROM containments WHERE state NOT IN ('empty','cleared')) OR
        EXISTS(SELECT 1 FROM reservations) OR
        EXISTS(SELECT 1 FROM attached_local_mode) OR
        EXISTS(SELECT 1 FROM machine_domains) OR
        EXISTS(SELECT 1 FROM machine_candidates) OR
        EXISTS(SELECT 1 FROM machine_grants) OR
        EXISTS(SELECT 1 FROM machine_reservations)",
        [],
        |r| r.get(0),
    )?;
    if busy {
        return Err(StoreError::InvalidState(
            "native executor restoration requires fully drained standalone history".into(),
        ));
    }
    let version: String = connection.query_row(
        "SELECT value FROM machine_meta WHERE key='schema_version'",
        [],
        |r| r.get(0),
    )?;
    if version != "1" {
        return Err(StoreError::InvalidState(
            "unknown machine schema during native restoration".into(),
        ));
    }
    Ok(())
}

fn open_sql_history(paths: &StorePaths, configuration: &Configuration) -> StoreResult<Connection> {
    let _database = owned(&paths.database, false)?;
    let connection = Connection::open_with_flags(
        &paths.database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    connection.execute_batch("BEGIN")?;
    if !database::schema_is_current(&connection)?
        || database::current_store_uuid(&connection)? != configuration.store_uuid
        || database::meta_value(&connection, "bound_host_id")?.as_deref()
            != Some(configuration.host_id.0.as_str())
    {
        return Err(invalid(
            "native SQL schema, Store or host binding differs; restoration refused without reset",
        )
        .into());
    }
    validate_store(&paths.root, &connection, configuration.store_uuid)?;
    require_quiescent_sql(&connection)?;
    Ok(connection)
}

/// Caller holds the endpoint lease. Keep Store and executor locks until the
/// kernel operation finishes. This path intentionally never constructs a Store.
pub(crate) fn restore_executors(root: &Path, endpoint: &str) -> StoreResult<serde_json::Value> {
    require_native_host()?;
    if std::fs::canonicalize(root)? != root {
        return Err(invalid("native restoration requires a canonical existing Store").into());
    }
    let _root = owned(root, true)?;
    crate::filesystem::require_durable_local_filesystem(root)?;
    let paths = StorePaths::new(root.to_path_buf());
    let lock = owned(&paths.lock, false)?;
    lock.try_lock_exclusive()?;
    let configuration = load(root)?.ok_or_else(|| invalid("native installation is missing"))?;
    let identity = crate::identity::probe_attached_linux_identity();
    if identity.host_id.as_ref() != Some(&configuration.host_id) {
        return Err(invalid("native restoration belongs to another host").into());
    }
    let config_bytes = {
        let mut bytes = Vec::new();
        owned(&paths.config, false)?
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("native configuration is oversized").into());
        }
        bytes
    };
    let config: crate::HostConfig = serde_json::from_slice(&config_bytes)?;
    config.validate().map_err(|e| invalid(&e.to_string()))?;
    let _connection = open_sql_history(&paths, &configuration)?;
    let _authority = owned(&root.join("authority"), true)?;
    let _authority_anchor = owned(&root.join("authority/anchor.json"), false)?;
    let _authority_registry = owned(&root.join("authority/registry.json"), false)?;
    let authority = crate::authority::Authority::validate_native_restore(
        &root.join("authority"),
        &configuration.host_id,
        configuration.store_uuid,
        configuration.domain,
    )?;
    let _executor_lock = owned(&root.join(DIRECTORY).join("executor/executor.lock"), false)?;
    let executor = crate::runner::linux::installed_registry(
        &root.join(DIRECTORY).join("executor"),
        configuration.journal,
        configuration.store_uuid,
        configuration.domain,
        Uuid::now_v7(),
        endpoint.into(),
    )?;
    if executor
        .native_inventory()?
        .iter()
        .any(|record| !record.sealed)
    {
        return Err(invalid(
            "unsealed executor history forbids kernel restoration, even when every path is absent",
        )
        .into());
    }
    let boundary = crate::runner::linux::restore_executor_root(
        &configuration.executor_cgroup,
        config.resources.ram_mb,
    )?;
    Ok(
        serde_json::json!({"store_uuid":configuration.store_uuid,"store_path":root,
        "installation":configuration.installation,"domain":configuration.domain,
        "journal":configuration.journal,"authority_epoch":authority.epoch,
        "executor":boundary,"configuration_sha256":format!("{:x}",Sha256::digest(&config_bytes)),
        "history_preserved":true}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn restore_sql_refuses_missing_corrupt_and_incompatible_history_without_reset() {
        let temp = crate::test_support::durable_tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_owned());
        let configuration = Configuration {
            version: 1,
            installation: Uuid::now_v7(),
            owner_uid: owner(),
            host_id: HostId("restore-fixture".into()),
            store_uuid: Uuid::now_v7(),
            domain: crate::ExecutionDomainId(Uuid::now_v7()),
            journal: Uuid::now_v7(),
            executor_cgroup: "/sys/fs/cgroup/test-only/executors".into(),
        };
        assert!(open_sql_history(&paths, &configuration).is_err());
        assert!(!paths.database.exists());
        std::fs::write(&paths.database, b"not SQLite; retain this corrupt history").unwrap();
        std::fs::set_permissions(&paths.database, std::fs::Permissions::from_mode(0o600)).unwrap();
        let original = std::fs::read(&paths.database).unwrap();
        assert!(open_sql_history(&paths, &configuration).is_err());
        assert_eq!(std::fs::read(&paths.database).unwrap(), original);
        std::fs::remove_file(&paths.database).unwrap();
        let db = Connection::open(&paths.database).unwrap();
        database::create_current_schema(
            &db,
            configuration.store_uuid,
            Some(&configuration.host_id),
        )
        .unwrap();
        db.execute(
            "UPDATE meta SET value='unsupported-restore-epoch' WHERE key='schema_epoch'",
            [],
        )
        .unwrap();
        drop(db);
        std::fs::set_permissions(&paths.database, std::fs::Permissions::from_mode(0o600)).unwrap();
        let original = std::fs::read(&paths.database).unwrap();
        assert!(open_sql_history(&paths, &configuration).is_err());
        assert_eq!(std::fs::read(&paths.database).unwrap(), original);
    }

    #[test]
    fn restore_sql_rejects_received_work_and_unsettled_resource_history() {
        let db = Connection::open_in_memory().unwrap();
        // Deliberately independent orphan rows: each layer must independently
        // refuse restore, including SQL rollback that hid its parent Job.
        for table in [
            "submissions",
            "jobs",
            "attempts",
            "invocations",
            "leases",
            "containments",
            "reservations",
            "attached_local_mode",
            "machine_domains",
            "machine_candidates",
            "machine_grants",
            "machine_reservations",
        ] {
            db.execute_batch(&format!("CREATE TABLE {table}(state TEXT NOT NULL)"))
                .unwrap();
        }
        db.execute_batch("CREATE TABLE machine_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO machine_meta VALUES('schema_version','1')").unwrap();
        require_quiescent_sql(&db).unwrap();
        for (table, state) in [
            ("submissions", "received"),
            ("jobs", "pending"),
            ("attempts", "running"),
            ("invocations", "starting"),
            ("leases", "granted"),
            ("containments", "uncertain"),
            ("reservations", "reserved"),
            ("attached_local_mode", "attached"),
            ("machine_domains", "present"),
            ("machine_candidates", "offered"),
            ("machine_grants", "armed"),
            ("machine_reservations", "reserved"),
        ] {
            db.execute(&format!("INSERT INTO {table} VALUES(?1)"), [state])
                .unwrap();
            assert!(require_quiescent_sql(&db).is_err(), "{table}");
            db.execute(&format!("DELETE FROM {table}"), []).unwrap();
        }
        require_quiescent_sql(&db).unwrap();
    }
}
