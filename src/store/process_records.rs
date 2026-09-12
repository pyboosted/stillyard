//! Additive typed identities; the historical FILETIME columns remain Windows-only.
use super::*;

pub(super) fn initialize(connection: &Connection) -> StoreResult<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS invocation_process_identities(
            invocation_id TEXT PRIMARY KEY REFERENCES invocations(id),
            identity_json TEXT NOT NULL);",
    )?;
    Ok(())
}

type LegacyColumns = (Option<String>, Option<String>, Option<i64>);

pub(super) fn legacy_columns(identity: &ProcessIdentity, pid: u32) -> StoreResult<LegacyColumns> {
    match identity {
        ProcessIdentity::Linux {
            host_id,
            boot_id,
            pid: actual,
            start_ticks,
            pid_namespace_inode,
            ..
        } if *actual == pid
            && pid != 0
            && *start_ticks != 0
            && *pid_namespace_inode != 0
            && !host_id.0.is_empty()
            && !boot_id.0.is_empty() =>
        {
            Ok((Some(host_id.0.clone()), Some(boot_id.0.clone()), None))
        }
        ProcessIdentity::Linux { .. } => Err(StoreError::InvalidState(
            "invalid exact Linux process identity".into(),
        )),
        _ => {
            let (host, boot, creation) = crate::identity::encode_process_record(identity, pid)
                .map_err(StoreError::InvalidState)?;
            Ok((Some(host.into()), Some(boot.into()), Some(creation)))
        }
    }
}

pub(super) fn record(
    tx: &Transaction<'_>,
    invocation: InvocationId,
    identity: &ProcessIdentity,
    pid: u32,
) -> StoreResult<()> {
    legacy_columns(identity, pid)?;
    let id = invocation.entity_uuid().to_string();
    let encoded = serde_json::to_string(identity)?;
    tx.execute(
        "INSERT OR IGNORE INTO invocation_process_identities VALUES (?1,?2)",
        params![id, encoded],
    )?;
    let existing: String = tx.query_row(
        "SELECT identity_json FROM invocation_process_identities WHERE invocation_id=?1",
        [&id],
        |row| row.get(0),
    )?;
    if existing != encoded {
        return Err(StoreError::InvalidState(
            "Invocation root identity cannot be replaced".into(),
        ));
    }
    Ok(())
}

impl Store {
    pub(super) fn process_identity_record(
        &self,
        invocation: &str,
        pid: Option<u32>,
        host: Option<String>,
        boot: Option<String>,
        creation: Option<i64>,
    ) -> StoreResult<Option<ProcessIdentity>> {
        let encoded: Option<String> = self
            .connection
            .query_row(
                "SELECT identity_json FROM invocation_process_identities WHERE invocation_id=?1",
                [invocation],
                |row| row.get(0),
            )
            .optional()?;
        let Some(encoded) = encoded else {
            return process_identity_from_columns(pid, host, boot, creation);
        };
        let identity: ProcessIdentity = serde_json::from_str(&encoded)?;
        let recorded_pid =
            pid.ok_or_else(|| StoreError::InvalidState("typed root has no recorded PID".into()))?;
        if legacy_columns(&identity, recorded_pid)? != (host, boot, creation) {
            return Err(StoreError::InvalidState(
                "typed root disagrees with Invocation columns".into(),
            ));
        }
        Ok(Some(identity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_linux_identity_never_uses_filetime_and_cannot_be_replaced() {
        let mut c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE invocations(id TEXT PRIMARY KEY);")
            .unwrap();
        initialize(&c).unwrap();
        let id = InvocationId::from_parts(Uuid::now_v7(), Uuid::now_v7());
        c.execute(
            "INSERT INTO invocations VALUES (?1)",
            [id.entity_uuid().to_string()],
        )
        .unwrap();
        let identity = ProcessIdentity::Linux {
            host_id: HostId("host".into()),
            boot_id: BootId("boot".into()),
            pid: 42,
            start_ticks: 17,
            pid_namespace_inode: 19,
            uid: 1000,
        };
        assert_eq!(
            legacy_columns(&identity, 42).unwrap(),
            (Some("host".into()), Some("boot".into()), None)
        );
        assert!(legacy_columns(&identity, 43).is_err());
        let tx = c.transaction().unwrap();
        record(&tx, id, &identity, 42).unwrap();
        record(&tx, id, &identity, 42).unwrap();
        let mut changed = identity.clone();
        if let ProcessIdentity::Linux { start_ticks, .. } = &mut changed {
            *start_ticks += 1;
        }
        assert!(record(&tx, id, &changed, 42).is_err());
        tx.commit().unwrap();
        assert_eq!(
            serde_json::from_str::<ProcessIdentity>(
                &c.query_row(
                    "SELECT identity_json FROM invocation_process_identities",
                    [],
                    |row| row.get::<_, String>(0)
                )
                .unwrap()
            )
            .unwrap(),
            identity
        );
    }
}
