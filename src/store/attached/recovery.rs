//! Couple the authenticated complete inventory to the retained local plans.
use super::*;

pub(super) fn synchronize(tx: &Transaction<'_>) -> StoreResult<()> {
    let grants = {
        let mut statement=tx.prepare("SELECT grant_json FROM attached_grants WHERE json_extract(grant_json,'$.state')!='offered'")?;
        statement
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for json in grants {
        let grant: crate::machine::GrantSnapshot = serde_json::from_str(&json)?;
        let key = serde_json::to_string(&grant.candidate.key)?;
        let plan:Option<(String,bool,bool)>=tx.query_row("SELECT candidate_json,released,release_pending FROM attached_local_plans WHERE allocation_key=?1",
            [&key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((candidate, released, pending)) = plan else {
            return Err(StoreError::InvalidState(
                "imported Grant has no continuous local plan".into(),
            ));
        };
        if candidate != serde_json::to_string(&grant.candidate)? {
            return Err(StoreError::InvalidState(
                "imported Grant differs from its local candidate".into(),
            ));
        }
        match grant.state {
            GrantState::Armed | GrantState::Uncertain => {
                if released {
                    return Err(StoreError::InvalidState(
                        "released local plan reappeared as an outstanding Grant".into(),
                    ));
                }
                tx.execute(
                    "UPDATE attached_local_plans SET armed=1 WHERE allocation_key=?1 AND armed=0",
                    [&key],
                )?;
            }
            GrantState::Released => {
                if grant.sealed_release.is_none() {
                    return Err(StoreError::InvalidState(
                        "imported Release lost its seal".into(),
                    ));
                }
                let retained:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM leases l JOIN attached_local_plans p ON p.lease_id=l.id
                    WHERE p.allocation_key=?1 AND l.state='granted')",[&key],|r|r.get(0))?;
                if retained && !pending {
                    return Err(StoreError::InvalidState(
                        "released remote Grant still backs local execution".into(),
                    ));
                }
                tx.execute(
                    "UPDATE attached_local_plans SET released=1 WHERE allocation_key=?1",
                    [&key],
                )?;
                tx.execute("UPDATE leases SET state='released' WHERE state='granted' AND id IN
                    (SELECT lease_id FROM attached_local_plans WHERE allocation_key=?1 AND release_pending=1)",[&key])?;
                tx.execute("UPDATE attached_local_plans SET slot='retired:'||slot||':'||lease_id WHERE allocation_key=?1 AND slot NOT LIKE 'retired:%'",[&key])?;
            }
            GrantState::Offered | GrantState::Expired => {}
        }
    }
    Ok(())
}

pub(super) fn acknowledge_settled(tx: &Transaction<'_>) -> StoreResult<()> {
    use crate::machine::manager;
    let outstanding:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attached_grants WHERE json_extract(grant_json,'$.state')!='released')",[],|r|r.get(0))?;
    if outstanding {
        return Ok(());
    }
    let mut safe = Vec::new();
    for (operation, command) in manager::recovery::abandoned(tx).map_err(protocol_error)? {
        if let Command::AuthorizeInvocation { intent, .. } = command {
            let resolved: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM invocations WHERE id=?1 AND state='resolved')",
                [intent.invocation_id.entity_uuid().to_string()],
                |r| r.get(0),
            )?;
            if !resolved {
                continue;
            }
        }
        safe.push(operation);
        if safe.len() == 256 {
            break;
        }
    }
    if !safe.is_empty() {
        manager::recovery::acknowledge_abandoned(tx, &safe).map_err(protocol_error)?;
    }
    Ok(())
}
