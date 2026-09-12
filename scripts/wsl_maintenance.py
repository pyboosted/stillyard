"""Read-only SQL/retained-journal cross-check for WSL maintenance barriers."""
import hashlib
import json
import uuid


def boundary_digest(record):
    fields = ['containment', 'lease', 'daemon_generation', 'creator', 'boundary',
              'root', 'executable_sha256']
    return hashlib.sha256(json.dumps([record[key] for key in fields],
                                    ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def sealed_pending_leases(db, store_uuid, records):
    """Permit binary repair, never resource release, for fully sealed final work.

    Intentionally supports only work Leases with consumed Tickets. Probe or
    interrupted prelaunch recovery must use the normal daemon recovery path.
    """
    retained = []
    for lease, attempt, invocation in db.execute(
            "select id,attempt_id,invocation_id from leases where state='granted'"):
        plans = db.execute("""select p.job_id,p.attempt_id,p.allocation_key,
                p.armed,p.committed,p.release_pending,p.released,j.state
                from attached_local_plans p join jobs j on j.id=p.job_id
                where p.lease_id=?""", (lease,)).fetchall()
        if (invocation is not None or len(plans) != 1 or plans[0][1] != attempt
                or plans[0][3:] != (1, 1, 1, 0, 'final')):
            raise RuntimeError('retained Lease is not final sealed-release-pending work: ' + lease)
        allocation = json.loads(plans[0][2])
        if allocation['manager_store_uuid'] != store_uuid or allocation['lease_id'] != lease:
            raise RuntimeError('retained Lease allocation identity differs')
        invocations = db.execute("""select i.id,i.state,c.id,c.state from invocations i
                left join containments c on c.invocation_id=i.id
                where i.attempt_id=? and i.role in ('primary','postcondition')""", (attempt,)).fetchall()
        keys = {store_uuid + '~' + row[0] for row in invocations}
        journal_keys = {key for key, record in records.items() if record['lease'] == lease}
        tickets = db.execute("select invocation_id,ticket_json,consumed,cleanup_json from attached_tickets where allocation_key=?",
                             (plans[0][2],)).fetchall()
        if (not keys or len(keys) != len(invocations) or keys != journal_keys
                or keys != {row[0] for row in tickets}):
            raise RuntimeError('retained Lease invocation/journal/Ticket sets differ')
        seals = []
        for inv, state, containment, containment_state in invocations:
            key = store_uuid + '~' + inv
            try:
                record = records[key]
                seal = record['seal']
                boundary = boundary_digest(record)
                proof = hashlib.sha256(json.dumps(seal, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
                local = db.execute('select boundary_sha256,proof_sha256 from attached_local_cleanup where invocation_id=?', (key,)).fetchone()
                ticket_row = next(row for row in tickets if row[0] == key)
                ticket, cleanup = json.loads(ticket_row[1]), json.loads(ticket_row[3])
                if (state != 'resolved' or containment_state != 'empty'
                        or record['containment'] != store_uuid + '~' + str(uuid.UUID(containment))
                        or seal['invocation'] != key or record['boundary'] is None
                        or seal['boundary_sha256'] != boundary
                        or seal['possibly_released'] is not True
                        or uuid.UUID(seal['seal_id']).int == 0
                        or local != (boundary, proof) or ticket_row[2] != 1
                        or record['release_intent'] != ticket or ticket['key'] != allocation
                        or ticket['intent']['invocation_id'] != key
                        or ticket['intent']['containment_id'] != record['containment']
                        or ticket['intent']['boundary_sha256'] != boundary
                        or cleanup != {'invocation_id': key,
                            'release_sequence': ticket['intent']['release_sequence'],
                            'boundary_sha256': boundary, 'proof_sha256': proof,
                            'user_code_released': True}):
                    raise ValueError('retained work lacks matching actual cleanup')
            except (KeyError, TypeError, ValueError, StopIteration) as error:
                raise RuntimeError('retained Lease lacks matching seal: ' + key) from error
            seals.append({'invocation': key, 'seal_id': seal['seal_id'], 'proof_sha256': proof})
        retained.append({'lease_id': lease, 'job_id': plans[0][0], 'seals': seals})
    return retained


def sql_barrier(db, store_uuid, records, *, allow_sealed_release_pending=False):
    """Require an admission transaction, no Lease, and actual seals for cleared rows.

    Caller must separately verify the journal checksum/ownership and recursive
    kernel emptiness. This never edits a row or adopts forced risk acceptance.
    """
    if not db.in_transaction:
        raise RuntimeError('maintenance requires an admission transaction')
    uuid.UUID(store_uuid)
    count = db.execute("select count(*) from leases where state='granted'").fetchone()[0]
    if count and not allow_sealed_release_pending:
        raise RuntimeError('outstanding local Lease prevents maintenance')
    retained = sealed_pending_leases(db, store_uuid, records) if count else []
    cleared = []
    for containment, invocation, state, resolution, encoded in db.execute(
            "select id,invocation_id,state,resolution,resolution_audit_json from containments where state!='empty'"):
        try:
            audit = json.loads(encoded)
            if (state != 'cleared' or resolution != 'proven_empty'
                    or audit['resolution'] != 'proven_empty'
                    or audit['last_reconciliation'] != 'proven_empty'
                    or audit['origin'] != 'automatic' or audit['forced'] is not None
                    or audit['lease_released'] is not True
                    or not isinstance(audit['resolved_unix_millis'], int)
                    or audit['resolved_unix_millis'] <= 0):
                raise ValueError('not an automatic proven-empty clearance')
            uuid.UUID(audit['daemon_generation'])
            key = store_uuid + '~' + str(uuid.UUID(invocation))
            record = records[key]
            seal = record['seal']
            if (record['containment'] != store_uuid + '~' + str(uuid.UUID(containment))
                    or seal['invocation'] != key or record['boundary'] is None
                    or seal['boundary_sha256'] != boundary_digest(record)
                    or not isinstance(seal['possibly_released'], bool)
                    or seal['possibly_released'] != (record['release_intent'] is not None)
                    or uuid.UUID(seal['seal_id']).int == 0):
                raise ValueError('clearance lacks its matching executor seal')
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError('outstanding or unproven containment prevents maintenance: ' + containment) from error
        cleared.append({'containment': containment, 'invocation': key,
                        'resolution': resolution, 'seal_id': seal['seal_id'],
                        'boundary_sha256': seal['boundary_sha256']})
    return {'granted_leases': count, 'sealed_release_pending': retained, 'blocking_containments': 0,
            'historical_proven_empty': cleared}
