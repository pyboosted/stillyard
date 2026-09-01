use super::*;

fn submit_job(store: &mut Store, job: &JobSpec) -> (Uuid, JobId) {
    let key = Uuid::now_v7();
    let hash = normalized_payload_hash(job).unwrap();
    let job_id = store.submit(key, &hash, job).unwrap().receipt.job_id;
    (key, job_id)
}

fn finish(store: &mut Store, job: &PreparedJob) {
    store
        .mark_finished(job, Some(0), JobOutcome::Succeeded, "succeeded")
        .unwrap();
}

fn cpu_job(root: &Path, units: u32) -> JobSpec {
    let mut job = spec(root);
    job.resources.cpu_units = Some(units);
    job
}

#[test]
fn priority_is_bounded_neutral_immutable_and_hashed_per_batch_member() {
    let temp = tempfile::tempdir().unwrap();
    let mut neutral = spec(temp.path());
    let mut value = serde_json::to_value(&neutral).unwrap();
    value.as_object_mut().unwrap().remove("priority");
    let omitted: JobSpec = serde_json::from_value(value).unwrap();
    assert_eq!(omitted.priority, crate::NEUTRAL_JOB_PRIORITY);
    omitted.validate().unwrap();

    let neutral_hash = normalized_payload_hash(&neutral).unwrap();
    neutral.priority = 1;
    assert_ne!(neutral_hash, normalized_payload_hash(&neutral).unwrap());
    neutral.priority = crate::MAX_JOB_PRIORITY + 1;
    assert!(matches!(
        neutral.validate(),
        Err(crate::Error::InvalidSpec(detail)) if detail.contains("priority")
    ));

    let mut first = spec(temp.path());
    first.priority = crate::MIN_JOB_PRIORITY;
    let mut second = spec(temp.path());
    second.priority = crate::MAX_JOB_PRIORITY;
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("low", first.clone(), Vec::new()),
            member("high", second, Vec::new()),
        ],
    };
    batch.validate().unwrap();
    let original_hash = normalized_batch_payload_hash(&batch).unwrap();
    let mut changed = batch.clone();
    changed.jobs[0].spec.priority += 1;
    assert_ne!(
        original_hash,
        normalized_batch_payload_hash(&changed).unwrap()
    );

    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let (_, accepted) = submit_job(&mut store, &first);
    first.priority = crate::MAX_JOB_PRIORITY;
    assert_eq!(
        store.status(accepted).unwrap().priority,
        crate::MIN_JOB_PRIORITY,
        "accepted priority is immutable with the retained JobSpec"
    );
}

#[test]
fn effective_priority_ages_monotonically_and_orders_by_original_acceptance() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut earlier = cpu_job(temp.path(), 1);
    earlier.priority = crate::NEUTRAL_JOB_PRIORITY;
    let (_, earlier_id) = submit_job(&mut store, &earlier);
    let mut fresh_high = cpu_job(temp.path(), 1);
    fresh_high.priority = crate::MAX_JOB_PRIORITY;
    let (_, high_id) = submit_job(&mut store, &fresh_high);
    assert_eq!(store.pending_jobs().unwrap()[0], high_id);
    assert_eq!(store.status(high_id).unwrap().queue_rank, Some(1));
    assert_eq!(store.status(earlier_id).unwrap().queue_rank, Some(2));

    let first = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(
        first.job_id, high_id,
        "fresh high priority overtakes neutral"
    );
    assert_eq!(
        store.prepare_next_job().unwrap().unwrap().job_id,
        earlier_id,
        "compatible work may still run after the priority choice"
    );
    assert_eq!(store.status(high_id).unwrap().state, JobState::Active);

    let equal_temp = tempfile::tempdir().unwrap();
    let mut equal_store = Store::open_with_capacities(
        StorePaths::new(equal_temp.path().to_path_buf()),
        capacities(),
    )
    .unwrap();
    let (_, equal_first) = submit_job(&mut equal_store, &cpu_job(equal_temp.path(), 1));
    let (_, equal_second) = submit_job(&mut equal_store, &cpu_job(equal_temp.path(), 1));
    let accepted: i64 = equal_store
        .connection
        .query_row(
            "SELECT accepted_ms FROM jobs WHERE id = ?1",
            [equal_first.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    equal_store
        .connection
        .execute(
            "UPDATE jobs SET accepted_ms = ?2 WHERE id = ?1",
            params![equal_second.entity_uuid().to_string(), accepted],
        )
        .unwrap();
    assert_eq!(equal_store.pending_jobs().unwrap()[0], equal_first);

    let aging_temp = tempfile::tempdir().unwrap();
    let mut aging_store = Store::open_with_capacities(
        StorePaths::new(aging_temp.path().to_path_buf()),
        capacities(),
    )
    .unwrap();
    let mut old_low = cpu_job(aging_temp.path(), 1);
    old_low.priority = crate::MIN_JOB_PRIORITY;
    let (_, old_low_id) = submit_job(&mut aging_store, &old_low);
    let old_acceptance = now_millis().saturating_sub(
        i64::try_from(crate::PRIORITY_AGING_QUANTUM_MILLIS.saturating_mul(7)).unwrap(),
    );
    aging_store
        .connection
        .execute(
            "UPDATE jobs SET accepted_ms = ?2 WHERE id = ?1",
            params![old_low_id.entity_uuid().to_string(), old_acceptance],
        )
        .unwrap();
    for _ in 0..8 {
        let mut high = cpu_job(aging_temp.path(), 1);
        high.priority = crate::MAX_JOB_PRIORITY;
        submit_job(&mut aging_store, &high);
    }
    assert_eq!(
        aging_store.pending_jobs().unwrap()[0],
        old_low_id,
        "seven aging quanta make -3 outrank a continuous stream of fresh +3 Jobs"
    );
    let before_observation = aging_store.event_head().unwrap();
    let snapshot = aging_store.status(old_low_id).unwrap();
    assert!(snapshot.effective_priority.unwrap() > i64::from(crate::MAX_JOB_PRIORITY));
    aging_store.list_jobs(&JobSelector::All, None, 32).unwrap();
    assert_eq!(
        aging_store.event_head().unwrap(),
        before_observation,
        "computing aging and queue rank must not write durable events"
    );

    assert_eq!(
        effective_priority_at(crate::MAX_JOB_PRIORITY, i64::MIN, i64::MAX),
        crate::MAX_EFFECTIVE_PRIORITY
    );
    assert!(
        effective_priority_at(crate::MIN_JOB_PRIORITY, 0, 120_000)
            >= effective_priority_at(crate::MIN_JOB_PRIORITY, 0, 60_000)
    );
}

#[test]
fn retry_and_restart_preserve_acceptance_and_aging_history() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths.clone(), capacities()).unwrap();
    let mut low = cpu_job(temp.path(), 1);
    low.priority = crate::MIN_JOB_PRIORITY;
    low.retry = RetryPolicy {
        max_attempts: 2,
        backoff_seconds: 0,
        retryable: vec!["process_failed".into()],
    };
    let (_, low_id) = submit_job(&mut store, &low);
    let accepted = now_millis().saturating_sub(
        i64::try_from(crate::PRIORITY_AGING_QUANTUM_MILLIS.saturating_mul(7)).unwrap(),
    );
    store
        .connection
        .execute(
            "UPDATE jobs SET accepted_ms = ?2 WHERE id = ?1",
            params![low_id.entity_uuid().to_string(), accepted],
        )
        .unwrap();
    let attempt = store.prepare_job(low_id).unwrap().unwrap();
    store
        .mark_invocation_resolved(&attempt, Some(1), None)
        .unwrap();
    assert!(
        store
            .settle_attempt(&attempt, AttemptVerdict::ProcessFailed)
            .unwrap()
    );
    let after_retry = store.status(low_id).unwrap();
    assert_eq!(after_retry.accepted_unix_millis, accepted);
    let effective = after_retry.effective_priority.unwrap();
    drop(store);

    let mut reopened = Store::open_with_capacities(paths, capacities()).unwrap();
    let after_restart = reopened.status(low_id).unwrap();
    assert_eq!(after_restart.accepted_unix_millis, accepted);
    assert!(after_restart.effective_priority.unwrap() >= effective);
    let mut fresh_high = cpu_job(temp.path(), 1);
    fresh_high.priority = crate::MAX_JOB_PRIORITY;
    submit_job(&mut reopened, &fresh_high);
    assert_eq!(reopened.pending_jobs().unwrap()[0], low_id);
}

#[test]
fn blocked_and_impossible_high_priority_jobs_do_not_head_of_line_block() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let fence = temp.path().join("gpu-provider");
    let mut fence_holder = spec(temp.path());
    fence_holder.resources.exclusive_fences = vec![fence.to_string_lossy().into_owned()];
    let (_, holder_id) = submit_job(&mut store, &fence_holder);
    let holder = store.prepare_job(holder_id).unwrap().unwrap();

    let mut blocked_gpu = spec(temp.path());
    blocked_gpu.priority = crate::MAX_JOB_PRIORITY;
    blocked_gpu.resources.gpu_slots = Some(1);
    blocked_gpu.resources.exclusive_fences = vec![fence.to_string_lossy().into_owned()];
    let (_, blocked_gpu_id) = submit_job(&mut store, &blocked_gpu);
    let mut cpu = cpu_job(temp.path(), 1);
    cpu.priority = crate::MIN_JOB_PRIORITY;
    let (_, cpu_id) = submit_job(&mut store, &cpu);
    let admitted = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(admitted.job_id, cpu_id);
    assert!(store.status(blocked_gpu_id).unwrap().reservation.is_none());
    assert_eq!(store.status(holder_id).unwrap().state, JobState::Active);

    let impossible_temp = tempfile::tempdir().unwrap();
    let mut impossible_store = Store::open_with_capacities(
        StorePaths::new(impossible_temp.path().to_path_buf()),
        capacities(),
    )
    .unwrap();
    let mut impossible = cpu_job(impossible_temp.path(), capacities().cpu_units + 1);
    impossible.priority = crate::MAX_JOB_PRIORITY;
    let (_, impossible_id) = submit_job(&mut impossible_store, &impossible);
    let (_, compatible_id) = submit_job(&mut impossible_store, &cpu_job(impossible_temp.path(), 1));
    assert_eq!(
        impossible_store.prepare_next_job().unwrap().unwrap().job_id,
        compatible_id
    );
    let impossible = impossible_store.status(impossible_id).unwrap();
    assert!(impossible.reservation.is_none());
    assert!(
        impossible
            .blockers
            .iter()
            .any(|blocker| blocker.code == "resource_capacity")
    );
    finish(&mut store, &holder);
}

#[test]
fn reservations_are_full_vector_bounded_observable_and_protect_only_claimed_scalars() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let (_, holder_id) = submit_job(&mut store, &cpu_job(temp.path(), 2));
    let holder = store.prepare_job(holder_id).unwrap().unwrap();

    let mut reserved_job = cpu_job(temp.path(), 3);
    reserved_job
        .resources
        .custom
        .insert("review_slots".into(), 1);
    let (reservation_key, reserved_id) = submit_job(&mut store, &reserved_job);
    let event_before = store.event_head().unwrap();
    assert!(store.prepare_next_job().unwrap().is_none());
    let snapshot = store.status(reserved_id).unwrap();
    let reservation = snapshot.reservation.clone().unwrap();
    assert_eq!(reservation.claims.cpu_units, 3);
    assert_eq!(reservation.claims.custom["review_slots"], 1);
    assert!(reservation.hold_deadline_unix_millis > reservation.created_unix_millis);
    assert!(store.event_head().unwrap().sequence > event_before.sequence);

    let hash = normalized_payload_hash(&reserved_job).unwrap();
    let replay = store
        .submit(reservation_key, &hash, &reserved_job)
        .unwrap()
        .receipt;
    let list = store
        .list_jobs(
            &JobSelector::Jobs {
                job_ids: vec![reserved_id],
            },
            None,
            1,
        )
        .unwrap();
    assert_eq!(replay.priority, snapshot.priority);
    assert_eq!(replay.effective_priority, snapshot.effective_priority);
    assert_eq!(replay.queue_rank, snapshot.queue_rank);
    assert_eq!(replay.accepted_unix_millis, snapshot.accepted_unix_millis);
    assert_eq!(replay.reservation, snapshot.reservation);
    assert_eq!(list.jobs[0].reservation, snapshot.reservation);

    let mut ordinary = cpu_job(temp.path(), 2);
    ordinary.priority = crate::MAX_JOB_PRIORITY;
    let (_, ordinary_id) = submit_job(&mut store, &ordinary);
    let mut orthogonal = spec(temp.path());
    orthogonal.resources.cargo_slots = Some(1);
    let (_, orthogonal_id) = submit_job(&mut store, &orthogonal);
    assert_eq!(
        store.prepare_next_job().unwrap().unwrap().job_id,
        orthogonal_id,
        "a zero CPU claim remains admissible through a CPU reservation"
    );
    assert_eq!(store.status(ordinary_id).unwrap().state, JobState::Pending);

    let resources = store.daemon_status("test").unwrap().resources.unwrap();
    assert_eq!(resources.cpu_units.granted, 2);
    assert_eq!(resources.cpu_units.reserved, 3);
    assert_eq!(resources.custom["review_slots"].reserved, 1);
    assert!(resources.cpu_units.reserved <= resources.cpu_units.capacity);

    finish(&mut store, &holder);
    let converted = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(converted.job_id, reserved_id);
    let converted_snapshot = store.status(reserved_id).unwrap();
    assert_eq!(converted_snapshot.state, JobState::Active);
    assert!(converted_snapshot.reservation.is_none());
    let resources = store.daemon_status("test").unwrap().resources.unwrap();
    assert_eq!(resources.cpu_units.granted, 3);
    assert_eq!(resources.cpu_units.reserved, 0);
}

#[test]
fn reservations_sum_to_capacity_and_non_scalar_changes_release_them() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let (_, holder_id) = submit_job(&mut store, &cpu_job(temp.path(), capacities().cpu_units));
    let holder = store.prepare_job(holder_id).unwrap().unwrap();
    let (_, first) = submit_job(&mut store, &cpu_job(temp.path(), 2));
    let (_, second) = submit_job(&mut store, &cpu_job(temp.path(), 2));
    assert!(store.prepare_next_job().unwrap().is_none());
    assert!(store.status(first).unwrap().reservation.is_some());
    assert!(store.status(second).unwrap().reservation.is_some());
    assert_eq!(
        store
            .daemon_status("test")
            .unwrap()
            .resources
            .unwrap()
            .cpu_units
            .reserved,
        u64::from(capacities().cpu_units)
    );
    finish(&mut store, &holder);
    assert!(
        !store
            .status(first)
            .unwrap()
            .blockers
            .iter()
            .any(|blocker| blocker.code == "resource_busy"),
        "a lower-ranked reservation is not a blocker for the protected first conversion"
    );
    assert_eq!(store.prepare_next_job().unwrap().unwrap().job_id, first);
    assert_eq!(
        store.prepare_next_job().unwrap().unwrap().job_id,
        second,
        "equal effective priority converts reservations in acceptance order"
    );

    let independent_temp = tempfile::tempdir().unwrap();
    let mut independent = Store::open_with_capacities(
        StorePaths::new(independent_temp.path().to_path_buf()),
        capacities(),
    )
    .unwrap();
    let (_, cpu_holder_id) = submit_job(
        &mut independent,
        &cpu_job(independent_temp.path(), capacities().cpu_units),
    );
    let cpu_holder = independent.prepare_job(cpu_holder_id).unwrap().unwrap();
    let mut cargo_holder_spec = spec(independent_temp.path());
    cargo_holder_spec.resources.cargo_slots = Some(1);
    let (_, cargo_holder_id) = submit_job(&mut independent, &cargo_holder_spec);
    let cargo_holder = independent.prepare_job(cargo_holder_id).unwrap().unwrap();
    let mut waiting_cpu = cpu_job(independent_temp.path(), 2);
    waiting_cpu.priority = crate::MAX_JOB_PRIORITY;
    let (_, waiting_cpu_id) = submit_job(&mut independent, &waiting_cpu);
    let mut waiting_cargo = spec(independent_temp.path());
    waiting_cargo.priority = crate::MIN_JOB_PRIORITY;
    waiting_cargo.resources.cargo_slots = Some(1);
    let (_, waiting_cargo_id) = submit_job(&mut independent, &waiting_cargo);
    assert!(independent.prepare_next_job().unwrap().is_none());
    assert!(
        independent
            .status(waiting_cpu_id)
            .unwrap()
            .reservation
            .is_some()
    );
    assert!(
        independent
            .status(waiting_cargo_id)
            .unwrap()
            .reservation
            .is_some()
    );
    finish(&mut independent, &cargo_holder);
    assert_eq!(
        independent.prepare_next_job().unwrap().unwrap().job_id,
        waiting_cargo_id,
        "a higher-ranked non-fitting CPU reservation cannot block independent Cargo conversion"
    );
    finish(&mut independent, &cpu_holder);

    let release_temp = tempfile::tempdir().unwrap();
    let mut release_store = Store::open_with_capacities(
        StorePaths::new(release_temp.path().to_path_buf()),
        capacities(),
    )
    .unwrap();
    let (_, scalar_holder_id) = submit_job(&mut release_store, &cpu_job(release_temp.path(), 2));
    let scalar_holder = release_store
        .prepare_job(scalar_holder_id)
        .unwrap()
        .unwrap();
    let fence = release_temp.path().join("mutable-fence");
    let mut waiting = cpu_job(release_temp.path(), 3);
    waiting.resources.exclusive_fences = vec![fence.to_string_lossy().into_owned()];
    let (_, waiting_id) = submit_job(&mut release_store, &waiting);
    assert!(release_store.prepare_next_job().unwrap().is_none());
    assert!(
        release_store
            .status(waiting_id)
            .unwrap()
            .reservation
            .is_some()
    );

    release_store
        .connection
        .execute(
            "INSERT INTO conditions(id, job_id, state, spec_json)
             VALUES (?1, ?2, 'waiting', ?3)",
            params![
                Uuid::now_v7().to_string(),
                waiting_id.entity_uuid().to_string(),
                r#"{"kind":"not_before","unix_millis":9223372036854775807}"#,
            ],
        )
        .unwrap();
    assert!(release_store.prepare_next_job().unwrap().is_none());
    assert!(
        release_store
            .status(waiting_id)
            .unwrap()
            .reservation
            .is_none(),
        "a changed Condition releases scalar protection immediately"
    );
    release_store
        .connection
        .execute(
            "DELETE FROM conditions WHERE job_id = ?1",
            [waiting_id.entity_uuid().to_string()],
        )
        .unwrap();
    assert!(release_store.prepare_next_job().unwrap().is_none());
    assert!(
        release_store
            .status(waiting_id)
            .unwrap()
            .reservation
            .is_some()
    );

    let mut fence_holder = spec(release_temp.path());
    fence_holder.resources.exclusive_fences = vec![fence.to_string_lossy().into_owned()];
    let (_, fence_holder_id) = submit_job(&mut release_store, &fence_holder);
    let fence_holder = release_store.prepare_job(fence_holder_id).unwrap().unwrap();
    assert!(release_store.prepare_next_job().unwrap().is_none());
    assert!(
        release_store
            .status(waiting_id)
            .unwrap()
            .reservation
            .is_none(),
        "a newly failing fence check releases scalar protection immediately"
    );
    assert_eq!(
        release_store
            .daemon_status("test")
            .unwrap()
            .resources
            .unwrap()
            .cpu_units
            .reserved,
        0
    );
    finish(&mut release_store, &fence_holder);
    finish(&mut release_store, &scalar_holder);
}

#[test]
fn reservation_deadline_survives_restart_expiry_yields_and_cancellation_cleans_up() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths.clone(), capacities()).unwrap();
    let (_, holder_id) = submit_job(&mut store, &cpu_job(temp.path(), 2));
    let holder = store.prepare_job(holder_id).unwrap().unwrap();
    let mut preferred = cpu_job(temp.path(), 3);
    preferred.priority = crate::MAX_JOB_PRIORITY;
    let (_, preferred_id) = submit_job(&mut store, &preferred);
    assert!(store.prepare_next_job().unwrap().is_none());
    let before_restart = store.status(preferred_id).unwrap().reservation.unwrap();
    drop(store);

    let mut reopened = Store::open_with_capacities(paths, capacities()).unwrap();
    let after_restart = reopened.status(preferred_id).unwrap().reservation.unwrap();
    assert_eq!(
        after_restart, before_restart,
        "restart cannot extend a hold"
    );
    let (_, competitor_id) = submit_job(&mut reopened, &cpu_job(temp.path(), 2));
    reopened
        .connection
        .execute(
            "UPDATE reservations SET created_ms = ?2, hold_deadline_ms = ?3 WHERE job_id = ?1",
            params![
                preferred_id.entity_uuid().to_string(),
                now_millis().saturating_sub(10_000),
                now_millis().saturating_sub(1),
            ],
        )
        .unwrap();
    // Recovery may already have released the never-started holder Lease. If it did not, release it
    // through the ordinary lifecycle before testing the post-expiry scheduling choice.
    if reopened.status(holder_id).unwrap().state == JobState::Active {
        finish(&mut reopened, &holder);
    }
    let admitted = reopened.prepare_next_job().unwrap().unwrap();
    assert_eq!(
        admitted.job_id, competitor_id,
        "the expired owner must yield during its durable reservation backoff"
    );
    let preferred_snapshot = reopened.status(preferred_id).unwrap();
    assert!(preferred_snapshot.reservation.is_none());
    assert!(
        preferred_snapshot
            .blockers
            .iter()
            .any(|blocker| blocker.code == "reservation_backoff")
    );

    finish(&mut reopened, &admitted);
    reopened
        .connection
        .execute(
            "UPDATE jobs SET reservation_not_before_ms = ?2 WHERE id = ?1",
            params![preferred_id.entity_uuid().to_string(), now_millis() - 1],
        )
        .unwrap();
    let (_, replacement_holder_id) = submit_job(&mut reopened, &cpu_job(temp.path(), 2));
    let replacement_holder = reopened
        .prepare_job(replacement_holder_id)
        .unwrap()
        .unwrap();
    assert!(reopened.prepare_next_job().unwrap().is_none());
    assert!(reopened.status(preferred_id).unwrap().reservation.is_some());
    reopened.cancel_jobs(&[preferred_id]).unwrap();
    assert!(reopened.status(preferred_id).unwrap().reservation.is_none());
    assert_eq!(
        reopened
            .daemon_status("test")
            .unwrap()
            .resources
            .unwrap()
            .cpu_units
            .reserved,
        0
    );
    finish(&mut reopened, &replacement_holder);
}
