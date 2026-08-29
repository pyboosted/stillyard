use super::*;
use std::collections::{HashMap, HashSet, VecDeque};

const TREE_SCAN_BUDGET: usize = 16_384;
const TREE_SELECTOR_JOB_LIMIT: usize = 64;
const TREE_RESPONSE_BUDGET_BYTES: usize = 8 * 1024 * 1024;
const TREE_RESPONSE_METADATA_RESERVE_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy)]
struct EmitOutcome {
    emitted: bool,
    page_full: bool,
}

#[derive(Clone)]
struct TreeRow {
    job_id: JobId,
    parent_id: Option<JobId>,
    accepted_ms: i64,
    state: JobState,
    batch_id: Option<BatchId>,
    labels: Vec<crate::Label>,
}

struct TreeModel {
    rows: Vec<TreeRow>,
    by_id: HashMap<JobId, usize>,
    children: HashMap<JobId, Vec<JobId>>,
    roots: Vec<JobId>,
}

struct Selection {
    eligible: HashSet<JobId>,
    visible: HashSet<JobId>,
    selected_content: HashSet<JobId>,
}

impl Store {
    pub(crate) fn tree(
        &self,
        selector: &JobSelector,
        root_cursor: Option<JobTreeRootCursor>,
        root_limit: u32,
        node_limit: u32,
        max_depth: Option<u32>,
    ) -> StoreResult<JobTreePage> {
        self.with_tree_snapshot(|store| {
            store.tree_in_snapshot(selector, root_cursor, root_limit, node_limit, max_depth)
        })
    }

    fn tree_in_snapshot(
        &self,
        selector: &JobSelector,
        root_cursor: Option<JobTreeRootCursor>,
        root_limit: u32,
        node_limit: u32,
        max_depth: Option<u32>,
    ) -> StoreResult<JobTreePage> {
        self.validate_tree_selector(selector)?;
        validate_tree_bounds(root_limit, node_limit, max_depth, 64)?;
        let normalized = normalize_selector(selector);
        let selector_token = selector_token(&normalized)?;
        let revision = self.tree_order_revision()?;
        if let Some(cursor) = &root_cursor {
            if cursor.store_uuid != self.store_uuid
                || cursor.root_job_id.store_uuid() != self.store_uuid
                || cursor.order_revision != revision
                || cursor.selector_hash != selector_token
            {
                return Err(StoreError::ViewStale(
                    "tree root cursor no longer describes the current ordered view".into(),
                ));
            }
        }

        let model = self.load_tree_model()?;
        let selection = select_nodes(&model, &normalized, max_depth.unwrap_or(64))?;
        let ordered = ordered_roots(&model, &selection)?;
        let start = match root_cursor.as_ref() {
            None => 0,
            Some(cursor) => ordered
                .iter()
                .position(|(job_id, bucket)| {
                    *job_id == cursor.root_job_id
                        && *bucket == cursor.bucket
                        && model.row(*job_id).accepted_ms == cursor.accepted_unix_millis
                })
                .map(|index| index + 1)
                .ok_or_else(|| StoreError::ViewStale("tree root cursor row is gone".into()))?,
        };

        let mut nodes = Vec::new();
        let mut encoded_budget = 0_usize;
        let mut emitted_roots = Vec::new();
        let root_limit = usize::try_from(root_limit).unwrap_or(1);
        let node_limit = usize::try_from(node_limit).unwrap_or(1);
        for (root_id, bucket) in ordered.iter().skip(start).take(root_limit) {
            if nodes.len() == node_limit {
                break;
            }
            let outcome = emit_node(
                self,
                &model,
                &selection,
                *root_id,
                0,
                None,
                node_limit,
                &selector_token,
                &mut nodes,
                &mut encoded_budget,
            )?;
            if outcome.emitted {
                emitted_roots.push((*root_id, *bucket));
            }
            if outcome.page_full {
                break;
            }
        }
        let consumed = start + emitted_roots.len();
        let next_root_cursor = if consumed < ordered.len() {
            emitted_roots
                .last()
                .map(|(job_id, bucket)| JobTreeRootCursor {
                    store_uuid: self.store_uuid,
                    order_revision: revision,
                    selector_hash: selector_token.clone(),
                    bucket: *bucket,
                    accepted_unix_millis: model.row(*job_id).accepted_ms,
                    root_job_id: *job_id,
                })
        } else {
            None
        };
        let page = JobTreePage {
            nodes,
            next_root_cursor,
            selected_job_id: selected_job_id(&normalized),
            event_cursor: self.event_head()?,
        };
        ensure_tree_response_budget(&page)?;
        Ok(page)
    }

    pub(crate) fn tree_for_job(
        &self,
        job_id: JobId,
        node_limit: u32,
        max_depth: Option<u32>,
    ) -> StoreResult<JobTreePage> {
        self.with_tree_snapshot(|store| {
            store.tree_for_job_in_snapshot(job_id, node_limit, max_depth)
        })
    }

    fn tree_for_job_in_snapshot(
        &self,
        job_id: JobId,
        node_limit: u32,
        max_depth: Option<u32>,
    ) -> StoreResult<JobTreePage> {
        if job_id.store_uuid() != self.store_uuid {
            return Err(StoreError::NotFound(job_id.to_string()));
        }
        validate_tree_bounds(1, node_limit, max_depth, 64)?;
        let model = self.load_tree_model()?;
        let mut path_count = 1_u32;
        let mut current = job_id;
        let mut seen = HashSet::new();
        while let Some(parent) = model
            .row_checked(current)?
            .parent_id
            .filter(|parent| model.by_id.contains_key(parent))
        {
            if !seen.insert(current) || path_count >= 64 {
                return Err(StoreError::InvalidState(
                    "tree_for_job ancestry contains a cycle or exceeds 64 jobs".into(),
                ));
            }
            path_count += 1;
            current = parent;
        }
        if node_limit < path_count {
            return Err(StoreError::InvalidSpec(format!(
                "tree_node_limit_too_small: requires {path_count} nodes for the retained ancestor path"
            )));
        }
        self.tree_in_snapshot(
            &JobSelector::Jobs {
                job_ids: vec![job_id],
            },
            None,
            1,
            node_limit,
            max_depth,
        )
    }

    pub(crate) fn tree_children(
        &self,
        cursor: &JobChildrenCursor,
        node_limit: u32,
        additional_depth: Option<u32>,
    ) -> StoreResult<JobChildrenPage> {
        self.with_tree_snapshot(|store| {
            store.tree_children_in_snapshot(cursor, node_limit, additional_depth)
        })
    }

    fn tree_children_in_snapshot(
        &self,
        cursor: &JobChildrenCursor,
        node_limit: u32,
        additional_depth: Option<u32>,
    ) -> StoreResult<JobChildrenPage> {
        let additional_depth = additional_depth.unwrap_or(0);
        validate_tree_bounds(1, node_limit, Some(additional_depth), 63)?;
        if cursor.store_uuid != self.store_uuid
            || cursor.parent_job_id.store_uuid() != self.store_uuid
            || cursor.child_job_id.store_uuid() != self.store_uuid
        {
            return Err(StoreError::ViewStale(
                "tree child cursor belongs to another store".into(),
            ));
        }
        let selector = selector_from_token(&cursor.selector_hash)?;
        self.validate_tree_selector(&selector)?;
        let model = self.load_tree_model()?;
        let selection = select_nodes(&model, &selector, 64)?;
        let children = model
            .children
            .get(&cursor.parent_job_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|job_id| selection.eligible.contains(job_id))
            .collect::<Vec<_>>();
        let start = children
            .iter()
            .position(|job_id| {
                *job_id == cursor.child_job_id
                    && model.row(*job_id).accepted_ms == cursor.accepted_unix_millis
            })
            .ok_or_else(|| StoreError::ViewStale("tree child cursor row is gone".into()))?;
        let node_limit = usize::try_from(node_limit).unwrap_or(1);
        let mut nodes = Vec::new();
        let mut encoded_budget = 0_usize;
        let mut next_index = start;
        while next_index < children.len() && nodes.len() < node_limit {
            let outcome = emit_node(
                self,
                &model,
                &selection,
                children[next_index],
                1,
                Some(additional_depth.saturating_add(1)),
                node_limit,
                &cursor.selector_hash,
                &mut nodes,
                &mut encoded_budget,
            )?;
            if !outcome.emitted {
                break;
            }
            next_index += 1;
            if outcome.page_full {
                break;
            }
        }
        let next_children_cursor = children.get(next_index).map(|job_id| {
            child_cursor(
                self.store_uuid,
                &cursor.selector_hash,
                cursor.parent_job_id,
                model.row(*job_id),
            )
        });
        let page = JobChildrenPage {
            parent_job_id: cursor.parent_job_id,
            nodes,
            next_children_cursor,
            event_cursor: self.event_head()?,
        };
        ensure_tree_response_budget(&page)?;
        Ok(page)
    }

    pub(crate) fn observe_trees(
        &self,
        selector: &JobTreeSelector,
        cursor: Option<EventCursor>,
        event_limit: u32,
        root_limit: u32,
        node_limit: u32,
        max_depth: Option<u32>,
    ) -> StoreResult<TreeObservationFrame> {
        self.with_tree_snapshot(|store| {
            store.observe_trees_in_snapshot(
                selector,
                cursor,
                event_limit,
                root_limit,
                node_limit,
                max_depth,
            )
        })
    }

    fn observe_trees_in_snapshot(
        &self,
        selector: &JobTreeSelector,
        cursor: Option<EventCursor>,
        event_limit: u32,
        root_limit: u32,
        node_limit: u32,
        max_depth: Option<u32>,
    ) -> StoreResult<TreeObservationFrame> {
        if selector.root_job_ids.is_empty()
            || selector.root_job_ids.len() > MAX_TREE_SELECTOR_JOBS
            || selector
                .root_job_ids
                .iter()
                .any(|job_id| job_id.store_uuid() != self.store_uuid)
        {
            return Err(StoreError::InvalidSpec(
                "tree observation requires 1..=64 current-store Job IDs".into(),
            ));
        }
        if !(1..=MAX_OBSERVATION_PAGE).contains(&event_limit) {
            return Err(StoreError::InvalidSpec(format!(
                "tree event_limit must be 1..={MAX_OBSERVATION_PAGE}"
            )));
        }
        validate_tree_bounds(root_limit, node_limit, max_depth, 64)?;
        let page_selector = JobSelector::Jobs {
            job_ids: selector.root_job_ids.clone(),
        };
        let head = self.event_head()?;
        let requested = cursor.unwrap_or(EventCursor {
            store_uuid: self.store_uuid,
            sequence: 0,
        });
        let frame = self.observe(&JobSelector::All, cursor, event_limit)?;
        match frame {
            ObservationFrame::Gap { gap, cursor, .. } => Ok(TreeObservationFrame::Gap {
                gap,
                snapshot: self.tree_in_snapshot(
                    &page_selector,
                    None,
                    root_limit,
                    node_limit,
                    max_depth,
                )?,
                cursor,
            }),
            ObservationFrame::Events { events, cursor } => {
                let mut retained_events = Vec::new();
                for event in events {
                    let mut retained = false;
                    for anchor in &selector.root_job_ids {
                        if event.job_id == *anchor
                            || job_descends_from(
                                &self.connection,
                                self.store_uuid,
                                event.job_id,
                                *anchor,
                            )?
                        {
                            retained = true;
                            break;
                        }
                    }
                    if retained {
                        retained_events.push(event);
                    }
                }
                if requested.store_uuid != self.store_uuid {
                    return Ok(TreeObservationFrame::Gap {
                        gap: EventGap {
                            requested,
                            oldest_available: EventCursor {
                                store_uuid: self.store_uuid,
                                sequence: head.sequence,
                            },
                        },
                        snapshot: self.tree_in_snapshot(
                            &page_selector,
                            None,
                            root_limit,
                            node_limit,
                            max_depth,
                        )?,
                        cursor: head,
                    });
                }
                Ok(TreeObservationFrame::Events {
                    events: retained_events,
                    cursor,
                })
            }
        }
    }

    fn load_tree_model(&self) -> StoreResult<TreeModel> {
        let mut statement = self.connection.prepare(
            "SELECT id, parent_job_id, accepted_ms, state, batch_id, spec_json
             FROM jobs ORDER BY accepted_ms, id LIMIT ?1",
        )?;
        let raw = statement
            .query_map([u64::try_from(TREE_SCAN_BUDGET + 1).unwrap()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if raw.len() > TREE_SCAN_BUDGET {
            return Err(StoreError::ViewUnavailable(format!(
                "tree_scan_limit: scanned more than {TREE_SCAN_BUDGET} Job rows"
            )));
        }
        let mut rows = Vec::with_capacity(raw.len());
        for (job, parent, accepted_ms, state, batch, spec) in raw {
            let spec: JobSpec = serde_json::from_str(&spec)?;
            rows.push(TreeRow {
                job_id: JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?),
                parent_id: parent
                    .map(|value| {
                        Uuid::parse_str(&value).map(|uuid| JobId::from_parts(self.store_uuid, uuid))
                    })
                    .transpose()?,
                accepted_ms,
                state: parse_job_state(&state)?,
                batch_id: batch
                    .map(|value| {
                        Uuid::parse_str(&value)
                            .map(|uuid| BatchId::from_parts(self.store_uuid, uuid))
                    })
                    .transpose()?,
                labels: spec.labels,
            });
        }
        let by_id = rows
            .iter()
            .enumerate()
            .map(|(index, row)| (row.job_id, index))
            .collect::<HashMap<_, _>>();
        let mut children = HashMap::<JobId, Vec<JobId>>::new();
        let mut roots = Vec::new();
        for row in &rows {
            match row.parent_id.filter(|parent| by_id.contains_key(parent)) {
                Some(parent) => children.entry(parent).or_default().push(row.job_id),
                None => roots.push(row.job_id),
            }
        }
        for child_rows in children.values_mut() {
            child_rows.sort_by_key(|job_id| {
                let row = &rows[by_id[job_id]];
                (row.accepted_ms, row.job_id)
            });
        }
        let model = TreeModel {
            rows,
            by_id,
            children,
            roots,
        };
        validate_parent_graph(&model)?;
        Ok(model)
    }

    fn tree_order_revision(&self) -> StoreResult<u64> {
        let value: String = self.connection.query_row(
            "SELECT value FROM meta WHERE key = 'tree_order_revision'",
            [],
            |row| row.get(0),
        )?;
        value.parse().map_err(|_| {
            StoreError::InvalidState("tree_order_revision is not an unsigned integer".into())
        })
    }

    fn validate_tree_selector(&self, selector: &JobSelector) -> StoreResult<()> {
        self.validate_selector(selector)?;
        if matches!(selector, JobSelector::Jobs { job_ids } if job_ids.len() > TREE_SELECTOR_JOB_LIMIT)
        {
            return Err(StoreError::InvalidSpec(format!(
                "tree Jobs selector exceeds {TREE_SELECTOR_JOB_LIMIT} anchors"
            )));
        }
        Ok(())
    }

    fn with_tree_snapshot<T>(
        &self,
        action: impl FnOnce(&Self) -> StoreResult<T>,
    ) -> StoreResult<T> {
        self.connection
            .execute_batch("BEGIN DEFERRED TRANSACTION")?;
        let result = action(self);
        let finish =
            self.connection
                .execute_batch(if result.is_ok() { "COMMIT" } else { "ROLLBACK" });
        match (result, finish) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error.into()),
        }
    }
}

fn validate_parent_graph(model: &TreeModel) -> StoreResult<()> {
    for row in &model.rows {
        let mut current = Some(row.job_id);
        let mut visited = HashSet::new();
        while let Some(job_id) = current {
            if !visited.insert(job_id) {
                return Err(StoreError::InvalidState(
                    "tree parent graph contains a cycle".into(),
                ));
            }
            if visited.len() > 64 {
                return Err(StoreError::ViewUnavailable(
                    "tree_scan_limit: parent ancestry exceeds 64 Jobs".into(),
                ));
            }
            current = model
                .by_id
                .get(&job_id)
                .and_then(|index| model.rows[*index].parent_id)
                .filter(|parent| model.by_id.contains_key(parent));
        }
    }
    Ok(())
}

fn ensure_tree_response_budget(value: &impl serde::Serialize) -> StoreResult<()> {
    let encoded = serde_json::to_vec(value)?;
    if encoded.len() > TREE_RESPONSE_BUDGET_BYTES {
        return Err(StoreError::ViewUnavailable(format!(
            "tree_response_limit: encoded response is {} bytes, maximum is {TREE_RESPONSE_BUDGET_BYTES}",
            encoded.len()
        )));
    }
    Ok(())
}

impl TreeModel {
    fn row(&self, job_id: JobId) -> &TreeRow {
        &self.rows[self.by_id[&job_id]]
    }

    fn row_checked(&self, job_id: JobId) -> StoreResult<&TreeRow> {
        self.by_id
            .get(&job_id)
            .map(|index| &self.rows[*index])
            .ok_or_else(|| StoreError::NotFound(job_id.to_string()))
    }
}

fn validate_tree_bounds(
    root_limit: u32,
    node_limit: u32,
    depth: Option<u32>,
    max_depth: u32,
) -> StoreResult<()> {
    if !(1..=256).contains(&root_limit)
        || !(1..=MAX_TREE_PAGE_NODES).contains(&node_limit)
        || depth.is_some_and(|depth| depth > max_depth)
    {
        return Err(StoreError::InvalidSpec("invalid tree page bounds".into()));
    }
    Ok(())
}

fn normalize_selector(selector: &JobSelector) -> JobSelector {
    match selector {
        JobSelector::All => JobSelector::All,
        JobSelector::Jobs { job_ids } => {
            let mut job_ids = job_ids.clone();
            job_ids.sort();
            job_ids.dedup();
            JobSelector::Jobs { job_ids }
        }
        JobSelector::Batch { batch_id } => JobSelector::Batch {
            batch_id: *batch_id,
        },
        JobSelector::Labels { labels } => {
            let mut labels = labels.clone();
            labels.sort();
            labels.dedup();
            JobSelector::Labels { labels }
        }
    }
}

fn selector_token(selector: &JobSelector) -> StoreResult<String> {
    let json = serde_json::to_vec(selector)?;
    let digest = Sha256::digest(&json);
    Ok(format!("{}:{}", hex(&digest), hex(&json)))
}

fn selector_from_token(token: &str) -> StoreResult<JobSelector> {
    let (claimed, encoded) = token
        .split_once(':')
        .ok_or_else(|| StoreError::ViewStale("invalid selector-bound cursor".into()))?;
    let json = unhex(encoded)?;
    if claimed != hex(&Sha256::digest(&json)) {
        return Err(StoreError::ViewStale(
            "selector-bound cursor checksum failed".into(),
        ));
    }
    serde_json::from_slice(&json).map_err(StoreError::Json)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(value: &str) -> StoreResult<Vec<u8>> {
    if value.len() % 2 != 0 {
        return Err(StoreError::ViewStale("invalid cursor encoding".into()));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| StoreError::ViewStale("invalid cursor encoding".into()))
        })
        .collect()
}

fn select_nodes(model: &TreeModel, selector: &JobSelector, depth: u32) -> StoreResult<Selection> {
    let root_ids = model.roots.iter().copied().collect::<HashSet<_>>();
    let anchors = model
        .rows
        .iter()
        .filter(|row| match selector {
            JobSelector::All => root_ids.contains(&row.job_id),
            JobSelector::Jobs { job_ids } => job_ids.contains(&row.job_id),
            JobSelector::Batch { batch_id } => row.batch_id == Some(*batch_id),
            JobSelector::Labels { labels } => labels.iter().all(|label| row.labels.contains(label)),
        })
        .map(|row| row.job_id)
        .collect::<Vec<_>>();
    if let JobSelector::Jobs { job_ids } = selector {
        if job_ids
            .iter()
            .any(|job_id| !model.by_id.contains_key(job_id))
        {
            return Err(StoreError::NotFound(
                "tree selector contains an unknown Job".into(),
            ));
        }
    }
    let mut selected_content = HashSet::new();
    let mut visible = HashSet::new();
    let mut queue = VecDeque::new();
    for anchor in &anchors {
        queue.push_back((*anchor, 0_u32));
    }
    while let Some((job_id, relative_depth)) = queue.pop_front() {
        if relative_depth <= depth {
            visible.insert(job_id);
        }
        if !selected_content.insert(job_id) {
            continue;
        }
        for child in model.children.get(&job_id).into_iter().flatten() {
            queue.push_back((*child, relative_depth + 1));
        }
    }
    let mut eligible = selected_content.clone();
    for anchor in anchors {
        let mut current = Some(anchor);
        let mut seen = HashSet::new();
        while let Some(job_id) = current {
            if !seen.insert(job_id) || seen.len() > 64 {
                return Err(StoreError::InvalidState(
                    "tree selector ancestry contains a cycle or exceeds 64 jobs".into(),
                ));
            }
            eligible.insert(job_id);
            visible.insert(job_id);
            current = model
                .by_id
                .get(&job_id)
                .and_then(|index| model.rows[*index].parent_id)
                .filter(|parent| model.by_id.contains_key(parent));
        }
    }
    Ok(Selection {
        eligible,
        visible,
        selected_content,
    })
}

fn ordered_roots(
    model: &TreeModel,
    selection: &Selection,
) -> StoreResult<Vec<(JobId, TreeAttentionBucket)>> {
    let mut roots = model
        .roots
        .iter()
        .copied()
        .filter(|root| selection.visible.contains(root))
        .map(|root| Ok((root, family_bucket(model, root)?)))
        .collect::<StoreResult<Vec<_>>>()?;
    roots.sort_by(|(left_id, left_bucket), (right_id, right_bucket)| {
        left_bucket.cmp(right_bucket).then_with(|| {
            let left = model.row(*left_id);
            let right = model.row(*right_id);
            right
                .accepted_ms
                .cmp(&left.accepted_ms)
                .then(right.job_id.cmp(&left.job_id))
        })
    });
    Ok(roots)
}

fn family_bucket(model: &TreeModel, root: JobId) -> StoreResult<TreeAttentionBucket> {
    let mut pending = vec![root];
    let mut seen = HashSet::new();
    let mut queued = false;
    while let Some(job_id) = pending.pop() {
        if !seen.insert(job_id) || seen.len() > TREE_SCAN_BUDGET {
            return Err(StoreError::ViewUnavailable(
                "tree_scan_limit while classifying a family".into(),
            ));
        }
        match model.row(job_id).state {
            JobState::Active | JobState::Finalizing => return Ok(TreeAttentionBucket::Running),
            JobState::Pending => queued = true,
            JobState::Final => {}
        }
        pending.extend(model.children.get(&job_id).into_iter().flatten().copied());
    }
    Ok(if queued {
        TreeAttentionBucket::Queued
    } else {
        TreeAttentionBucket::Finished
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_node(
    store: &Store,
    model: &TreeModel,
    selection: &Selection,
    job_id: JobId,
    depth: u32,
    emission_depth_limit: Option<u32>,
    node_limit: usize,
    selector_token: &str,
    nodes: &mut Vec<crate::JobTreeNode>,
    encoded_budget: &mut usize,
) -> StoreResult<EmitOutcome> {
    if nodes.len() >= node_limit {
        return Ok(EmitOutcome {
            emitted: false,
            page_full: true,
        });
    }
    let row = model.row(job_id);
    let eligible_children = model
        .children
        .get(&job_id)
        .into_iter()
        .flatten()
        .copied()
        .filter(|child| selection.eligible.contains(child))
        .collect::<Vec<_>>();
    let node = crate::JobTreeNode {
        summary: store.job_summary(job_id)?,
        depth,
        family_attention: (depth == 0)
            .then(|| family_bucket(model, job_id))
            .transpose()?,
        context_only: !selection.selected_content.contains(&job_id),
        parent_retained: row
            .parent_id
            .map(|parent| model.by_id.contains_key(&parent)),
        has_children: !eligible_children.is_empty(),
        descendants_truncated: false,
        next_children_cursor: None,
    };
    // Reserve enough for the largest mutation still possible on this node (a selector-bound
    // child cursor plus JSON field overhead), then use the exact encoded base node size.
    let node_cost = serde_json::to_vec(&node)?
        .len()
        .saturating_add(selector_token.len())
        .saturating_add(512);
    let budget = TREE_RESPONSE_BUDGET_BYTES - TREE_RESPONSE_METADATA_RESERVE_BYTES;
    if encoded_budget.saturating_add(node_cost) > budget {
        if nodes.is_empty() {
            return Err(StoreError::ViewUnavailable(
                "tree_response_limit: one bounded tree node exceeds the encoded response budget"
                    .into(),
            ));
        }
        return Ok(EmitOutcome {
            emitted: false,
            page_full: true,
        });
    }
    *encoded_budget = encoded_budget.saturating_add(node_cost);
    let node_index = nodes.len();
    nodes.push(node);
    if eligible_children.is_empty() {
        return Ok(EmitOutcome {
            emitted: true,
            page_full: false,
        });
    }
    for (index, child) in eligible_children.iter().enumerate() {
        if !selection.visible.contains(child)
            || emission_depth_limit.is_some_and(|limit| depth >= limit)
        {
            nodes[node_index].descendants_truncated = true;
            nodes[node_index].next_children_cursor = Some(child_cursor(
                store.store_uuid,
                selector_token,
                job_id,
                model.row(*child),
            ));
            return Ok(EmitOutcome {
                emitted: true,
                page_full: false,
            });
        }
        if nodes.len() >= node_limit {
            nodes[node_index].descendants_truncated = true;
            nodes[node_index].next_children_cursor = Some(child_cursor(
                store.store_uuid,
                selector_token,
                job_id,
                model.row(*child),
            ));
            return Ok(EmitOutcome {
                emitted: true,
                page_full: true,
            });
        }
        let outcome = emit_node(
            store,
            model,
            selection,
            *child,
            depth + 1,
            emission_depth_limit,
            node_limit,
            selector_token,
            nodes,
            encoded_budget,
        )?;
        if outcome.page_full {
            let next = if outcome.emitted {
                eligible_children.get(index + 1)
            } else {
                Some(child)
            };
            if let Some(next) = next {
                nodes[node_index].descendants_truncated = true;
                nodes[node_index].next_children_cursor = Some(child_cursor(
                    store.store_uuid,
                    selector_token,
                    job_id,
                    model.row(*next),
                ));
            }
            return Ok(EmitOutcome {
                emitted: true,
                page_full: true,
            });
        }
    }
    Ok(EmitOutcome {
        emitted: true,
        page_full: false,
    })
}

fn child_cursor(
    store_uuid: Uuid,
    selector_token: &str,
    parent_job_id: JobId,
    child: &TreeRow,
) -> JobChildrenCursor {
    JobChildrenCursor {
        store_uuid,
        selector_hash: selector_token.into(),
        parent_job_id,
        accepted_unix_millis: child.accepted_ms,
        child_job_id: child.job_id,
    }
}

fn selected_job_id(selector: &JobSelector) -> Option<JobId> {
    match selector {
        JobSelector::Jobs { job_ids } if job_ids.len() == 1 => job_ids.first().copied(),
        _ => None,
    }
}
