use std::collections::{HashMap, HashSet};
use std::time::Instant;

use stillyard::{
    Client, EventCursor, JobChildrenCursor, JobChildrenPage, JobId, JobListPage, JobOutcome,
    JobSelector, JobState, JobSummary, JobTreeNode, JobTreePage, JobTreeRootCursor,
    ObservationFrame, TreeObservationFrame,
};

use super::{App, Bucket};

#[derive(Default)]
pub(super) struct TreeView {
    pub(super) order: Vec<JobId>,
    pub(super) depths: HashMap<JobId, u32>,
    pub(super) parents: HashMap<JobId, JobId>,
    pub(super) children: HashMap<JobId, Vec<JobId>>,
    has_children: HashSet<JobId>,
    incomplete_children: HashSet<JobId>,
    child_outcomes: HashMap<JobId, ChildOutcomeSummary>,
    pub(super) buckets: HashMap<JobId, Bucket>,
    pub(super) context_only: HashSet<JobId>,
    pub(super) orphans: HashSet<JobId>,
    pub(super) expanded: HashSet<JobId>,
    expanded_by_user: HashSet<JobId>,
    collapsed_by_user: HashSet<JobId>,
    pub(super) unavailable: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ChildOutcomeSummary {
    total: usize,
    succeeded: usize,
    failed: usize,
    active: usize,
    incomplete: bool,
}

impl ChildOutcomeSummary {
    fn render(self) -> String {
        let suffix = if self.incomplete { "+" } else { "" };
        let mut outcomes = Vec::new();
        if self.succeeded > 0 {
            outcomes.push(format!("{} ok", self.succeeded));
        }
        if self.failed > 0 {
            outcomes.push(format!("{} failed", self.failed));
        }
        if self.active > 0 {
            outcomes.push(format!("{} active", self.active));
        }
        let noun = if self.total == 1 && !self.incomplete {
            "child"
        } else {
            "children"
        };
        if outcomes.is_empty() {
            format!("{}{suffix} {noun}", self.total)
        } else {
            format!("{}{suffix} {noun}: {}", self.total, outcomes.join(", "))
        }
    }
}

impl TreeView {
    fn inherit_expansion_overrides(&mut self, previous: &mut Self) {
        self.expanded_by_user = std::mem::take(&mut previous.expanded_by_user);
        self.collapsed_by_user = std::mem::take(&mut previous.collapsed_by_user);
        for job_id in &self.collapsed_by_user {
            self.expanded.remove(job_id);
        }
        for job_id in &self.expanded_by_user {
            if self.children.contains_key(job_id) {
                self.expanded.insert(*job_id);
            }
        }
    }

    pub(super) fn collapsed_outcome_summary(&self, job_id: JobId) -> Option<String> {
        (!self.expanded.contains(&job_id))
            .then(|| self.child_outcomes.get(&job_id).copied())
            .flatten()
            .map(ChildOutcomeSummary::render)
    }
}

impl App {
    pub(super) fn tree_hidden(&self, job_id: JobId) -> bool {
        let mut current = self.tree.parents.get(&job_id).copied();
        while let Some(parent) = current {
            if !self.tree.expanded.contains(&parent) {
                return true;
            }
            current = self.tree.parents.get(&parent).copied();
        }
        false
    }

    pub(super) fn bucket_for(&self, job: &JobSummary) -> Bucket {
        self.tree
            .buckets
            .get(&job.job_id)
            .copied()
            .unwrap_or_else(|| Bucket::of(job))
    }

    pub(super) fn collapse_or_parent(&mut self) {
        let Some(job_id) = self.selected_job() else {
            return;
        };
        if self.tree.expanded.remove(&job_id) {
            self.tree.expanded_by_user.remove(&job_id);
            self.tree.collapsed_by_user.insert(job_id);
            return;
        }
        if let Some(parent) = self.tree.parents.get(&job_id).copied()
            && let Some(index) = self.page.jobs.iter().position(|job| job.job_id == parent)
        {
            self.selected = index;
        }
    }

    pub(super) fn expand_or_child(&mut self) {
        let Some(job_id) = self.selected_job() else {
            return;
        };
        if self.tree.children.contains_key(&job_id) && !self.tree.expanded.contains(&job_id) {
            self.tree.expanded.insert(job_id);
            self.tree.collapsed_by_user.remove(&job_id);
            self.tree.expanded_by_user.insert(job_id);
            return;
        }
        if let Some(child) = self
            .tree
            .children
            .get(&job_id)
            .and_then(|children| children.first())
            .copied()
            && let Some(index) = self.page.jobs.iter().position(|job| job.job_id == child)
        {
            self.selected = index;
        }
    }
}

pub(super) fn request_deadline(overall: Instant) -> Instant {
    overall.min(Instant::now() + std::time::Duration::from_secs(2))
}

pub(super) fn observe_for_refresh(
    client: &Client,
    selector: &JobSelector,
    cursor: EventCursor,
    deadline: Instant,
) -> stillyard::Result<ObservationFrame> {
    let JobSelector::Jobs { job_ids } = selector else {
        return client.observe(
            selector.clone(),
            Some(cursor),
            stillyard::MAX_OBSERVATION_PAGE,
            std::time::Duration::from_secs(30),
            deadline,
            None,
        );
    };
    match client.observe_trees(
        stillyard::JobTreeSelector {
            root_job_ids: job_ids.clone(),
        },
        Some(cursor),
        stillyard::MAX_OBSERVATION_PAGE,
        256,
        stillyard::MAX_TREE_PAGE_NODES,
        None,
        std::time::Duration::from_secs(30),
        deadline,
        None,
    )? {
        TreeObservationFrame::Events { events, cursor } => {
            Ok(ObservationFrame::Events { events, cursor })
        }
        TreeObservationFrame::Gap {
            gap,
            snapshot,
            cursor,
        } => Ok(ObservationFrame::Gap {
            gap,
            snapshot: JobListPage::from_jobs(
                snapshot
                    .nodes
                    .into_iter()
                    .map(|node| node.summary)
                    .collect(),
                cursor,
            ),
            cursor,
        }),
        _ => Err(stillyard::Error::Protocol(
            "unknown tree observation frame".into(),
        )),
    }
}

fn replace_page(app: &mut App, page: JobListPage) {
    let selected = app.selected_job();
    app.page = page;
    app.settle_selection(selected);
}

pub(super) fn refresh_page(
    client: &Client,
    app: &mut App,
    selector: &JobSelector,
    limit: u32,
    deadline: Instant,
) -> stillyard::Result<()> {
    let (page, mut tree) = load_tree_page(client, selector.clone(), limit, deadline)?;
    tree.inherit_expansion_overrides(&mut app.tree);
    app.tree = tree;
    replace_page(app, page);
    Ok(())
}

pub(super) fn load_tree_page(
    client: &Client,
    selector: JobSelector,
    limit: u32,
    deadline: Instant,
) -> stillyard::Result<(JobListPage, TreeView)> {
    load_tree_page_from(client, selector, limit, deadline)
}

trait TreePageSource {
    fn roots(
        &self,
        selector: JobSelector,
        cursor: Option<JobTreeRootCursor>,
        deadline: Instant,
    ) -> stillyard::Result<JobTreePage>;

    fn children(
        &self,
        cursor: JobChildrenCursor,
        deadline: Instant,
    ) -> stillyard::Result<JobChildrenPage>;

    fn flat(
        &self,
        selector: JobSelector,
        limit: u32,
        deadline: Instant,
    ) -> stillyard::Result<JobListPage>;
}

impl TreePageSource for Client {
    fn roots(
        &self,
        selector: JobSelector,
        cursor: Option<JobTreeRootCursor>,
        deadline: Instant,
    ) -> stillyard::Result<JobTreePage> {
        Client::tree(
            self,
            selector,
            cursor,
            256,
            stillyard::MAX_TREE_PAGE_NODES,
            None,
            deadline,
            None,
        )
    }

    fn children(
        &self,
        cursor: JobChildrenCursor,
        deadline: Instant,
    ) -> stillyard::Result<JobChildrenPage> {
        Client::tree_children(
            self,
            cursor,
            stillyard::MAX_TREE_PAGE_NODES,
            None,
            deadline,
            None,
        )
    }

    fn flat(
        &self,
        selector: JobSelector,
        limit: u32,
        deadline: Instant,
    ) -> stillyard::Result<JobListPage> {
        Client::list(self, selector, None, limit, deadline, None)
    }
}

fn load_tree_page_from(
    source: &impl TreePageSource,
    selector: JobSelector,
    limit: u32,
    deadline: Instant,
) -> stillyard::Result<(JobListPage, TreeView)> {
    loop {
        if Instant::now() >= deadline {
            return Err(stillyard::Error::DeadlineElapsed);
        }
        match load_tree_page_attempt(source, selector.clone(), limit, deadline) {
            // A root cursor binds the ordered view. Throw away every partial node and immediately
            // restart at page one so generations can never be mixed in one TUI snapshot.
            Err(stillyard::Error::ViewStale { .. }) => continue,
            result => return result,
        }
    }
}

fn load_tree_page_attempt(
    source: &impl TreePageSource,
    selector: JobSelector,
    limit: u32,
    deadline: Instant,
) -> stillyard::Result<(JobListPage, TreeView)> {
    let mut tree = TreeView::default();
    let mut summaries = HashMap::new();
    let mut seen = HashSet::new();
    let mut ingest_order = Vec::new();
    let mut root_order = Vec::new();
    let mut root_cursor = None;
    let mut event_cursor = None;
    let limit_usize = usize::try_from(limit).unwrap_or(1);
    loop {
        let page = match source.roots(selector.clone(), root_cursor, deadline) {
            Ok(page) => page,
            Err(stillyard::Error::ViewUnavailable { detail }) => {
                let flat = source.flat(selector, limit, deadline)?;
                tree.unavailable = Some(detail);
                return Ok((flat, tree));
            }
            Err(error) => return Err(error),
        };
        if event_cursor.is_none() {
            event_cursor = Some(page.event_cursor);
        }
        let mut child_cursors = Vec::new();
        ingest_tree_nodes(
            page.nodes,
            &mut tree,
            &mut summaries,
            &mut seen,
            &mut ingest_order,
            &mut root_order,
            &mut child_cursors,
            limit_usize,
        );
        while let Some(cursor) = pop_next_child_cursor(&mut child_cursors, &tree, &root_order) {
            if seen.len() >= limit_usize {
                break;
            }
            let child_page = match source.children(cursor, deadline) {
                Ok(page) => page,
                Err(stillyard::Error::ViewUnavailable { detail }) => {
                    let flat = source.flat(selector, limit, deadline)?;
                    tree.unavailable = Some(detail);
                    return Ok((flat, tree));
                }
                Err(error) => return Err(error),
            };
            let parent_job_id = child_page.parent_job_id;
            if let Some(cursor) = child_page.next_children_cursor.clone() {
                tree.incomplete_children.insert(parent_job_id);
                child_cursors.push(cursor);
            } else {
                tree.incomplete_children.remove(&parent_job_id);
            }
            ingest_tree_nodes(
                child_page.nodes,
                &mut tree,
                &mut summaries,
                &mut seen,
                &mut ingest_order,
                &mut root_order,
                &mut child_cursors,
                limit_usize,
            );
        }
        if seen.len() >= limit_usize {
            break;
        }
        let Some(next) = page.next_root_cursor else {
            break;
        };
        root_cursor = Some(next);
    }
    for job in summaries.values() {
        if job.state != JobState::Final {
            // Include the active/queued node itself: an active branch with only finished children
            // is still an active branch and must start expanded.
            let mut current = Some(job.job_id);
            while let Some(parent) = current {
                tree.expanded.insert(parent);
                current = tree.parents.get(&parent).copied();
            }
        }
    }
    summarize_child_outcomes(&mut tree, &summaries);
    tree.order = depth_first_order(&tree, &root_order, &ingest_order);
    let jobs = tree
        .order
        .iter()
        .filter_map(|job_id| summaries.get(job_id).cloned())
        .collect();
    Ok((
        JobListPage::from_jobs(
            jobs,
            event_cursor.expect("at least one successful tree page was loaded"),
        ),
        tree,
    ))
}

#[allow(clippy::too_many_arguments)]
fn ingest_tree_nodes(
    nodes: Vec<JobTreeNode>,
    tree: &mut TreeView,
    summaries: &mut HashMap<JobId, JobSummary>,
    seen: &mut HashSet<JobId>,
    ingest_order: &mut Vec<JobId>,
    root_order: &mut Vec<JobId>,
    child_cursors: &mut Vec<JobChildrenCursor>,
    limit: usize,
) {
    for node in nodes {
        if seen.len() >= limit {
            break;
        }
        if let Some(cursor) = node.next_children_cursor.clone() {
            tree.incomplete_children.insert(node.summary.job_id);
            child_cursors.push(cursor);
        }
        let job_id = node.summary.job_id;
        if !seen.insert(job_id) {
            continue;
        }
        if node.has_children {
            tree.has_children.insert(job_id);
        }
        let depth = node
            .summary
            .parent
            .and_then(|parent| tree.depths.get(&parent.job_id).copied())
            .map_or(node.depth, |parent_depth| parent_depth.saturating_add(1));
        tree.depths.insert(job_id, depth);
        if node.context_only {
            tree.context_only.insert(job_id);
        }
        if node.parent_retained == Some(false) {
            tree.orphans.insert(job_id);
        } else if let Some(parent) = node.summary.parent.map(|parent| parent.job_id) {
            tree.parents.insert(job_id, parent);
            tree.children.entry(parent).or_default().push(job_id);
        }
        if node.depth == 0 || !tree.parents.contains_key(&job_id) {
            root_order.push(job_id);
        }
        let bucket = node
            .family_attention
            .map(Bucket::from_attention)
            .or_else(|| {
                node.summary
                    .parent
                    .and_then(|parent| tree.buckets.get(&parent.job_id).copied())
            })
            .unwrap_or_else(|| Bucket::of(&node.summary));
        tree.buckets.insert(job_id, bucket);
        if node.depth == 0 && bucket != Bucket::Finished {
            tree.expanded.insert(job_id);
        }
        ingest_order.push(job_id);
        summaries.insert(job_id, node.summary);
    }
}

fn pop_next_child_cursor(
    cursors: &mut Vec<JobChildrenCursor>,
    tree: &TreeView,
    roots: &[JobId],
) -> Option<JobChildrenCursor> {
    let next = cursors
        .iter()
        .enumerate()
        .min_by_key(|(_, cursor)| continuation_position(tree, roots, cursor.parent_job_id))
        .map(|(index, _)| index)?;
    Some(cursors.remove(next))
}

fn continuation_position(tree: &TreeView, roots: &[JobId], parent: JobId) -> Vec<usize> {
    let mut ancestry = vec![parent];
    let mut current = parent;
    let mut visited = HashSet::new();
    while visited.insert(current) {
        let Some(next) = tree.parents.get(&current).copied() else {
            break;
        };
        ancestry.push(next);
        current = next;
    }
    ancestry.reverse();
    let mut position = Vec::with_capacity(ancestry.len() + 1);
    position.push(
        roots
            .iter()
            .position(|root| *root == ancestry[0])
            .unwrap_or(usize::MAX),
    );
    for edge in ancestry.windows(2) {
        position.push(
            tree.children
                .get(&edge[0])
                .and_then(|children| children.iter().position(|child| *child == edge[1]))
                .unwrap_or(usize::MAX),
        );
    }
    // The continuation is the next direct child, after every direct child already represented.
    position.push(tree.children.get(&parent).map_or(0, Vec::len));
    position
}

fn depth_first_order(tree: &TreeView, roots: &[JobId], ingest_order: &[JobId]) -> Vec<JobId> {
    fn visit(job_id: JobId, tree: &TreeView, seen: &mut HashSet<JobId>, order: &mut Vec<JobId>) {
        if !seen.insert(job_id) {
            return;
        }
        order.push(job_id);
        if let Some(children) = tree.children.get(&job_id) {
            for child in children {
                visit(*child, tree, seen, order);
            }
        }
    }

    let mut seen = HashSet::new();
    let mut order = Vec::with_capacity(ingest_order.len());
    for root in roots {
        visit(*root, tree, &mut seen, &mut order);
    }
    // Fail visibly but safely for malformed relation data instead of dropping a selectable Job.
    for job_id in ingest_order {
        visit(*job_id, tree, &mut seen, &mut order);
    }
    order
}

fn summarize_child_outcomes(tree: &mut TreeView, summaries: &HashMap<JobId, JobSummary>) {
    tree.child_outcomes = tree
        .has_children
        .iter()
        .map(|parent| {
            let children = tree.children.get(parent).map_or(&[][..], Vec::as_slice);
            let mut summary = ChildOutcomeSummary {
                total: children.len(),
                incomplete: children.is_empty() || tree.incomplete_children.contains(parent),
                ..ChildOutcomeSummary::default()
            };
            for child in children {
                let Some(job) = summaries.get(child) else {
                    continue;
                };
                match (job.state, job.outcome) {
                    (JobState::Final, Some(JobOutcome::Succeeded)) => summary.succeeded += 1,
                    (JobState::Final, _) => summary.failed += 1,
                    _ => summary.active += 1,
                }
            }
            (*parent, summary)
        })
        .collect();
}

impl Bucket {
    pub(super) fn from_attention(bucket: stillyard::TreeAttentionBucket) -> Self {
        match bucket {
            stillyard::TreeAttentionBucket::Running => Self::Running,
            stillyard::TreeAttentionBucket::Queued => Self::Queued,
            stillyard::TreeAttentionBucket::Finished => Self::Finished,
            _ => Self::Finished,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::time::Duration;

    enum RootReply {
        Page(JobTreePage),
        Stale,
    }

    struct FakeSource {
        roots: RefCell<VecDeque<RootReply>>,
        children: RefCell<VecDeque<JobChildrenPage>>,
        root_calls: RefCell<Vec<bool>>,
        child_calls: RefCell<Vec<JobId>>,
    }

    impl FakeSource {
        fn new(roots: Vec<RootReply>, children: Vec<JobChildrenPage>) -> Self {
            Self {
                roots: RefCell::new(roots.into()),
                children: RefCell::new(children.into()),
                root_calls: RefCell::default(),
                child_calls: RefCell::default(),
            }
        }
    }

    impl TreePageSource for FakeSource {
        fn roots(
            &self,
            _selector: JobSelector,
            cursor: Option<JobTreeRootCursor>,
            _deadline: Instant,
        ) -> stillyard::Result<JobTreePage> {
            self.root_calls.borrow_mut().push(cursor.is_some());
            match self.roots.borrow_mut().pop_front().expect("root reply") {
                RootReply::Page(page) => Ok(page),
                RootReply::Stale => Err(stillyard::Error::ViewStale {
                    detail: "mutated ordered view".into(),
                }),
            }
        }

        fn children(
            &self,
            cursor: JobChildrenCursor,
            _deadline: Instant,
        ) -> stillyard::Result<JobChildrenPage> {
            self.child_calls.borrow_mut().push(cursor.parent_job_id);
            let page = self.children.borrow_mut().pop_front().expect("child reply");
            assert_eq!(page.parent_job_id, cursor.parent_job_id);
            Ok(page)
        }

        fn flat(
            &self,
            _selector: JobSelector,
            _limit: u32,
            _deadline: Instant,
        ) -> stillyard::Result<JobListPage> {
            panic!("flat fallback was not expected")
        }
    }

    fn job_id() -> JobId {
        serde_json::from_value(serde_json::json!(format!(
            "{}~{}",
            uuid::Uuid::nil(),
            uuid::Uuid::now_v7()
        )))
        .unwrap()
    }

    fn summary(
        job_id: JobId,
        parent: Option<JobId>,
        state: &str,
        outcome: Option<&str>,
    ) -> JobSummary {
        let parent = parent.map(|job_id| {
            serde_json::json!({
                "job_id": job_id,
                "attempt_id": format!("{}~{}", uuid::Uuid::nil(), uuid::Uuid::now_v7()),
                "invocation_id": format!("{}~{}", uuid::Uuid::nil(), uuid::Uuid::now_v7())
            })
        });
        serde_json::from_value(serde_json::json!({
            "job_id": job_id,
            "command_preview": "test",
            "batch_id": null,
            "batch_member": null,
            "parent": parent,
            "state": state,
            "outcome": outcome,
            "accepted_unix_millis": 1,
            "started_unix_millis": null,
            "finished_unix_millis": null,
            "queue_rank": null,
            "estimate": { "confidence": "unknown", "start_in_millis": null, "assumptions": [] },
            "claims": {},
            "labels": [],
            "blocker": null,
            "attempt_id": null,
            "invocation_id": null,
            "stdout_committed": 0,
            "stderr_committed": 0
        }))
        .unwrap()
    }

    fn node(
        summary: JobSummary,
        depth: u32,
        family_attention: Option<&str>,
        has_children: bool,
        next_children_cursor: Option<JobChildrenCursor>,
    ) -> JobTreeNode {
        serde_json::from_value(serde_json::json!({
            "summary": summary,
            "depth": depth,
            "family_attention": family_attention,
            "context_only": false,
            "parent_retained": if depth == 0 { None } else { Some(true) },
            "has_children": has_children,
            "descendants_truncated": next_children_cursor.is_some(),
            "next_children_cursor": next_children_cursor
        }))
        .unwrap()
    }

    fn root_page(
        nodes: Vec<JobTreeNode>,
        next_root_cursor: Option<JobTreeRootCursor>,
    ) -> JobTreePage {
        serde_json::from_value(serde_json::json!({
            "nodes": nodes,
            "next_root_cursor": next_root_cursor,
            "selected_job_id": null,
            "event_cursor": { "store_uuid": uuid::Uuid::nil(), "sequence": 1 }
        }))
        .unwrap()
    }

    fn child_page(
        parent_job_id: JobId,
        nodes: Vec<JobTreeNode>,
        next_children_cursor: Option<JobChildrenCursor>,
    ) -> JobChildrenPage {
        serde_json::from_value(serde_json::json!({
            "parent_job_id": parent_job_id,
            "nodes": nodes,
            "next_children_cursor": next_children_cursor,
            "event_cursor": { "store_uuid": uuid::Uuid::nil(), "sequence": 1 }
        }))
        .unwrap()
    }

    fn child_cursor(parent_job_id: JobId, child_job_id: JobId) -> JobChildrenCursor {
        JobChildrenCursor {
            store_uuid: uuid::Uuid::nil(),
            selector_hash: "test-selector".into(),
            parent_job_id,
            accepted_unix_millis: 1,
            child_job_id,
        }
    }

    #[test]
    fn user_expansion_overrides_survive_refresh() {
        let expanded = job_id();
        let collapsed = job_id();
        let child = job_id();
        let mut previous = TreeView::default();
        previous.expanded_by_user.insert(expanded);
        previous.collapsed_by_user.insert(collapsed);
        let mut refreshed = TreeView::default();
        refreshed.children.insert(expanded, vec![child]);
        refreshed.children.insert(collapsed, vec![child]);
        refreshed.expanded.insert(collapsed);

        refreshed.inherit_expansion_overrides(&mut previous);

        assert!(refreshed.expanded.contains(&expanded));
        assert!(!refreshed.expanded.contains(&collapsed));
    }

    #[test]
    fn continuation_pages_are_linearized_depth_first() {
        let root = job_id();
        let branch = job_id();
        let deep_child = job_id();
        let root_sibling = job_id();
        let root_continuation = child_cursor(root, root_sibling);
        let branch_continuation = child_cursor(branch, deep_child);
        let source = FakeSource::new(
            vec![RootReply::Page(root_page(
                vec![
                    node(
                        summary(root, None, "final", Some("succeeded")),
                        0,
                        Some("finished"),
                        true,
                        Some(root_continuation),
                    ),
                    node(
                        summary(branch, Some(root), "final", Some("succeeded")),
                        1,
                        None,
                        true,
                        Some(branch_continuation),
                    ),
                ],
                None,
            ))],
            vec![
                child_page(
                    branch,
                    vec![node(
                        summary(deep_child, Some(branch), "final", Some("succeeded")),
                        1,
                        None,
                        false,
                        None,
                    )],
                    None,
                ),
                child_page(
                    root,
                    vec![node(
                        summary(root_sibling, Some(root), "final", Some("succeeded")),
                        1,
                        None,
                        false,
                        None,
                    )],
                    None,
                ),
            ],
        );

        let (page, tree) = load_tree_page_from(
            &source,
            JobSelector::All,
            16,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

        assert_eq!(tree.order, vec![root, branch, deep_child, root_sibling]);
        assert_eq!(
            page.jobs.iter().map(|job| job.job_id).collect::<Vec<_>>(),
            tree.order
        );
        assert_eq!(*source.child_calls.borrow(), vec![branch, root]);
    }

    #[test]
    fn sibling_branch_continuations_keep_left_to_right_depth_first_priority() {
        let root = job_id();
        let left = job_id();
        let left_child = job_id();
        let right = job_id();
        let right_child = job_id();
        let source = FakeSource::new(
            vec![RootReply::Page(root_page(
                vec![node(
                    summary(root, None, "final", Some("succeeded")),
                    0,
                    Some("finished"),
                    true,
                    Some(child_cursor(root, left)),
                )],
                None,
            ))],
            vec![
                child_page(
                    root,
                    vec![
                        node(
                            summary(left, Some(root), "final", Some("succeeded")),
                            1,
                            None,
                            true,
                            Some(child_cursor(left, left_child)),
                        ),
                        node(
                            summary(right, Some(root), "final", Some("succeeded")),
                            1,
                            None,
                            true,
                            Some(child_cursor(right, right_child)),
                        ),
                    ],
                    None,
                ),
                child_page(
                    left,
                    vec![node(
                        summary(left_child, Some(left), "final", Some("succeeded")),
                        1,
                        None,
                        false,
                        None,
                    )],
                    None,
                ),
                child_page(
                    right,
                    vec![node(
                        summary(right_child, Some(right), "final", Some("succeeded")),
                        1,
                        None,
                        false,
                        None,
                    )],
                    None,
                ),
            ],
        );

        let (_, tree) = load_tree_page_from(
            &source,
            JobSelector::All,
            16,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

        assert_eq!(tree.order, vec![root, left, left_child, right, right_child]);
        assert_eq!(*source.child_calls.borrow(), vec![root, left, right]);
    }

    #[test]
    fn stale_root_cursor_discards_partial_tree_and_restarts_at_page_one() {
        let stale_root = job_id();
        let fresh_root = job_id();
        let next = JobTreeRootCursor {
            store_uuid: uuid::Uuid::nil(),
            order_revision: 1,
            selector_hash: "test-selector".into(),
            bucket: stillyard::TreeAttentionBucket::Finished,
            accepted_unix_millis: 1,
            root_job_id: stale_root,
        };
        let source = FakeSource::new(
            vec![
                RootReply::Page(root_page(
                    vec![node(
                        summary(stale_root, None, "final", Some("succeeded")),
                        0,
                        Some("finished"),
                        false,
                        None,
                    )],
                    Some(next),
                )),
                RootReply::Stale,
                RootReply::Page(root_page(
                    vec![node(
                        summary(fresh_root, None, "final", Some("succeeded")),
                        0,
                        Some("finished"),
                        false,
                        None,
                    )],
                    None,
                )),
            ],
            Vec::new(),
        );

        let (page, _) = load_tree_page_from(
            &source,
            JobSelector::All,
            16,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

        assert_eq!(
            page.jobs.iter().map(|job| job.job_id).collect::<Vec<_>>(),
            vec![fresh_root]
        );
        assert_eq!(*source.root_calls.borrow(), vec![false, true, false]);
    }

    #[test]
    fn active_branches_expand_and_finished_children_have_outcome_summary() {
        let root = job_id();
        let active = job_id();
        let active_child = job_id();
        let finished = job_id();
        let ok_one = job_id();
        let ok_two = job_id();
        let failed = job_id();
        let source = FakeSource::new(
            vec![RootReply::Page(root_page(
                vec![
                    node(
                        summary(root, None, "final", Some("succeeded")),
                        0,
                        Some("queued"),
                        true,
                        None,
                    ),
                    node(
                        summary(active, Some(root), "pending", None),
                        1,
                        None,
                        true,
                        None,
                    ),
                    node(
                        summary(active_child, Some(active), "final", Some("succeeded")),
                        2,
                        None,
                        false,
                        None,
                    ),
                    node(
                        summary(finished, Some(root), "final", Some("succeeded")),
                        1,
                        None,
                        true,
                        None,
                    ),
                    node(
                        summary(ok_one, Some(finished), "final", Some("succeeded")),
                        2,
                        None,
                        false,
                        None,
                    ),
                    node(
                        summary(ok_two, Some(finished), "final", Some("succeeded")),
                        2,
                        None,
                        false,
                        None,
                    ),
                    node(
                        summary(failed, Some(finished), "final", Some("failed")),
                        2,
                        None,
                        false,
                        None,
                    ),
                ],
                None,
            ))],
            Vec::new(),
        );

        let (_, tree) = load_tree_page_from(
            &source,
            JobSelector::All,
            16,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

        assert!(tree.expanded.contains(&root));
        assert!(tree.expanded.contains(&active));
        assert!(!tree.expanded.contains(&finished));
        assert_eq!(
            tree.collapsed_outcome_summary(finished).as_deref(),
            Some("3 children: 2 ok, 1 failed")
        );
    }
}
