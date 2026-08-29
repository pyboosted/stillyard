use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

use stillyard::{
    Client, EventCursor, JobId, JobListPage, JobSelector, JobState, JobSummary, JobTreeNode,
    ObservationFrame, TreeObservationFrame,
};

use super::{App, Bucket};

#[derive(Default)]
pub(super) struct TreeView {
    pub(super) order: Vec<JobId>,
    pub(super) depths: HashMap<JobId, u32>,
    pub(super) parents: HashMap<JobId, JobId>,
    pub(super) children: HashMap<JobId, Vec<JobId>>,
    pub(super) buckets: HashMap<JobId, Bucket>,
    pub(super) context_only: HashSet<JobId>,
    pub(super) orphans: HashSet<JobId>,
    pub(super) expanded: HashSet<JobId>,
    expanded_by_user: HashSet<JobId>,
    collapsed_by_user: HashSet<JobId>,
    pub(super) unavailable: Option<String>,
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
    let mut tree = TreeView::default();
    let mut jobs = Vec::new();
    let mut seen = HashSet::new();
    let mut root_cursor = None;
    let mut event_cursor = None;
    loop {
        let page = match client.tree(
            selector.clone(),
            root_cursor,
            256,
            stillyard::MAX_TREE_PAGE_NODES,
            None,
            deadline,
            None,
        ) {
            Ok(page) => page,
            Err(stillyard::Error::ViewUnavailable { detail }) => {
                let flat = client.list(selector, None, limit, deadline, None)?;
                tree.unavailable = Some(detail);
                return Ok((flat, tree));
            }
            Err(error) => return Err(error),
        };
        if event_cursor.is_none() {
            event_cursor = Some(page.event_cursor);
        }
        let mut child_cursors = VecDeque::new();
        ingest_tree_nodes(
            page.nodes,
            &mut tree,
            &mut jobs,
            &mut seen,
            &mut child_cursors,
            usize::try_from(limit).unwrap_or(1),
        );
        while let Some(cursor) = child_cursors.pop_front() {
            if jobs.len() >= usize::try_from(limit).unwrap_or(1) {
                break;
            }
            let child_page = client.tree_children(
                cursor,
                stillyard::MAX_TREE_PAGE_NODES,
                None,
                deadline,
                None,
            )?;
            ingest_tree_nodes(
                child_page.nodes,
                &mut tree,
                &mut jobs,
                &mut seen,
                &mut child_cursors,
                usize::try_from(limit).unwrap_or(1),
            );
            if let Some(cursor) = child_page.next_children_cursor {
                child_cursors.push_back(cursor);
            }
        }
        if jobs.len() >= usize::try_from(limit).unwrap_or(1) {
            break;
        }
        let Some(next) = page.next_root_cursor else {
            break;
        };
        root_cursor = Some(next);
    }
    for job in &jobs {
        if job.state != JobState::Final {
            let mut current = job.parent.map(|parent| parent.job_id);
            while let Some(parent) = current {
                tree.expanded.insert(parent);
                current = tree.parents.get(&parent).copied();
            }
        }
    }
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
    jobs: &mut Vec<JobSummary>,
    seen: &mut HashSet<JobId>,
    child_cursors: &mut VecDeque<stillyard::JobChildrenCursor>,
    limit: usize,
) {
    for node in nodes {
        if jobs.len() >= limit {
            break;
        }
        if let Some(cursor) = node.next_children_cursor.clone() {
            child_cursors.push_back(cursor);
        }
        let job_id = node.summary.job_id;
        if !seen.insert(job_id) {
            continue;
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
        tree.order.push(job_id);
        jobs.push(node.summary);
    }
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

    #[test]
    fn user_expansion_overrides_survive_refresh() {
        let job_id = || {
            serde_json::from_value(serde_json::json!(format!(
                "{}~{}",
                uuid::Uuid::nil(),
                uuid::Uuid::now_v7()
            )))
            .unwrap()
        };
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
}
