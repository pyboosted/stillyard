//! Operator terminal for `stillyard watch`.
//!
//! One screen combines the attention-grouped queue, selected Job detail, and log tabs.
//! Enter expands a pane, Tab moves focus, and the footer names context-sensitive keys.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::io;
use std::sync::mpsc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout, Margin, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
};
use stillyard::{
    AttemptVerdict, Client, JobId, JobListPage, JobOutcome, JobSelector, JobSnapshot, JobState,
    JobSummary, LogStream, ObservationFrame, ResourceClaims,
};

mod tree;
use tree::{TreeView, load_tree_page, observe_for_refresh, refresh_page, request_deadline};

const LOG_WINDOW_BYTES: usize = 64 * 1024;
/// Label key that names the operator-facing project of a Job (see `AGENTS.md`).
const PROJECT_LABEL: &str = "project";

/// 256-colour palette. Semantic state colours never reuse the accent so a coloured cell
/// always means "state", never "focus".
mod palette {
    use ratatui::style::Color;

    pub const ACCENT: Color = Color::Indexed(111);
    pub const RUNNING: Color = Color::Indexed(81);
    pub const QUEUED: Color = Color::Indexed(179);
    pub const OK: Color = Color::Indexed(114);
    pub const BAD: Color = Color::Indexed(203);
    pub const WARN: Color = Color::Indexed(215);
    pub const MUTED: Color = Color::Indexed(246);
    pub const DIM: Color = Color::Indexed(243);
    pub const DIMMER: Color = Color::Indexed(238);
    pub const BORDER: Color = Color::Indexed(238);
    pub const SELECTION: Color = Color::Indexed(236);
    pub const TEXT: Color = Color::Indexed(253);
    pub const LOG: Color = Color::Indexed(250);
    /// Muted hues that stay legible next to the state colours; a project name hashes to one.
    pub const PROJECTS: [u8; 6] = [141, 211, 180, 176, 110, 223];
}

enum Message {
    Input(Event),
    Observation(stillyard::Result<ObservationFrame>),
    Status(String),
    Heartbeat,
}

struct TerminalRestore;

impl Drop for TerminalRestore {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pane {
    Queue,
    Detail,
    Logs,
}

impl Pane {
    fn next(self) -> Self {
        match self {
            Self::Queue => Self::Detail,
            Self::Detail => Self::Logs,
            Self::Logs => Self::Queue,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Queue => Self::Logs,
            Self::Detail => Self::Queue,
            Self::Logs => Self::Detail,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Bucket {
    Running,
    Queued,
    Finished,
}

impl Bucket {
    fn of(job: &JobSummary) -> Self {
        match job.state {
            JobState::Pending => Self::Queued,
            JobState::Active | JobState::Finalizing => Self::Running,
            JobState::Final => Self::Finished,
            _ => Self::Finished,
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Running => "Running",
            Self::Queued => "Queued",
            Self::Finished => "Finished",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowKind {
    Group {
        bucket: Bucket,
        count: usize,
    },
    /// Index into `App::page.jobs`.
    Job(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Link {
    Connected,
    Reconnecting,
    Stale,
}

struct App {
    page: JobListPage,
    tree: TreeView,
    /// Index into `page.jobs` of the selected Job (stable across re-sorting).
    selected: usize,
    detail: Option<JobSnapshot>,
    detail_job: Option<JobId>,
    stdout_offset: u64,
    stderr_offset: u64,
    stdout: VecDeque<u8>,
    stderr: VecDeque<u8>,
    status: String,
    link: Link,
    last_refresh: Instant,
    focus: Pane,
    fullscreen: bool,
    stream: LogStream,
    follow: bool,
    project_filter: Option<String>,
    queue_offset: usize,
    detail_scroll: usize,
    log_scroll: usize,
    /// Viewport rows of each pane as of the last draw; key handling pages by these.
    queue_view: usize,
    detail_view: usize,
    log_view: usize,
    detail_len: usize,
    log_len: usize,
}

impl App {
    fn selected_job(&self) -> Option<JobId> {
        self.page.jobs.get(self.selected).map(|job| job.job_id)
    }

    fn projects(&self) -> Vec<String> {
        self.page
            .jobs
            .iter()
            .filter_map(project_of)
            .map(str::to_owned)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn job_visible(&self, job: &JobSummary) -> bool {
        self.project_filter
            .as_deref()
            .is_none_or(|filter| project_of(job) == Some(filter))
    }

    /// Job indices in display order: running first, then queued by rank, then finished
    /// newest first.
    fn ordered_jobs(&self) -> Vec<usize> {
        if !self.tree.order.is_empty() {
            let by_id = self
                .page
                .jobs
                .iter()
                .enumerate()
                .map(|(index, job)| (job.job_id, index))
                .collect::<HashMap<_, _>>();
            return self
                .tree
                .order
                .iter()
                .filter(|job_id| !self.tree_hidden(**job_id))
                .filter_map(|job_id| by_id.get(job_id).copied())
                .filter(|index| self.job_visible(&self.page.jobs[*index]))
                .collect();
        }
        let mut order: Vec<usize> = (0..self.page.jobs.len())
            .filter(|&index| self.job_visible(&self.page.jobs[index]))
            .collect();
        order.sort_by_key(|&index| sort_key(&self.page.jobs[index]));
        order
    }

    fn rows(&self) -> Vec<RowKind> {
        let order = self.ordered_jobs();
        let mut rows = Vec::with_capacity(order.len() + 3);
        let mut current: Option<Bucket> = None;
        for &index in &order {
            let bucket = self.bucket_for(&self.page.jobs[index]);
            if current != Some(bucket) {
                current = Some(bucket);
                let count = order
                    .iter()
                    .filter(|&&other| self.bucket_for(&self.page.jobs[other]) == bucket)
                    .count();
                rows.push(RowKind::Group { bucket, count });
            }
            rows.push(RowKind::Job(index));
        }
        rows
    }

    fn select_relative(&mut self, delta: isize) {
        let order = self.ordered_jobs();
        if order.is_empty() {
            return;
        }
        let position = order
            .iter()
            .position(|&index| index == self.selected)
            .unwrap_or(0);
        let target = position.saturating_add_signed(delta).min(order.len() - 1);
        self.selected = order[target];
    }

    fn select_edge(&mut self, last: bool) {
        let order = self.ordered_jobs();
        if let Some(&index) = if last { order.last() } else { order.first() } {
            self.selected = index;
        }
    }

    /// Keeps the selection on a visible Job after the page or the filter changed.
    fn settle_selection(&mut self, previous: Option<JobId>) {
        let by_id = previous
            .and_then(|job_id| self.page.jobs.iter().position(|job| job.job_id == job_id))
            .filter(|&index| self.job_visible(&self.page.jobs[index]));
        self.selected = by_id
            .or_else(|| self.ordered_jobs().first().copied())
            .unwrap_or(0);
    }

    fn cycle_project(&mut self) {
        let projects = self.projects();
        let next = match &self.project_filter {
            None => projects.first().cloned(),
            Some(current) => projects
                .iter()
                .position(|project| project == current)
                .and_then(|position| projects.get(position + 1))
                .cloned(),
        };
        self.project_filter = next;
        self.settle_selection(self.selected_job());
        self.queue_offset = 0;
    }

    fn log_buffer(&self) -> &VecDeque<u8> {
        match self.stream {
            LogStream::Stdout => &self.stdout,
            _ => &self.stderr,
        }
    }

    /// Bytes committed before the retained window of the current stream.
    fn log_window_start(&self) -> u64 {
        let (offset, buffer) = match self.stream {
            LogStream::Stdout => (self.stdout_offset, &self.stdout),
            _ => (self.stderr_offset, &self.stderr),
        };
        offset.saturating_sub(buffer.len() as u64)
    }

    fn scroll_logs(&mut self, delta: isize) {
        self.follow = false;
        self.log_scroll = clamp_scroll(
            self.log_scroll.saturating_add_signed(delta),
            self.log_len,
            self.log_view,
        );
    }

    fn scroll_detail(&mut self, delta: isize) {
        self.detail_scroll = clamp_scroll(
            self.detail_scroll.saturating_add_signed(delta),
            self.detail_len,
            self.detail_view,
        );
    }

    fn switch_stream(&mut self, stream: LogStream) {
        if self.stream != stream {
            self.stream = stream;
            self.log_scroll = 0;
            self.follow = true;
        }
    }
}

fn project_of(job: &JobSummary) -> Option<&str> {
    job.labels
        .iter()
        .find(|label| label.key == PROJECT_LABEL)
        .map(|label| label.value.as_str())
}

fn sort_key(job: &JobSummary) -> (Bucket, i64, i64) {
    match Bucket::of(job) {
        Bucket::Running => (
            Bucket::Running,
            job.started_unix_millis.unwrap_or(job.accepted_unix_millis),
            job.accepted_unix_millis,
        ),
        Bucket::Queued => (
            Bucket::Queued,
            job.queue_rank
                .map_or(i64::MAX, |rank| i64::try_from(rank).unwrap_or(i64::MAX)),
            job.accepted_unix_millis,
        ),
        Bucket::Finished => (
            Bucket::Finished,
            -job.finished_unix_millis.unwrap_or(job.accepted_unix_millis),
            -job.accepted_unix_millis,
        ),
    }
}

fn clamp_scroll(scroll: usize, len: usize, view: usize) -> usize {
    scroll.min(len.saturating_sub(view))
}

pub(crate) fn run(
    client: Client,
    selector: JobSelector,
    limit: u32,
    deadline: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let limit = limit.clamp(1, stillyard::MAX_OBSERVATION_PAGE);
    let (page, tree) =
        load_tree_page(&client, selector.clone(), limit, request_deadline(deadline))?;
    let initial_cursor = page.event_cursor;
    let initial_status = tree.unavailable.as_ref().map_or_else(
        || "connected".to_owned(),
        |reason| format!("TREE VIEW UNAVAILABLE: {reason}"),
    );
    let mut app = App {
        page,
        tree,
        selected: 0,
        detail: None,
        detail_job: None,
        stdout_offset: 0,
        stderr_offset: 0,
        stdout: VecDeque::with_capacity(LOG_WINDOW_BYTES),
        stderr: VecDeque::with_capacity(LOG_WINDOW_BYTES),
        status: initial_status,
        link: Link::Connected,
        last_refresh: Instant::now(),
        focus: Pane::Queue,
        fullscreen: false,
        stream: LogStream::Stdout,
        follow: true,
        project_filter: None,
        queue_offset: 0,
        detail_scroll: 0,
        log_scroll: 0,
        queue_view: 1,
        detail_view: 1,
        log_view: 1,
        detail_len: 0,
        log_len: 0,
    };
    app.settle_selection(None);
    refresh_detail(&client, &mut app, request_deadline(deadline))?;

    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let _restore = TerminalRestore;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    terminal.clear()?;

    let (sender, receiver) = mpsc::sync_channel(1);
    let input_sender = sender.clone();
    std::thread::Builder::new()
        .name("stillyard-watch-input".into())
        .spawn(move || {
            while let Ok(input) = event::read() {
                if input_sender.send(Message::Input(input)).is_err() {
                    break;
                }
            }
        })?;
    let observer = client.clone();
    let refresh_selector = selector.clone();
    std::thread::Builder::new()
        .name("stillyard-watch-events".into())
        .spawn(move || {
            let mut cursor = initial_cursor;
            loop {
                let requested = cursor;
                match observe_for_refresh(&observer, &selector, cursor, deadline) {
                    Ok(frame) => {
                        cursor = frame.cursor();
                        if matches!(
                            &frame,
                            ObservationFrame::Events { events, .. } if events.is_empty()
                        ) && cursor == requested
                        {
                            if sender.send(Message::Heartbeat).is_err() {
                                break;
                            }
                            continue;
                        }
                        if sender.send(Message::Observation(Ok(frame))).is_err() {
                            break;
                        }
                    }
                    Err(stillyard::Error::Unavailable(message)) => {
                        if sender
                            .send(Message::Status(format!(
                                "reconnecting after daemon loss: {message}"
                            )))
                            .is_err()
                        {
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                    Err(stillyard::Error::Io(error)) => {
                        if sender
                            .send(Message::Status(format!(
                                "reconnecting after daemon I/O loss: {error}"
                            )))
                            .is_err()
                        {
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                    Err(error) => {
                        let _ = sender.send(Message::Observation(Err(error)));
                        break;
                    }
                }
            }
        })?;

    loop {
        terminal.draw(|frame| draw(frame, &mut app))?;
        let message = match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
            Ok(message) => message,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if app.last_refresh.elapsed() >= std::time::Duration::from_secs(30) {
                    app.link = Link::Stale;
                    app.status = format!(
                        "stale: no successful refresh for {:.0}s",
                        app.last_refresh.elapsed().as_secs_f64()
                    );
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("watch channels disconnected".into());
            }
        };
        match message {
            Message::Input(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                let mut reselect = false;
                match key.code {
                    KeyCode::Char('q') => return Ok(()),
                    KeyCode::Esc => app.fullscreen = false,
                    KeyCode::Enter => app.fullscreen = !app.fullscreen,
                    KeyCode::Tab => app.focus = app.focus.next(),
                    KeyCode::BackTab => app.focus = app.focus.previous(),
                    KeyCode::Char('p') => {
                        app.cycle_project();
                        reselect = true;
                    }
                    KeyCode::Char('r') => {
                        let refresh_deadline = request_deadline(deadline);
                        match refresh_page(
                            &client,
                            &mut app,
                            &refresh_selector,
                            limit,
                            refresh_deadline,
                        )
                        .and_then(|()| refresh_detail(&client, &mut app, refresh_deadline))
                        {
                            Ok(()) => {
                                app.status = "manually refreshed".into();
                                app.link = Link::Connected;
                                app.last_refresh = Instant::now();
                            }
                            Err(error) => {
                                app.status = format!("stale after refresh error: {error}");
                            }
                        }
                    }
                    code => match app.focus {
                        Pane::Queue => match code {
                            KeyCode::Up | KeyCode::Char('k') => {
                                app.select_relative(-1);
                                reselect = true;
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                app.select_relative(1);
                                reselect = true;
                            }
                            KeyCode::PageUp => {
                                app.select_relative(-(app.queue_view as isize).max(1));
                                reselect = true;
                            }
                            KeyCode::PageDown => {
                                app.select_relative((app.queue_view as isize).max(1));
                                reselect = true;
                            }
                            KeyCode::Home | KeyCode::Char('g') => {
                                app.select_edge(false);
                                reselect = true;
                            }
                            KeyCode::End | KeyCode::Char('G') => {
                                app.select_edge(true);
                                reselect = true;
                            }
                            KeyCode::Left | KeyCode::Char('h') => {
                                app.collapse_or_parent();
                                reselect = true;
                            }
                            KeyCode::Right | KeyCode::Char('l') => {
                                app.expand_or_child();
                                reselect = true;
                            }
                            _ => {}
                        },
                        Pane::Detail => match code {
                            KeyCode::Up | KeyCode::Char('k') => app.scroll_detail(-1),
                            KeyCode::Down | KeyCode::Char('j') => app.scroll_detail(1),
                            KeyCode::PageUp => {
                                app.scroll_detail(-(app.detail_view as isize).max(1));
                            }
                            KeyCode::PageDown => {
                                app.scroll_detail((app.detail_view as isize).max(1));
                            }
                            KeyCode::Home | KeyCode::Char('g') => app.detail_scroll = 0,
                            KeyCode::End | KeyCode::Char('G') => {
                                app.detail_scroll = usize::MAX;
                                app.scroll_detail(0);
                            }
                            _ => {}
                        },
                        Pane::Logs => match code {
                            KeyCode::Up | KeyCode::Char('k') => app.scroll_logs(-1),
                            KeyCode::Down | KeyCode::Char('j') => app.scroll_logs(1),
                            KeyCode::PageUp => {
                                app.scroll_logs(-(app.log_view as isize).max(1));
                            }
                            KeyCode::PageDown => {
                                app.scroll_logs((app.log_view as isize).max(1));
                            }
                            KeyCode::Home | KeyCode::Char('g') => {
                                app.follow = false;
                                app.log_scroll = 0;
                            }
                            KeyCode::End | KeyCode::Char('G') => app.follow = true,
                            KeyCode::Char('f') => app.follow = !app.follow,
                            KeyCode::Left | KeyCode::Char('h') => {
                                app.switch_stream(LogStream::Stdout);
                            }
                            KeyCode::Right | KeyCode::Char('l') => {
                                app.switch_stream(LogStream::Stderr);
                            }
                            _ => {}
                        },
                    },
                }
                if reselect {
                    if let Err(error) =
                        refresh_detail(&client, &mut app, request_deadline(deadline))
                    {
                        app.status = format!("stale after detail error: {error}");
                    } else {
                        app.last_refresh = Instant::now();
                    }
                }
            }
            Message::Input(_) => {}
            Message::Observation(Ok(frame)) => {
                app.status = match &frame {
                    ObservationFrame::Events { events, cursor } => {
                        format!("{} event(s), cursor {cursor}", events.len())
                    }
                    ObservationFrame::Gap { gap, cursor, .. } => format!(
                        "GAP {} -> {}, resynchronized at {cursor}",
                        gap.requested, gap.oldest_available
                    ),
                    _ => "unknown observation frame; refreshed".into(),
                };
                let refresh_deadline = request_deadline(deadline);
                let refresh = match frame {
                    ObservationFrame::Gap { .. } => refresh_page(
                        &client,
                        &mut app,
                        &refresh_selector,
                        limit,
                        refresh_deadline,
                    )
                    .and_then(|()| refresh_detail(&client, &mut app, refresh_deadline)),
                    ObservationFrame::Events { .. } => refresh_page(
                        &client,
                        &mut app,
                        &refresh_selector,
                        limit,
                        refresh_deadline,
                    )
                    .and_then(|()| refresh_detail(&client, &mut app, refresh_deadline)),
                    _ => Ok(()),
                };
                if let Err(error) = refresh {
                    app.status = format!("stale after observation refresh error: {error}");
                } else {
                    app.link = Link::Connected;
                    app.last_refresh = Instant::now();
                    if let Some(reason) = &app.tree.unavailable {
                        app.status = format!("TREE VIEW UNAVAILABLE: {reason}");
                    }
                }
            }
            Message::Observation(Err(error)) => {
                if matches!(
                    error,
                    stillyard::Error::DeadlineElapsed | stillyard::Error::Canceled
                ) {
                    return Err(error.into());
                }
                app.link = Link::Stale;
                app.status = format!("stale: observer stopped: {error}");
            }
            Message::Status(status) => {
                if status.starts_with("reconnecting") {
                    app.link = Link::Reconnecting;
                }
                app.status = status;
            }
            Message::Heartbeat => {
                app.last_refresh = Instant::now();
                if app.link != Link::Connected {
                    app.link = Link::Connected;
                    app.status = "connected; idle".into();
                }
            }
        }
    }
}

fn refresh_detail(client: &Client, app: &mut App, deadline: Instant) -> stillyard::Result<()> {
    let selected = app.selected_job();
    if selected != app.detail_job {
        app.detail_job = selected;
        app.stdout.clear();
        app.stderr.clear();
        app.detail_scroll = 0;
        app.log_scroll = 0;
        app.follow = true;
        if let Some(summary) = app.page.jobs.get(app.selected) {
            app.stdout_offset = summary
                .stdout_committed
                .saturating_sub(LOG_WINDOW_BYTES as u64);
            app.stderr_offset = summary
                .stderr_committed
                .saturating_sub(LOG_WINDOW_BYTES as u64);
        }
    }
    let Some(job_id) = selected else {
        app.detail = None;
        return Ok(());
    };
    app.detail = Some(client.status(job_id, deadline, None)?);
    let stdout_gap = read_log_window(
        client,
        job_id,
        LogStream::Stdout,
        &mut app.stdout_offset,
        &mut app.stdout,
        deadline,
    )?;
    let stderr_gap = read_log_window(
        client,
        job_id,
        LogStream::Stderr,
        &mut app.stderr_offset,
        &mut app.stderr,
        deadline,
    )?;
    if let Some(gap) = stdout_gap.or(stderr_gap) {
        app.status = format!("log GAP resynchronized: {gap}");
    }
    Ok(())
}

fn read_log_window(
    client: &Client,
    job_id: JobId,
    stream: LogStream,
    offset: &mut u64,
    buffer: &mut VecDeque<u8>,
    deadline: Instant,
) -> stillyard::Result<Option<String>> {
    let mut chunk = client.logs(
        job_id,
        stream,
        *offset,
        LOG_WINDOW_BYTES as u32,
        deadline,
        None,
    )?;
    let gap = chunk.gap.clone();
    if let Some(replacement_offset) = gap_resync_offset(
        chunk.offset,
        chunk.next_offset,
        gap.is_some(),
        LOG_WINDOW_BYTES as u64,
    ) {
        *offset = replacement_offset;
        chunk = client.logs(
            job_id,
            stream,
            *offset,
            LOG_WINDOW_BYTES as u32,
            deadline,
            None,
        )?;
    }
    if gap.is_some() {
        buffer.clear();
    }
    buffer.extend(chunk.bytes);
    *offset = chunk.next_offset;
    while buffer.len() > LOG_WINDOW_BYTES {
        buffer.pop_front();
    }
    Ok(gap)
}

// ---------------------------------------------------------------------------------------------
// Time

/// Wall clock captured once per frame: the instant and the local UTC offset.
#[derive(Clone, Copy)]
struct Clock {
    now_millis: i64,
    offset_minutes: i32,
}

impl Clock {
    fn capture() -> Self {
        Self {
            now_millis: unix_millis_now(),
            offset_minutes: local_offset_minutes(),
        }
    }

    fn local(self, millis: i64) -> (i64, u32, u32, u32, u32, u32, u32) {
        utc_components(millis + i64::from(self.offset_minutes) * 60_000)
    }

    /// `HH:MM:SS` for today, `MM-DD HH:MM` otherwise.
    fn compact(self, millis: i64) -> String {
        let (year, month, day, hour, minute, second, _) = self.local(millis);
        let (today_year, today_month, today_day, ..) = self.local(self.now_millis);
        if (year, month, day) == (today_year, today_month, today_day) {
            format!("{hour:02}:{minute:02}:{second:02}")
        } else {
            format!("{month:02}-{day:02} {hour:02}:{minute:02}")
        }
    }

    /// `YYYY-MM-DD HH:MM:SS.mmm`.
    fn exact(self, millis: i64) -> String {
        let (year, month, day, hour, minute, second, fraction) = self.local(millis);
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{fraction:03}")
    }

    /// `HH:MM:SS.mmm`, for timestamps that share the date of an earlier `exact` one.
    fn precise(self, millis: i64) -> String {
        let (_, _, _, hour, minute, second, fraction) = self.local(millis);
        format!("{hour:02}:{minute:02}:{second:02}.{fraction:03}")
    }

    fn offset_label(self) -> String {
        let sign = if self.offset_minutes < 0 { '-' } else { '+' };
        let magnitude = self.offset_minutes.unsigned_abs();
        format!("UTC{sign}{:02}:{:02}", magnitude / 60, magnitude % 60)
    }
}

fn unix_millis_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(windows)]
fn local_offset_minutes() -> i32 {
    use windows_sys::Win32::Foundation::SYSTEMTIME;
    use windows_sys::Win32::System::SystemInformation::{GetLocalTime, GetSystemTime};

    // SAFETY: both calls only write into the zero-initialised SYSTEMTIME values we own.
    let (utc, local) = unsafe {
        let mut utc: SYSTEMTIME = std::mem::zeroed();
        let mut local: SYSTEMTIME = std::mem::zeroed();
        GetSystemTime(&raw mut utc);
        GetLocalTime(&raw mut local);
        (utc, local)
    };
    let minutes = |time: &SYSTEMTIME| {
        days_from_civil(
            i64::from(time.wYear),
            u32::from(time.wMonth),
            u32::from(time.wDay),
        ) * 1_440
            + i64::from(time.wHour) * 60
            + i64::from(time.wMinute)
    };
    i32::try_from(minutes(&local) - minutes(&utc)).unwrap_or(0)
}

#[cfg(not(windows))]
fn local_offset_minutes() -> i32 {
    0
}

fn utc_components(millis: i64) -> (i64, u32, u32, u32, u32, u32, u32) {
    let seconds = millis.div_euclid(1000);
    let fraction = millis.rem_euclid(1000) as u32;
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_date_from_unix_days(days);
    (
        year,
        month,
        day,
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60,
        fraction,
    )
}

fn civil_date_from_unix_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month as u32, day as u32)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_prime = i64::from((month + 9) % 12);
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn format_duration(millis: u64) -> String {
    let seconds = millis / 1000;
    if seconds < 60 {
        format!("{:.1}s", millis as f64 / 1000.0)
    } else if seconds < 3_600 {
        format!("{}m{:02}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h{:02}m", seconds / 3_600, (seconds % 3_600) / 60)
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1_024 {
        format!("{bytes} B")
    } else if bytes < 1_024 * 1_024 {
        format!("{:.1}K", bytes as f64 / 1_024.0)
    } else {
        format!("{:.1}M", bytes as f64 / (1_024.0 * 1_024.0))
    }
}

// ---------------------------------------------------------------------------------------------
// Text helpers

/// Truncates to `width` characters, ending with `…` when something was cut.
fn ellipsize(text: &str, width: usize) -> String {
    let count = text.chars().count();
    if count <= width {
        return text.to_owned();
    }
    if width == 0 {
        return String::new();
    }
    let mut cut: String = text.chars().take(width - 1).collect();
    cut.push('…');
    cut
}

fn pad_right(text: &str, width: usize) -> String {
    let mut padded = ellipsize(text, width);
    let count = padded.chars().count();
    padded.extend(std::iter::repeat_n(' ', width.saturating_sub(count)));
    padded
}

fn pad_left(text: &str, width: usize) -> String {
    let cut = ellipsize(text, width);
    let count = cut.chars().count();
    let mut padded: String = std::iter::repeat_n(' ', width.saturating_sub(count)).collect();
    padded.push_str(&cut);
    padded
}

fn span_width(spans: &[Span<'_>]) -> usize {
    spans.iter().map(|span| span.content.chars().count()).sum()
}

/// Character-level wrapping of styled spans; continuation lines are indented by `indent`.
fn wrap_spans(spans: &[Span<'static>], width: usize, indent: usize) -> Vec<Line<'static>> {
    let width = width.max(1);
    let indent = indent.min(width - 1);
    let mut lines = Vec::new();
    let mut current: Vec<Span<'static>> = Vec::new();
    let mut column = 0;
    for span in spans {
        let mut fragment = String::new();
        for character in span.content.chars() {
            if column == width {
                if !fragment.is_empty() {
                    current.push(Span::styled(std::mem::take(&mut fragment), span.style));
                }
                lines.push(Line::from(std::mem::take(&mut current)));
                if indent > 0 {
                    current.push(Span::raw(" ".repeat(indent)));
                }
                column = indent;
            }
            fragment.push(character);
            column += 1;
        }
        if !fragment.is_empty() {
            current.push(Span::styled(fragment, span.style));
        }
    }
    if !current.is_empty() || lines.is_empty() {
        lines.push(Line::from(current));
    }
    lines
}

/// Log bytes for display: ANSI escape sequences dropped, other control characters marked.
fn terminal_text(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut output = String::with_capacity(text.len());
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '\x1b' => {
                if characters.peek() == Some(&'[') {
                    characters.next();
                    for parameter in characters.by_ref() {
                        if ('\x40'..='\x7e').contains(&parameter) {
                            break;
                        }
                    }
                } else {
                    output.push('\u{fffd}');
                }
            }
            '\n' | '\r' | '\t' => output.push(character),
            character if character.is_control() => output.push('\u{fffd}'),
            character => output.push(character),
        }
    }
    output
}

fn gap_resync_offset(requested: u64, committed: u64, gap: bool, window: u64) -> Option<u64> {
    (gap && committed != requested).then(|| committed.saturating_sub(window))
}

// ---------------------------------------------------------------------------------------------
// Domain rendering

struct StateVisual {
    glyph: &'static str,
    label: &'static str,
    color: Color,
}

fn state_visual(state: JobState, outcome: Option<JobOutcome>) -> StateVisual {
    let (glyph, label, color) = match (state, outcome) {
        (JobState::Pending, _) => ("◌", "Queued", palette::QUEUED),
        (JobState::Active, _) => ("●", "Running", palette::RUNNING),
        (JobState::Finalizing, _) => ("●", "Finalizing", palette::RUNNING),
        (_, Some(JobOutcome::Succeeded)) => ("✓", "Succeeded", palette::OK),
        (_, Some(JobOutcome::Failed)) => ("✗", "Failed", palette::BAD),
        (_, Some(JobOutcome::TimedOut)) => ("⏱", "Timed out", palette::BAD),
        (_, Some(JobOutcome::Interrupted)) => ("✗", "Interrupted", palette::BAD),
        (_, Some(JobOutcome::Canceled)) => ("–", "Canceled", palette::MUTED),
        (_, Some(JobOutcome::Skipped)) => ("⊘", "Skipped", palette::MUTED),
        _ => ("?", "Final", palette::MUTED),
    };
    StateVisual {
        glyph,
        label,
        color,
    }
}

fn verdict_color(verdict: AttemptVerdict) -> Color {
    match verdict {
        AttemptVerdict::Succeeded => palette::OK,
        AttemptVerdict::Canceled => palette::MUTED,
        _ => palette::BAD,
    }
}

fn project_color(name: &str) -> Color {
    let mut hash: u32 = 2_166_136_261;
    for byte in name.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    Color::Indexed(palette::PROJECTS[(hash % palette::PROJECTS.len() as u32) as usize])
}

/// `(name, value)` chips; impacts and fences carry their name only.
fn claim_chips(claims: &ResourceClaims) -> Vec<(String, String)> {
    let mut chips = Vec::new();
    if let Some(value) = claims.cpu_units {
        chips.push(("cpu".into(), value.to_string()));
    }
    if let Some(value) = claims.ram_mb {
        chips.push(("ram".into(), format!("{value}M")));
    }
    if let Some(value) = claims.cargo_slots {
        chips.push(("cargo".into(), value.to_string()));
    }
    if let Some(value) = claims.gpu_slots {
        chips.push(("gpu".into(), value.to_string()));
    }
    chips.extend(
        claims
            .custom
            .iter()
            .map(|(name, value)| (name.clone(), value.to_string())),
    );
    if !claims.shared_fences.is_empty() {
        chips.push(("shared".into(), claims.shared_fences.len().to_string()));
    }
    if !claims.exclusive_fences.is_empty() {
        chips.push((
            "exclusive".into(),
            claims.exclusive_fences.len().to_string(),
        ));
    }
    chips.extend(
        claims
            .impacts
            .iter()
            .map(|impact| (impact.clone(), String::new())),
    );
    chips
}

#[cfg(test)]
fn format_claims(claims: &ResourceClaims) -> String {
    claim_chips(claims)
        .into_iter()
        .map(|(name, value)| {
            if value.is_empty() {
                name
            } else {
                format!("{name} {value}")
            }
        })
        .collect::<Vec<_>>()
        .join(" · ")
}

fn claim_spans(claims: &ResourceClaims) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for (index, (name, value)) in claim_chips(claims).into_iter().enumerate() {
        if index > 0 {
            spans.push(Span::styled(" · ", dimmer()));
        }
        if value.is_empty() {
            spans.push(Span::styled(name, dim()));
        } else {
            spans.push(Span::styled(format!("{name} "), dim()));
            spans.push(Span::styled(value, text()));
        }
    }
    spans
}

fn command_spans(command: &str) -> Vec<Span<'static>> {
    let (executable, arguments) = split_command(command);
    let mut spans = vec![Span::styled(
        executable.to_owned(),
        text().add_modifier(Modifier::BOLD),
    )];
    if !arguments.is_empty() {
        spans.push(Span::styled(format!(" {arguments}"), dim()));
    }
    spans
}

/// Splits a preview into its executable and the rest, honouring a quoted executable.
fn split_command(command: &str) -> (&str, &str) {
    let trimmed = command.trim_start();
    if let Some(rest) = trimmed.strip_prefix('"') {
        if let Some(end) = rest.find('"') {
            let executable = &trimmed[..end + 2];
            return (executable, trimmed[end + 2..].trim_start());
        }
    }
    match trimmed.split_once(' ') {
        Some((executable, arguments)) => (executable, arguments.trim_start()),
        None => (trimmed, ""),
    }
}

fn accent() -> Style {
    Style::default().fg(palette::ACCENT)
}

fn text() -> Style {
    Style::default().fg(palette::TEXT)
}

fn dim() -> Style {
    Style::default().fg(palette::DIM)
}

fn dimmer() -> Style {
    Style::default().fg(palette::DIMMER)
}

fn bold(style: Style) -> Style {
    style.add_modifier(Modifier::BOLD)
}

// ---------------------------------------------------------------------------------------------
// Drawing

fn draw(frame: &mut ratatui::Frame<'_>, app: &mut App) {
    let clock = Clock::capture();
    let [header, body, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    frame.render_widget(header_line(app, clock, header.width as usize), header);

    if app.fullscreen {
        match app.focus {
            Pane::Queue => draw_queue(frame, app, clock, body, true),
            Pane::Detail => draw_detail(frame, app, clock, body, true),
            Pane::Logs => draw_logs(frame, app, body, true),
        }
    } else {
        let rows = app.rows().len();
        let half = usize::from(body.height) / 2;
        let queue_height = (rows + 3).clamp(5, half.max(5));
        let remaining = usize::from(body.height).saturating_sub(queue_height);
        let detail_lines = detail_lines(app, clock, usize::from(body.width.saturating_sub(2)));
        let logs_empty = app.stdout.is_empty() && app.stderr.is_empty();
        let detail_cap = if logs_empty {
            remaining.saturating_sub(3)
        } else {
            remaining * 55 / 100
        };
        let detail_height = (detail_lines.len() + 2).clamp(3, detail_cap.max(3));
        let [queue, detail, logs] = Layout::vertical([
            Constraint::Length(queue_height as u16),
            Constraint::Length(detail_height as u16),
            Constraint::Min(3),
        ])
        .areas(body);
        draw_queue(frame, app, clock, queue, app.focus == Pane::Queue);
        draw_detail_lines(frame, app, detail_lines, detail, app.focus == Pane::Detail);
        draw_logs(frame, app, logs, app.focus == Pane::Logs);
    }

    frame.render_widget(footer_line(app, footer.width as usize), footer);
}

fn pane_block(title: Line<'static>, focused: bool) -> Block<'static> {
    let border = if focused {
        accent()
    } else {
        Style::default().fg(palette::BORDER)
    };
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(border)
        .title(title)
}

fn draw_scrollbar(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    content: usize,
    position: usize,
    viewport: usize,
    focused: bool,
) {
    if content <= viewport || area.height < 3 {
        return;
    }
    let thumb = if focused { accent() } else { dim() };
    let mut state = ScrollbarState::new(content.saturating_sub(viewport).max(1))
        .position(position.min(content.saturating_sub(viewport)))
        .viewport_content_length(viewport);
    frame.render_stateful_widget(
        Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(None)
            .end_symbol(None)
            .track_symbol(Some("│"))
            .thumb_symbol("┃")
            .track_style(if focused {
                accent()
            } else {
                Style::default().fg(palette::BORDER)
            })
            .thumb_style(thumb),
        area.inner(Margin {
            vertical: 1,
            horizontal: 0,
        }),
        &mut state,
    );
}

fn header_line(app: &App, clock: Clock, width: usize) -> Line<'static> {
    let (dot, dot_style, link) = match app.link {
        Link::Connected => ("●", Style::default().fg(palette::OK), "connected"),
        Link::Reconnecting => ("◌", Style::default().fg(palette::QUEUED), "reconnecting"),
        Link::Stale => ("●", Style::default().fg(palette::BAD), "stale"),
    };
    let mut left = vec![
        Span::styled(" stillyard", bold(accent())),
        Span::styled("  ", text()),
        Span::styled(dot, dot_style),
        Span::styled(format!(" {link}"), dim()),
        Span::styled("   project ", dim()),
    ];
    match &app.project_filter {
        None => left.push(Span::styled("all", text())),
        Some(project) => left.push(Span::styled(
            project.clone(),
            bold(Style::default().fg(project_color(project))),
        )),
    }
    left.push(Span::styled(" ▾", dim()));
    let mut project_list = Vec::new();
    let projects = app.projects();
    if app.project_filter.is_none() && !projects.is_empty() {
        project_list.push(Span::styled("  ", text()));
        for (index, project) in projects.iter().enumerate() {
            if index > 0 {
                project_list.push(Span::styled(" · ", dimmer()));
            }
            project_list.push(Span::styled(
                project.clone(),
                Style::default().fg(project_color(project)),
            ));
        }
    }

    let mut running = 0;
    let mut queued = 0;
    let mut done = 0;
    let mut failed = 0;
    let mut canceled = 0;
    for job in &app.page.jobs {
        match (Bucket::of(job), job.outcome) {
            (Bucket::Running, _) => running += 1,
            (Bucket::Queued, _) => queued += 1,
            (Bucket::Finished, Some(JobOutcome::Succeeded)) => done += 1,
            (Bucket::Finished, Some(JobOutcome::Canceled | JobOutcome::Skipped)) => canceled += 1,
            (Bucket::Finished, _) => failed += 1,
        }
    }
    let mut right = vec![
        Span::styled(
            format!("● {running} running"),
            Style::default().fg(palette::RUNNING),
        ),
        Span::styled("   ", text()),
        Span::styled(
            format!("◌ {queued} queued"),
            Style::default().fg(palette::QUEUED),
        ),
        Span::styled("   ", text()),
        Span::styled(format!("✓ {done} done"), Style::default().fg(palette::OK)),
        Span::styled("   ", text()),
        Span::styled(
            format!("✗ {failed} failed"),
            Style::default().fg(palette::BAD),
        ),
    ];
    if canceled > 0 {
        right.push(Span::styled("   ", text()));
        right.push(Span::styled(
            format!("– {canceled} canceled"),
            Style::default().fg(palette::MUTED),
        ));
    }
    let zone = vec![
        Span::styled("   ", text()),
        Span::styled(clock.offset_label(), dimmer()),
    ];
    right.push(Span::styled(" ", text()));

    // Degrade from the least important decoration: the project list, then the zone label.
    let fits = |left: &[Span<'_>], extra: &[Span<'_>], right: &[Span<'_>], zone: &[Span<'_>]| {
        span_width(left) + span_width(extra) + span_width(right) + span_width(zone) < width
    };
    let (extra, zone): (Vec<Span<'static>>, Vec<Span<'static>>) =
        if fits(&left, &project_list, &right, &zone) {
            (project_list, zone)
        } else if fits(&left, &[], &right, &zone) {
            (Vec::new(), zone)
        } else {
            (Vec::new(), Vec::new())
        };
    left.extend(extra);
    let trailer = right.pop();
    right.extend(zone);
    right.extend(trailer);
    join_edges(left, right, width)
}

/// Left- and right-aligned span groups on one line; the right group yields when cramped.
fn join_edges(
    mut left: Vec<Span<'static>>,
    right: Vec<Span<'static>>,
    width: usize,
) -> Line<'static> {
    let left_width = span_width(&left);
    let right_width = span_width(&right);
    if left_width + right_width < width {
        left.push(Span::raw(" ".repeat(width - left_width - right_width)));
        left.extend(right);
    }
    Line::from(left)
}

fn footer_line(app: &App, width: usize) -> Line<'static> {
    let expand: (&str, &str) = if app.fullscreen {
        ("Esc", "back")
    } else {
        ("⏎", "fullscreen")
    };
    let keys: Vec<(&str, &str)> = match app.focus {
        Pane::Queue => vec![
            ("↑↓", "navigate"),
            ("⇥", "pane"),
            expand,
            ("p", "project"),
            ("r", "refresh"),
            ("q", "detach"),
        ],
        Pane::Detail => vec![
            ("↑↓", "scroll"),
            ("g/G", "top/end"),
            ("⇥", "pane"),
            expand,
            ("q", "detach"),
        ],
        Pane::Logs => vec![
            ("←→", "stream"),
            ("↑↓", "scroll"),
            ("g/G", "top/end"),
            ("f", "follow"),
            ("⇥", "pane"),
            expand,
            ("q", "detach"),
        ],
    };
    let mut left = vec![Span::raw(" ")];
    for (index, (key, label)) in keys.into_iter().enumerate() {
        if index > 0 {
            left.push(Span::raw("   "));
        }
        left.push(Span::styled(key.to_owned(), bold(accent())));
        left.push(Span::styled(format!(" {label}"), dim()));
    }
    let status_room = width.saturating_sub(span_width(&left) + 3);
    let right = vec![
        Span::styled(ellipsize(&app.status, status_room), dim()),
        Span::raw(" "),
    ];
    join_edges(left, right, width)
}

// --- queue ------------------------------------------------------------------------------------

struct Column {
    title: &'static str,
    width: u16,
}

fn queue_columns(width: u16) -> (Vec<Column>, Vec<Constraint>) {
    let mut columns = vec![Column {
        title: "STATE",
        width: 13,
    }];
    if width >= 80 {
        columns.push(Column {
            title: "PROJECT",
            width: 12,
        });
    }
    columns.push(Column {
        title: "WHEN",
        width: 11,
    });
    columns.push(Column {
        title: "TIME",
        width: 10,
    });
    if width >= 100 {
        columns.push(Column {
            title: "CLAIMS",
            width: 22,
        });
    }
    columns.push(Column {
        title: "COMMAND",
        width: 0,
    });
    if width >= 120 {
        columns.push(Column {
            title: "NOTE",
            width: 0,
        });
    }
    let constraints = columns
        .iter()
        .map(|column| match column.title {
            "COMMAND" => Constraint::Fill(3),
            "NOTE" => Constraint::Fill(1),
            _ => Constraint::Length(column.width),
        })
        .collect();
    (columns, constraints)
}

fn draw_queue(
    frame: &mut ratatui::Frame<'_>,
    app: &mut App,
    clock: Clock,
    area: Rect,
    focused: bool,
) {
    let rows = app.rows();
    let total_jobs = app.ordered_jobs().len();
    let mut title = vec![Span::styled(
        " Queue ",
        bold(if focused { accent() } else { dim() }),
    )];
    if let Some(project) = &app.project_filter {
        title.push(Span::styled(
            format!("{project} "),
            Style::default().fg(project_color(project)),
        ));
    }
    let block = pane_block(Line::from(title), focused).title_top(
        Line::from(vec![
            Span::styled(format!(" {total_jobs} jobs  "), dim()),
            Span::styled(clock.compact(clock.now_millis), dim()),
            Span::raw(" "),
        ])
        .right_aligned(),
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height < 2 || inner.width < 20 {
        return;
    }

    // One column for the selection bar, then the table columns.
    let table_area = Rect {
        x: inner.x + 1,
        width: inner.width.saturating_sub(1),
        ..inner
    };
    let (columns, constraints) = queue_columns(inner.width);
    let widths: Vec<usize> = Layout::horizontal(constraints)
        .spacing(1)
        .split(table_area)
        .iter()
        .map(|rect| usize::from(rect.width))
        .collect();

    let view = usize::from(inner.height) - 1;
    app.queue_view = view;
    let selected_row = rows
        .iter()
        .position(|row| *row == RowKind::Job(app.selected));
    if let Some(selected_row) = selected_row {
        if selected_row < app.queue_offset {
            app.queue_offset = selected_row.saturating_sub(1);
        } else if selected_row >= app.queue_offset + view {
            app.queue_offset = selected_row + 1 - view;
        }
    }
    app.queue_offset = clamp_scroll(app.queue_offset, rows.len(), view);

    let mut lines = Vec::with_capacity(view + 1);
    let mut header = vec![Span::raw(" ")];
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            header.push(Span::raw(" "));
        }
        header.push(Span::styled(
            pad_right(column.title, widths[index]),
            bold(dim()),
        ));
    }
    lines.push(Line::from(header));

    if rows.is_empty() {
        lines.push(Line::from(Span::styled(
            "  no jobs match the current selector and project filter",
            dim(),
        )));
    }
    for row in rows.iter().skip(app.queue_offset).take(view) {
        lines.push(match *row {
            RowKind::Group { bucket, count } => {
                let label = format!(" {} ({count}) ", bucket.title());
                let rule = usize::from(inner.width).saturating_sub(label.chars().count() + 2);
                Line::from(vec![
                    Span::raw(" "),
                    Span::styled("─", dimmer()),
                    Span::styled(label, dim()),
                    Span::styled("─".repeat(rule), dimmer()),
                ])
            }
            RowKind::Job(index) => {
                let job = &app.page.jobs[index];
                let selected = index == app.selected;
                let mut spans = vec![if selected {
                    Span::styled("▌", accent())
                } else {
                    Span::raw(" ")
                }];
                for (column_index, column) in columns.iter().enumerate() {
                    if column_index > 0 {
                        spans.push(Span::raw(" "));
                    }
                    spans.extend(queue_cell(app, job, column, widths[column_index], clock));
                }
                let mut line = Line::from(spans);
                if selected {
                    line = line.patch_style(Style::default().bg(palette::SELECTION));
                }
                line
            }
        });
    }
    frame.render_widget(Paragraph::new(lines), inner);
    draw_scrollbar(frame, area, rows.len(), app.queue_offset, view, focused);
}

fn queue_cell(
    app: &App,
    job: &JobSummary,
    column: &Column,
    width: usize,
    clock: Clock,
) -> Vec<Span<'static>> {
    let visual = state_visual(job.state, job.outcome);
    let bucket = Bucket::of(job);
    let context_only = app.tree.context_only.contains(&job.job_id);
    match column.title {
        "STATE" => vec![Span::styled(
            pad_right(&format!("{} {}", visual.glyph, visual.label), width),
            if context_only {
                dim()
            } else if bucket == Bucket::Running {
                bold(Style::default().fg(visual.color))
            } else {
                Style::default().fg(visual.color)
            },
        )],
        "PROJECT" => match project_of(job) {
            Some(project) => vec![Span::styled(
                pad_right(project, width),
                Style::default().fg(project_color(project)),
            )],
            None => vec![Span::styled(pad_right("—", width), dimmer())],
        },
        "WHEN" => {
            let when = match bucket {
                Bucket::Queued => job.accepted_unix_millis,
                Bucket::Running => job.started_unix_millis.unwrap_or(job.accepted_unix_millis),
                Bucket::Finished => job
                    .finished_unix_millis
                    .or(job.started_unix_millis)
                    .unwrap_or(job.accepted_unix_millis),
            };
            vec![Span::styled(pad_right(&clock.compact(when), width), text())]
        }
        "TIME" => {
            let (value, style) = match bucket {
                Bucket::Queued => {
                    let rank = job
                        .queue_rank
                        .map_or_else(|| "#?".to_owned(), |rank| format!("#{rank}"));
                    let eta = job.estimate.start_in_millis.map_or_else(
                        || "?".to_owned(),
                        |millis| format!("~{}", format_duration(millis)),
                    );
                    (
                        format!("{rank} {eta}"),
                        Style::default().fg(palette::QUEUED),
                    )
                }
                Bucket::Running => (
                    format!(
                        "▶ {}",
                        format_duration(elapsed_millis(job.started_unix_millis, None, clock))
                    ),
                    Style::default().fg(palette::RUNNING),
                ),
                Bucket::Finished => (
                    job.started_unix_millis.map_or_else(
                        || "—".to_owned(),
                        |started| {
                            format_duration(elapsed_millis(
                                Some(started),
                                job.finished_unix_millis,
                                clock,
                            ))
                        },
                    ),
                    dim(),
                ),
            };
            vec![Span::styled(pad_right(&value, width), style)]
        }
        "CLAIMS" => fit_spans(claim_spans(&job.claims), width),
        "COMMAND" => {
            let command = job.batch_member.as_ref().map_or_else(
                || job.command_preview.clone(),
                |member| {
                    if job.command_preview.is_empty() {
                        member.clone()
                    } else {
                        format!("{member}: {}", job.command_preview)
                    }
                },
            );
            let depth = app.tree.depths.get(&job.job_id).copied().unwrap_or(0);
            let guide = if app.tree.orphans.contains(&job.job_id) {
                "?─ ".to_owned()
            } else if depth == 0 {
                String::new()
            } else {
                format!("{}├─ ", "│  ".repeat(depth.saturating_sub(1) as usize))
            };
            let command = format!("{guide}{command}");
            if context_only {
                fit_spans(vec![Span::styled(command, dim())], width)
            } else {
                fit_spans(command_spans(&command), width)
            }
        }
        "NOTE" => {
            let note = if app.tree.orphans.contains(&job.job_id) {
                "orphan: parent not retained".to_owned()
            } else {
                job.blocker
                    .as_ref()
                    .map(|blocker| blocker.code.clone())
                    .or_else(|| app.tree.collapsed_outcome_summary(job.job_id))
                    .unwrap_or_default()
            };
            let style = if bucket == Bucket::Queued && !note.is_empty() {
                Style::default().fg(palette::WARN)
            } else {
                dim()
            };
            vec![Span::styled(pad_right(&note, width), style)]
        }
        _ => vec![Span::raw(" ".repeat(width))],
    }
}

fn elapsed_millis(started: Option<i64>, finished: Option<i64>, clock: Clock) -> u64 {
    let Some(started) = started else {
        return 0;
    };
    let end = finished.unwrap_or(clock.now_millis);
    u64::try_from(end.saturating_sub(started)).unwrap_or(0)
}

/// Clips styled spans to `width` characters (ending in `…`) and pads to exactly `width`.
fn fit_spans(spans: Vec<Span<'static>>, width: usize) -> Vec<Span<'static>> {
    let total = span_width(&spans);
    let mut output = Vec::with_capacity(spans.len() + 1);
    if total <= width {
        output.extend(spans);
        output.push(Span::raw(" ".repeat(width - total)));
        return output;
    }
    let mut remaining = width.saturating_sub(1);
    for span in spans {
        if remaining == 0 {
            break;
        }
        let count = span.content.chars().count();
        if count <= remaining {
            remaining -= count;
            output.push(span);
        } else {
            let cut: String = span.content.chars().take(remaining).collect();
            output.push(Span::styled(cut, span.style));
            remaining = 0;
        }
    }
    if width > 0 {
        output.push(Span::styled("…", dim()));
    }
    output
}

// --- detail -----------------------------------------------------------------------------------

const KEY_WIDTH: usize = 10;

fn detail_lines(app: &App, clock: Clock, width: usize) -> Vec<Line<'static>> {
    let Some(job) = &app.detail else {
        return vec![Line::from(Span::styled(" No Job selected", dim()))];
    };
    let selected_command = app
        .page
        .jobs
        .get(app.selected)
        .map(|summary| summary.command_preview.as_str())
        .unwrap_or_default();
    let mut lines = Vec::new();
    let mut kv = |key: &str, spans: Vec<Span<'static>>| {
        let mut all = vec![Span::styled(
            format!("{}  ", pad_left(key, KEY_WIDTH)),
            dim(),
        )];
        all.extend(spans);
        lines.extend(wrap_spans(&all, width, KEY_WIDTH + 2));
    };

    if !selected_command.is_empty() {
        kv("Command", command_spans(selected_command));
    }
    kv(
        "CWD",
        vec![Span::styled(
            job.spec.working_directory.display().to_string(),
            text(),
        )],
    );
    let mut timeline = vec![Span::styled(clock.exact(job.accepted_unix_millis), text())];
    if let Some(started) = job.started_unix_millis {
        timeline.push(Span::styled("   started ", dim()));
        timeline.push(Span::styled(clock.precise(started), text()));
    }
    if let Some(finished) = job.finished_unix_millis {
        timeline.push(Span::styled("   finished ", dim()));
        timeline.push(Span::styled(clock.precise(finished), text()));
    }
    match (job.started_unix_millis, job.finished_unix_millis) {
        (Some(started), finished) => {
            timeline.push(Span::styled(
                format!(
                    "   {} {}",
                    format_duration(elapsed_millis(Some(started), finished, clock)),
                    if finished.is_some() { "run" } else { "so far" }
                ),
                dim(),
            ));
        }
        (None, Some(finished)) => {
            timeline.push(Span::styled(
                format!(
                    "   {} in queue, never started",
                    format_duration(elapsed_millis(
                        Some(job.accepted_unix_millis),
                        Some(finished),
                        clock
                    ))
                ),
                dim(),
            ));
        }
        (None, None) => {
            timeline.push(Span::styled(
                format!(
                    "   waiting {}",
                    format_duration(elapsed_millis(Some(job.accepted_unix_millis), None, clock))
                ),
                dim(),
            ));
        }
    }
    kv("Accepted", timeline);
    let visual = state_visual(job.state, job.outcome);
    let mut state = vec![Span::styled(
        format!("{} {}", visual.glyph, visual.label),
        bold(Style::default().fg(visual.color)),
    )];
    if job.cancel_requested && job.state != JobState::Final {
        state.push(Span::styled(
            "   cancel requested",
            Style::default().fg(palette::WARN),
        ));
    }
    if let Some(exit_code) = job.root_exit_code {
        state.push(Span::styled(format!("   exit {exit_code}"), dim()));
    }
    kv("State", state);
    let claims = claim_spans(&job.spec.resources);
    if !claims.is_empty() {
        kv("Claims", claims);
    }
    if let Some(policy) = &job.spec.child_submission_policy {
        kv(
            "Child policy",
            vec![
                Span::styled("authorization only; not reserved  ", dim()),
                Span::styled(
                    serde_json::to_string(policy).unwrap_or_else(|_| "?".into()),
                    text(),
                ),
            ],
        );
    }
    if !job.spec.labels.is_empty() {
        let mut spans = Vec::new();
        for (index, label) in job.spec.labels.iter().enumerate() {
            if index > 0 {
                spans.push(Span::raw("   "));
            }
            spans.push(Span::styled(label.key.clone(), dim()));
            spans.push(Span::styled("=", dimmer()));
            let style = if label.key == PROJECT_LABEL {
                Style::default().fg(project_color(&label.value))
            } else {
                text()
            };
            spans.push(Span::styled(label.value.clone(), style));
        }
        kv("Labels", spans);
    }
    if let Some(parent) = &job.parent {
        kv(
            "Parent",
            vec![Span::styled(
                format!(
                    "{} / {} / {}",
                    parent.job_id, parent.attempt_id, parent.invocation_id
                ),
                text(),
            )],
        );
    }
    if let Some(admission) = &job.managed_policy_admission {
        kv(
            "Policy admit",
            vec![Span::styled(
                format!(
                    "{} ancestor(s), evaluated {}; effective {}",
                    admission.policy_ancestors.len(),
                    clock.precise(admission.evaluated_unix_millis),
                    serde_json::to_string(&admission.effective_policy)
                        .unwrap_or_else(|_| "?".into())
                ),
                text(),
            )],
        );
    }
    if let Some(batch_id) = job.batch_id {
        let mut spans = vec![Span::styled(batch_id.to_string(), text())];
        if let Some(member) = &job.batch_member {
            spans.push(Span::styled(format!(" / {member}"), text()));
        }
        kv("Batch", spans);
    }
    if let Some(gpu) = &job.gpu_provenance {
        kv(
            "GPU",
            vec![
                Span::styled(gpu.uuid.clone(), text()),
                Span::styled(format!("  driver {}", gpu.driver_version), dim()),
            ],
        );
    }
    if job.admission.is_none() && !job.blockers.is_empty() {
        for blocker in &job.blockers {
            kv(
                "Blocked",
                vec![
                    Span::styled(
                        blocker.code.clone(),
                        bold(Style::default().fg(palette::WARN)),
                    ),
                    Span::styled(
                        format!("  {}", blocker.detail),
                        Style::default().fg(palette::WARN),
                    ),
                ],
            );
        }
    }

    if let Some(admission) = &job.admission {
        lines.push(Line::default());
        let (glyph, style) = match admission.state {
            stillyard::AdmissionDecisionState::Failed => ("✗", Style::default().fg(palette::BAD)),
            stillyard::AdmissionDecisionState::Reserved
            | stillyard::AdmissionDecisionState::Released => {
                ("✓", Style::default().fg(palette::OK))
            }
            _ => ("◌", Style::default().fg(palette::QUEUED)),
        };
        let mut heading = vec![
            Span::styled(" Admission", bold(accent())),
            Span::raw("   "),
            Span::styled(format!("{glyph} {:?}", admission.state), bold(style)),
        ];
        if let Some(evaluated) = admission.evaluated_unix_millis {
            heading.push(Span::styled("   evaluated ", dim()));
            heading.push(Span::styled(clock.precise(evaluated), text()));
        }
        if admission.final_sample {
            heading.push(Span::styled("   final sample", dim()));
        }
        if admission.deferral_count > 0 {
            heading.push(Span::styled(
                format!("   deferrals {}", admission.deferral_count),
                dim(),
            ));
        }
        lines.push(Line::from(heading));
        if !admission.operands.is_empty() {
            lines.push(Line::from(vec![
                Span::raw("   "),
                Span::styled(
                    format!(
                        "{} {} {} {} {}   verdict",
                        pad_right("operand", 12),
                        pad_left("requested", 10),
                        pad_left("observed", 10),
                        pad_left("capacity", 10),
                        pad_left("margin", 8)
                    ),
                    bold(dim()),
                ),
            ]));
            for operand in &admission.operands {
                let optional = |value: Option<u64>, width: usize| match value {
                    Some(value) => Span::styled(pad_left(&value.to_string(), width), text()),
                    None => Span::styled(pad_left("—", width), dimmer()),
                };
                let mut spans = vec![
                    Span::raw("   "),
                    Span::styled(pad_right(&operand.name, 12), text()),
                    Span::raw(" "),
                    Span::styled(pad_left(&operand.requested.to_string(), 10), text()),
                    Span::raw(" "),
                    optional(operand.observed, 10),
                    Span::raw(" "),
                    optional(operand.configured_capacity, 10),
                    Span::raw(" "),
                    Span::styled(pad_left(&operand.safety_margin.to_string(), 8), text()),
                    Span::raw("   "),
                ];
                spans.push(verdict_span(operand.satisfied));
                if let Some(granted) = operand.granted_debit {
                    spans.push(Span::styled(format!("  granted {granted}"), dim()));
                }
                lines.push(Line::from(spans));
            }
        }
        for detector in &admission.detectors {
            let mut spans = vec![
                Span::raw("   "),
                Span::styled(detector.detector.clone(), text()),
            ];
            if let Some(observed) = detector.observed {
                spans.push(Span::styled(format!("  observed {observed}"), dim()));
            }
            if let Some(threshold) = detector.threshold {
                spans.push(Span::styled(format!("  threshold {threshold}"), dim()));
            }
            spans.push(Span::raw("   "));
            spans.push(verdict_span(detector.satisfied));
            if let Some(detail) = &detector.detail {
                spans.push(Span::styled(format!("  {detail}"), dim()));
            }
            lines.extend(wrap_spans(&spans, width, 5));
        }
        for blocker in &admission.blockers {
            let spans = vec![
                Span::raw("   "),
                Span::styled(
                    format!("✗ {}", blocker.code),
                    bold(Style::default().fg(palette::BAD)),
                ),
                Span::styled(
                    format!("  {}", blocker.detail),
                    Style::default().fg(palette::BAD),
                ),
            ];
            lines.extend(wrap_spans(&spans, width, 5));
        }
    }

    if !job.attempts.is_empty() {
        lines.push(Line::default());
        lines.push(Line::from(Span::styled(" Attempts", bold(accent()))));
        for attempt in &job.attempts {
            let mut spans = vec![
                Span::raw("   "),
                Span::styled(attempt.attempt_index.to_string(), text()),
                Span::raw("  "),
            ];
            match attempt.verdict {
                Some(verdict) => {
                    let mut label = verdict.as_str().to_owned();
                    if let Some(reason) = &attempt.reason_code {
                        label.push('/');
                        label.push_str(reason);
                    }
                    spans.push(Span::styled(
                        label,
                        bold(Style::default().fg(verdict_color(verdict))),
                    ));
                }
                None => spans.push(Span::styled(
                    "in progress",
                    bold(Style::default().fg(palette::RUNNING)),
                )),
            }
            spans.push(Span::styled("   created ", dim()));
            spans.push(Span::styled(
                clock.precise(attempt.created_unix_millis),
                text(),
            ));
            if let Some(started) = attempt.started_unix_millis {
                spans.push(Span::styled("   started ", dim()));
                spans.push(Span::styled(clock.precise(started), text()));
            }
            if let Some(finished) = attempt.finished_unix_millis {
                spans.push(Span::styled("   finished ", dim()));
                spans.push(Span::styled(clock.precise(finished), text()));
            }
            spans.push(Span::styled(
                format!(
                    "   {} invocation{}",
                    attempt.invocations.len(),
                    if attempt.invocations.len() == 1 {
                        ""
                    } else {
                        "s"
                    }
                ),
                dim(),
            ));
            lines.extend(wrap_spans(&spans, width, 6));
            if attempt.invocations.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("   └ ", dimmer()),
                    Span::styled("no invocation was launched", dim()),
                ]));
            }
            for (index, invocation) in attempt.invocations.iter().enumerate() {
                let branch = if index + 1 == attempt.invocations.len() {
                    "└ "
                } else {
                    "├ "
                };
                let mut spans = vec![
                    Span::raw("   "),
                    Span::styled(branch, dimmer()),
                    Span::styled(
                        format!("{:?}[{}]", invocation.role, invocation.role_index),
                        text(),
                    ),
                    Span::styled(format!("  {:?}", invocation.state), text()),
                    Span::styled(
                        format!("  containment {:?}", invocation.containment.state),
                        dim(),
                    ),
                ];
                if let Some(exit_code) = invocation.root_exit_code {
                    let classification = invocation
                        .exit_classification
                        .map(|classification| format!("/{classification:?}"))
                        .unwrap_or_default();
                    spans.push(Span::styled(
                        format!("  exit {exit_code}{classification}"),
                        if exit_code == 0 {
                            Style::default().fg(palette::OK)
                        } else {
                            Style::default().fg(palette::BAD)
                        },
                    ));
                }
                if let Some(incident) = invocation.containment.incident_id {
                    spans.push(Span::styled(
                        format!("  incident {incident}"),
                        Style::default().fg(palette::WARN),
                    ));
                }
                if let Some(started) = invocation.started_unix_millis {
                    spans.push(Span::styled("  started ", dim()));
                    spans.push(Span::styled(clock.precise(started), text()));
                }
                if let Some(finished) = invocation.finished_unix_millis {
                    spans.push(Span::styled("  finished ", dim()));
                    spans.push(Span::styled(clock.precise(finished), text()));
                }
                lines.extend(wrap_spans(&spans, width, 5));
            }
        }
    }
    lines
}

fn verdict_span(satisfied: bool) -> Span<'static> {
    if satisfied {
        Span::styled("✓ pass", Style::default().fg(palette::OK))
    } else {
        Span::styled("✗ blocked", Style::default().fg(palette::BAD))
    }
}

fn draw_detail(
    frame: &mut ratatui::Frame<'_>,
    app: &mut App,
    clock: Clock,
    area: Rect,
    focused: bool,
) {
    let lines = detail_lines(app, clock, usize::from(area.width.saturating_sub(2)));
    draw_detail_lines(frame, app, lines, area, focused);
}

fn draw_detail_lines(
    frame: &mut ratatui::Frame<'_>,
    app: &mut App,
    lines: Vec<Line<'static>>,
    area: Rect,
    focused: bool,
) {
    let mut title = vec![Span::styled(
        " Job ",
        bold(if focused { accent() } else { dim() }),
    )];
    let mut right = Vec::new();
    if let Some(job) = &app.detail {
        title.push(Span::styled(format!("{} ", job.job_id), dim()));
        let visual = state_visual(job.state, job.outcome);
        right.push(Span::styled(
            format!(" {} {} ", visual.glyph, visual.label),
            Style::default().fg(visual.color),
        ));
    }
    let block = pane_block(Line::from(title), focused).title_top(Line::from(right).right_aligned());
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let view = usize::from(inner.height);
    app.detail_view = view.max(1);
    app.detail_len = lines.len();
    app.detail_scroll = clamp_scroll(app.detail_scroll, lines.len(), view);
    let visible: Vec<Line<'static>> = lines
        .iter()
        .skip(app.detail_scroll)
        .take(view)
        .cloned()
        .collect();
    frame.render_widget(Paragraph::new(visible), inner);
    draw_scrollbar(frame, area, lines.len(), app.detail_scroll, view, focused);
}

// --- logs -------------------------------------------------------------------------------------

fn draw_logs(frame: &mut ratatui::Frame<'_>, app: &mut App, area: Rect, focused: bool) {
    let stdout_len = app.stdout.len() as u64;
    let stderr_len = app.stderr.len() as u64;
    let tab = |name: &str, stream: LogStream, len: u64| {
        let active = app.stream == stream;
        let name_style = if active {
            bold(if focused { accent() } else { text() })
        } else if len > 0 && stream == LogStream::Stderr {
            Style::default().fg(palette::WARN)
        } else {
            dim()
        };
        vec![
            Span::styled(format!(" {name} "), name_style),
            Span::styled(format_bytes(len), if active { text() } else { dim() }),
            Span::raw(" "),
        ]
    };
    let mut title = tab("stdout", LogStream::Stdout, stdout_len);
    title.push(Span::styled("│", Style::default().fg(palette::BORDER)));
    title.extend(tab("stderr", LogStream::Stderr, stderr_len));
    let mut right = Vec::new();
    let window_start = app.log_window_start();
    if window_start > 0 {
        right.push(Span::styled(
            format!(" {} earlier not shown ", format_bytes(window_start)),
            dim(),
        ));
    }
    right.push(if app.follow {
        Span::styled(" ⇣ follow ", Style::default().fg(palette::OK))
    } else {
        Span::styled(" ⇣ follow off ", dim())
    });
    let block = pane_block(Line::from(title), focused).title_top(Line::from(right).right_aligned());
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width < 4 {
        return;
    }

    let width = usize::from(inner.width) - 1;
    let buffer = app.log_buffer();
    let lines: Vec<Line<'static>> = if buffer.is_empty() {
        let other = match app.stream {
            LogStream::Stdout => ("stderr", stderr_len, "→"),
            _ => ("stdout", stdout_len, "←"),
        };
        let hint = if other.1 > 0 {
            format!(
                " {:?} is empty · {} has {} ({})",
                app.stream,
                other.0,
                format_bytes(other.1),
                other.2
            )
            .to_lowercase()
        } else {
            " no output committed for this job".to_owned()
        };
        vec![Line::from(Span::styled(hint, dimmer()))]
    } else {
        let (front, back) = buffer.as_slices();
        let mut bytes = Vec::with_capacity(buffer.len());
        bytes.extend_from_slice(front);
        bytes.extend_from_slice(back);
        let text = terminal_text(&bytes);
        let log_style = Style::default().fg(palette::LOG);
        text.lines()
            .flat_map(|line| {
                let spans = [Span::styled(format!(" {line}"), log_style)];
                wrap_spans(&spans, width, 1)
            })
            .collect()
    };
    let view = usize::from(inner.height);
    app.log_view = view;
    app.log_len = lines.len();
    if app.follow {
        app.log_scroll = lines.len().saturating_sub(view);
    }
    app.log_scroll = clamp_scroll(app.log_scroll, lines.len(), view);
    let visible: Vec<Line<'static>> = lines
        .iter()
        .skip(app.log_scroll)
        .take(view)
        .cloned()
        .collect();
    frame.render_widget(Paragraph::new(visible), inner);
    draw_scrollbar(frame, area, lines.len(), app.log_scroll, view, focused);
}

#[cfg(test)]
mod tests {
    use super::*;
    use stillyard::Estimate;

    fn summary(
        state: &str,
        outcome: Option<&str>,
        accepted: i64,
        started: Option<i64>,
        finished: Option<i64>,
        rank: Option<u64>,
        project: Option<&str>,
    ) -> JobSummary {
        let labels = project
            .map(|project| serde_json::json!([{ "key": "project", "value": project }]))
            .unwrap_or_else(|| serde_json::json!([]));
        serde_json::from_value(serde_json::json!({
            "job_id": format!("{}~{}", uuid::Uuid::nil(), uuid::Uuid::now_v7()),
            "command_preview": "cargo test --workspace",
            "batch_id": null,
            "batch_member": null,
            "parent": null,
            "state": state,
            "outcome": outcome,
            "accepted_unix_millis": accepted,
            "started_unix_millis": started,
            "finished_unix_millis": finished,
            "queue_rank": rank,
            "estimate": serde_json::to_value(Estimate::unknown("test")).unwrap(),
            "claims": serde_json::to_value(ResourceClaims::default()).unwrap(),
            "labels": labels,
            "blocker": null,
            "attempt_id": null,
            "invocation_id": null,
            "stdout_committed": 0,
            "stderr_committed": 0,
        }))
        .expect("test summary deserializes")
    }

    fn app_with(jobs: Vec<JobSummary>) -> App {
        let page: JobListPage = serde_json::from_value(serde_json::json!({
            "jobs": [],
            "next_cursor": null,
            "event_cursor": { "store_uuid": uuid::Uuid::nil(), "sequence": 0 },
        }))
        .expect("test page deserializes");
        let mut app = App {
            page,
            tree: TreeView::default(),
            selected: 0,
            detail: None,
            detail_job: None,
            stdout_offset: 0,
            stderr_offset: 0,
            stdout: VecDeque::new(),
            stderr: VecDeque::new(),
            status: String::new(),
            link: Link::Connected,
            last_refresh: Instant::now(),
            focus: Pane::Queue,
            fullscreen: false,
            stream: LogStream::Stdout,
            follow: true,
            project_filter: None,
            queue_offset: 0,
            detail_scroll: 0,
            log_scroll: 0,
            queue_view: 10,
            detail_view: 10,
            log_view: 10,
            detail_len: 0,
            log_len: 0,
        };
        app.page.jobs = jobs;
        app.settle_selection(None);
        app
    }

    fn snapshot(summary: &JobSummary) -> JobSnapshot {
        let spec = serde_json::json!({
            "spec_version": 2,
            "executable": r"C:\tools\cmd.exe",
            "args": ["/d", "/c", "echo released>target\\dogfood\\e-released.txt"],
            "working_directory": r"C:\Development\stillyard",
            "resources": { "gpu_slots": 1 },
            "labels": [
                { "key": "project", "value": "lab" },
                { "key": "gate", "value": "dogfood" }
            ],
        });
        serde_json::from_value(serde_json::json!({
            "job_id": summary.job_id,
            "submission_id": format!("{}~{}", uuid::Uuid::nil(), uuid::Uuid::now_v7()),
            "batch_id": null,
            "batch_member": null,
            "state": "final",
            "outcome": "canceled",
            "attempt_id": null,
            "invocation_id": null,
            "containment_id": null,
            "root_exit_code": null,
            "cancel_requested": false,
            "accepted_unix_millis": summary.accepted_unix_millis,
            "started_unix_millis": null,
            "finished_unix_millis": summary.accepted_unix_millis + 8_107,
            "spec": spec,
            "parent": null,
            "blockers": [],
            "attempts": [{
                "attempt_id": format!("{}~{}", uuid::Uuid::nil(), uuid::Uuid::now_v7()),
                "attempt_index": 1,
                "verdict": "canceled",
                "reason_code": null,
                "created_unix_millis": summary.accepted_unix_millis + 27,
                "started_unix_millis": null,
                "deadline_unix_millis": null,
                "finished_unix_millis": summary.accepted_unix_millis + 8_107,
                "admission": null,
                "invocations": [],
            }],
            "gpu_provenance": null,
            "admission": {
                "state": "failed",
                "evaluated_unix_millis": summary.accepted_unix_millis + 7_250,
                "observation_generation": null,
                "blockers": [{
                    "code": "observation_missing",
                    "detail": "nvml: NVML provider is disabled by host policy",
                }],
                "operands": [{
                    "name": "gpu_slots",
                    "requested": 1,
                    "configured_capacity": 1,
                    "observed": null,
                    "safety_margin": 0,
                    "granted_debit": null,
                    "satisfied": true,
                }, {
                    "name": "vram_mb",
                    "requested": 1024,
                    "configured_capacity": null,
                    "observed": null,
                    "safety_margin": 0,
                    "granted_debit": null,
                    "satisfied": false,
                }],
                "detectors": [],
                "gpu_provenance": null,
                "final_sample": true,
                "deferral_count": 0,
            },
            "daemon_generation": uuid::Uuid::now_v7(),
        }))
        .expect("test snapshot deserializes")
    }

    fn screen_text(app: &mut App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(width, height))
            .expect("test terminal");
        terminal
            .draw(|frame| draw(frame, app))
            .expect("frame draws without panicking");
        let buffer = terminal.backend().buffer();
        let mut text = String::new();
        for y in 0..height {
            for x in 0..width {
                text.push_str(buffer.cell((x, y)).map_or(" ", |cell| cell.symbol()));
            }
            text.push('\n');
        }
        text
    }

    #[test]
    fn screen_renders_groups_selection_detail_and_scrollbars_at_every_size() {
        let now = unix_millis_now();
        let mut jobs = vec![
            summary(
                "active",
                None,
                now - 9_000,
                Some(now - 6_200),
                None,
                None,
                Some("stillyard"),
            ),
            summary(
                "pending",
                None,
                now - 4_000,
                None,
                None,
                Some(1),
                Some("the-debrix"),
            ),
            summary(
                "final",
                Some("canceled"),
                now - 30_000,
                None,
                Some(now - 21_000),
                None,
                Some("lab"),
            ),
        ];
        for index in 0..40 {
            jobs.push(summary(
                "final",
                Some(if index % 3 == 0 {
                    "failed"
                } else {
                    "succeeded"
                }),
                now - 100_000 - index * 1_000,
                Some(now - 99_000 - index * 1_000),
                Some(now - 90_000 - index * 1_000),
                None,
                Some("stillyard"),
            ));
        }
        let mut app = app_with(jobs);
        app.selected = 2;
        app.detail = Some(snapshot(&app.page.jobs[2]));
        app.detail_job = Some(app.page.jobs[2].job_id);
        for line in 0..300 {
            app.stdout
                .extend(format!("test line {line} \x1b[32mok\x1b[0m\n").bytes());
        }
        app.stdout_offset = 4_096 + app.stdout.len() as u64;

        let wide = screen_text(&mut app, 136, 44);
        for expected in [
            "stillyard",
            "1 running",
            "1 queued",
            "26 done",
            "14 failed",
            "1 canceled",
            "PROJECT",
            "Running (1)",
            "Queued (1)",
            "Finished (41)",
            "▌– Canceled",
            "lab",
            "the-debrix",
            "#1 ?",
            "▶ 6.",
            "Admission",
            "stdout",
            "⇣ follow",
            "4.0K earlier not shown",
            "test line 299 ok",
            "┃",
            "⏎ fullscreen",
        ] {
            assert!(
                wide.contains(expected),
                "136x44 screen lacks {expected:?}:\n{wide}"
            );
        }
        assert!(
            !wide.contains("\x1b"),
            "ANSI escapes must not reach the screen"
        );

        app.focus = Pane::Detail;
        app.fullscreen = true;
        let detail = screen_text(&mut app, 136, 44);
        for expected in [
            "✗ Failed",
            "✗ observation_missing",
            "nvml: NVML provider is disabled by host policy",
            "gpu_slots",
            "✗ blocked",
            "✓ pass",
            "Attempts",
            "canceled",
            "no invocation was launched",
            "project=lab",
            "gate=dogfood",
            "in queue, never started",
        ] {
            assert!(
                detail.contains(expected),
                "fullscreen detail lacks {expected:?}:\n{detail}"
            );
        }

        app.focus = Pane::Logs;
        app.fullscreen = true;
        let logs = screen_text(&mut app, 136, 44);
        assert!(
            logs.contains("test line 299 ok"),
            "fullscreen logs follow the tail:\n{logs}"
        );
        assert!(
            logs.contains("Esc back"),
            "fullscreen footer offers a way back:\n{logs}"
        );
        assert!(
            !logs.contains("Running (1)"),
            "fullscreen logs hide the queue:\n{logs}"
        );
        app.scroll_logs(-1_000);
        let top = screen_text(&mut app, 136, 44);
        assert!(
            top.contains("test line 0 ok"),
            "scrolling to the top shows the first line:\n{top}"
        );
        assert!(
            top.contains("follow off"),
            "manual scrolling switches follow off:\n{top}"
        );

        app.fullscreen = false;
        app.focus = Pane::Queue;
        for (width, height) in [(100, 30), (70, 20), (40, 10), (20, 5), (3, 2)] {
            let small = screen_text(&mut app, width, height);
            assert!(!small.contains("\x1b"), "{width}x{height}:\n{small}");
        }
        let narrow = screen_text(&mut app, 70, 20);
        assert!(
            !narrow.contains("PROJECT"),
            "narrow screens drop the project column:\n{narrow}"
        );
        assert!(
            narrow.contains("STATE"),
            "narrow screens keep the state column:\n{narrow}"
        );
    }

    #[test]
    fn terminal_rendering_drops_ansi_and_marks_other_controls() {
        assert_eq!(
            terminal_text(b"ok\x1b[31mred\x1b[0m\n\x07\xff"),
            "okred\n\u{fffd}\u{fffd}"
        );
        assert_eq!(terminal_text(b"a\tb\r\n"), "a\tb\r\n");
    }

    #[test]
    fn retry_gap_reseats_the_log_window_behind_new_committed_output() {
        assert_eq!(gap_resync_offset(100_000, 8_192, true, 65_536), Some(0));
        assert_eq!(
            gap_resync_offset(100_000, 80_000, true, 65_536),
            Some(14_464)
        );
        assert_eq!(gap_resync_offset(8_192, 8_192, true, 65_536), None);
    }

    #[test]
    fn clock_renders_local_time_and_elides_todays_date() {
        let clock = Clock {
            now_millis: 1_782_675_296_123,
            offset_minutes: 180,
        };
        assert_eq!(clock.exact(1_782_675_296_123), "2026-06-28 22:34:56.123");
        assert_eq!(clock.precise(1_782_675_296_123), "22:34:56.123");
        assert_eq!(clock.compact(1_782_675_296_123), "22:34:56");
        assert_eq!(clock.compact(1_782_675_296_123 - 86_400_000), "06-27 22:34");
        assert_eq!(clock.offset_label(), "UTC+03:00");
        let negative = Clock {
            now_millis: 0,
            offset_minutes: -330,
        };
        assert_eq!(negative.offset_label(), "UTC-05:30");
        assert_eq!(negative.exact(0), "1969-12-31 18:30:00.000");
    }

    #[test]
    fn civil_day_conversion_round_trips() {
        for days in [-719_468, -1, 0, 1, 19_000, 20_632, 2_932_896] {
            let (year, month, day) = civil_date_from_unix_days(days);
            assert_eq!(
                days_from_civil(year, month, day),
                days,
                "{year}-{month}-{day}"
            );
        }
        assert_eq!(days_from_civil(2026, 8, 29), 20_694);
    }

    #[test]
    fn durations_and_sizes_use_operator_units() {
        assert_eq!(format_duration(6_200), "6.2s");
        assert_eq!(format_duration(252_000), "4m12s");
        assert_eq!(format_duration(3_780_000), "1h03m");
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(12_697), "12.4K");
        assert_eq!(format_bytes(3 * 1_024 * 1_024), "3.0M");
    }

    #[test]
    fn text_fits_columns_with_an_ellipsis() {
        assert_eq!(ellipsize("observation_missing", 8), "observa…");
        assert_eq!(ellipsize("short", 8), "short");
        assert_eq!(pad_right("ab", 4), "ab  ");
        assert_eq!(pad_left("ab", 4), "  ab");
        let spans = vec![
            Span::styled("cmd.exe", text()),
            Span::styled(" /d /c echo", dim()),
        ];
        let fitted = fit_spans(spans.clone(), 10);
        assert_eq!(
            fitted
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>(),
            "cmd.exe /…"
        );
        let padded = fit_spans(spans, 24);
        assert_eq!(span_width(&padded), 24);
    }

    #[test]
    fn wrapping_counts_lines_exactly_and_indents_continuations() {
        let spans = vec![
            Span::styled("Command  ", dim()),
            Span::styled("abcdefghijklmnopqrstuvwxyz", text()),
        ];
        let lines = wrap_spans(&spans, 12, 4);
        let rendered: Vec<String> = lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                "Command  abc",
                "    defghijk",
                "    lmnopqrs",
                "    tuvwxyz"
            ]
        );
        assert_eq!(wrap_spans(&[], 12, 4).len(), 1);
    }

    #[test]
    fn claims_render_as_chips() {
        assert_eq!(format_claims(&ResourceClaims::default()), "");
        let mut claims = ResourceClaims {
            cpu_units: Some(4),
            ram_mb: Some(512),
            cargo_slots: Some(1),
            gpu_slots: None,
            ..ResourceClaims::default()
        };
        claims.custom.insert("review_slots".into(), 2);
        claims.exclusive_fences.push(r"C:\worktree".into());
        claims.impacts.push("cpu_heavy".into());
        assert_eq!(
            format_claims(&claims),
            "cpu 4 · ram 512M · cargo 1 · review_slots 2 · exclusive 1 · cpu_heavy"
        );
    }

    #[test]
    fn commands_split_into_executable_and_arguments() {
        assert_eq!(
            split_command("cargo test --workspace"),
            ("cargo", "test --workspace")
        );
        assert_eq!(
            split_command(r#""C:\Program Files\tool.exe" --flag"#),
            (r#""C:\Program Files\tool.exe""#, "--flag")
        );
        assert_eq!(split_command("solo"), ("solo", ""));
    }

    #[test]
    fn states_map_to_glyphs_and_semantic_colours() {
        let running = state_visual(JobState::Active, None);
        assert_eq!(
            (running.glyph, running.label, running.color),
            ("●", "Running", palette::RUNNING)
        );
        let failed = state_visual(JobState::Final, Some(JobOutcome::TimedOut));
        assert_eq!(
            (failed.glyph, failed.label, failed.color),
            ("⏱", "Timed out", palette::BAD)
        );
        let canceled = state_visual(JobState::Final, Some(JobOutcome::Canceled));
        assert_eq!(canceled.color, palette::MUTED);
        assert_eq!(project_color("stillyard"), project_color("stillyard"));
    }

    #[test]
    fn queue_groups_running_then_queued_then_finished_newest_first() {
        let app = app_with(vec![
            summary(
                "final",
                Some("succeeded"),
                1_000,
                Some(1_100),
                Some(2_000),
                None,
                Some("a"),
            ),
            summary("pending", None, 3_000, None, None, Some(2), Some("b")),
            summary("active", None, 500, Some(900), None, None, Some("a")),
            summary("pending", None, 2_500, None, None, Some(1), None),
            summary(
                "final",
                Some("failed"),
                1_500,
                Some(1_600),
                Some(5_000),
                None,
                Some("b"),
            ),
        ]);
        assert_eq!(app.ordered_jobs(), vec![2, 3, 1, 4, 0]);
        let rows = app.rows();
        assert_eq!(
            rows,
            vec![
                RowKind::Group {
                    bucket: Bucket::Running,
                    count: 1
                },
                RowKind::Job(2),
                RowKind::Group {
                    bucket: Bucket::Queued,
                    count: 2
                },
                RowKind::Job(3),
                RowKind::Job(1),
                RowKind::Group {
                    bucket: Bucket::Finished,
                    count: 2
                },
                RowKind::Job(4),
                RowKind::Job(0),
            ]
        );
        assert_eq!(app.selected, 2, "selection starts on the first running job");
        assert_eq!(app.projects(), vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn selection_walks_the_display_order_and_skips_group_rows() {
        let mut app = app_with(vec![
            summary(
                "final",
                Some("succeeded"),
                1_000,
                Some(1_100),
                Some(2_000),
                None,
                None,
            ),
            summary("pending", None, 3_000, None, None, Some(1), None),
            summary("active", None, 500, Some(900), None, None, None),
        ]);
        assert_eq!(app.selected, 2);
        app.select_relative(1);
        assert_eq!(app.selected, 1);
        app.select_relative(1);
        assert_eq!(app.selected, 0);
        app.select_relative(1);
        assert_eq!(app.selected, 0, "clamps at the end");
        app.select_relative(-5);
        assert_eq!(app.selected, 2, "clamps at the start");
        app.select_edge(true);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn project_filter_cycles_through_known_projects_and_keeps_a_visible_selection() {
        let mut app = app_with(vec![
            summary(
                "active",
                None,
                500,
                Some(900),
                None,
                None,
                Some("stillyard"),
            ),
            summary("pending", None, 3_000, None, None, Some(1), Some("lab")),
            summary("pending", None, 3_100, None, None, Some(2), None),
        ]);
        assert_eq!(app.selected, 0);
        app.cycle_project();
        assert_eq!(app.project_filter.as_deref(), Some("lab"));
        assert_eq!(app.ordered_jobs(), vec![1]);
        assert_eq!(app.selected, 1, "hidden selection moves to a visible job");
        app.cycle_project();
        assert_eq!(app.project_filter.as_deref(), Some("stillyard"));
        assert_eq!(app.selected, 0);
        app.cycle_project();
        assert_eq!(app.project_filter, None);
        assert_eq!(app.ordered_jobs(), vec![0, 1, 2]);
    }

    #[test]
    fn scrolling_clamps_to_content_and_follow_pins_logs_to_the_tail() {
        let mut app = app_with(Vec::new());
        app.detail_len = 25;
        app.detail_view = 10;
        app.scroll_detail(100);
        assert_eq!(app.detail_scroll, 15);
        app.scroll_detail(-100);
        assert_eq!(app.detail_scroll, 0);
        app.log_len = 40;
        app.log_view = 10;
        assert!(app.follow);
        app.scroll_logs(-1);
        assert!(!app.follow, "manual scrolling releases follow");
        app.switch_stream(LogStream::Stderr);
        assert!(app.follow);
        assert_eq!(app.log_scroll, 0);
        assert_eq!(clamp_scroll(7, 5, 10), 0);
    }

    #[test]
    fn queue_columns_shed_detail_on_narrow_terminals() {
        let wide: Vec<&str> = queue_columns(140)
            .0
            .iter()
            .map(|column| column.title)
            .collect();
        assert_eq!(
            wide,
            vec![
                "STATE", "PROJECT", "WHEN", "TIME", "CLAIMS", "COMMAND", "NOTE"
            ]
        );
        let narrow: Vec<&str> = queue_columns(70)
            .0
            .iter()
            .map(|column| column.title)
            .collect();
        assert_eq!(narrow, vec!["STATE", "WHEN", "TIME", "COMMAND"]);
    }

    #[test]
    fn edges_join_on_one_line_and_the_right_edge_yields_when_cramped() {
        let line = join_edges(vec![Span::raw("left")], vec![Span::raw("right")], 12);
        let rendered: String = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect();
        assert_eq!(rendered, "left   right");
        let cramped = join_edges(vec![Span::raw("left")], vec![Span::raw("right")], 6);
        let rendered: String = cramped
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect();
        assert_eq!(rendered, "left");
    }
}
