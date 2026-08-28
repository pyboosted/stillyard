use std::collections::VecDeque;
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
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap};
use stillyard::{
    Client, JobId, JobListPage, JobSelector, JobSnapshot, JobState, LogStream, ObservationFrame,
};

const LOG_WINDOW_BYTES: usize = 64 * 1024;

enum Message {
    Input(Event),
    Observation(stillyard::Result<ObservationFrame>),
    Status(String),
}

struct TerminalRestore;

impl Drop for TerminalRestore {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

struct App {
    page: JobListPage,
    selected: usize,
    detail: Option<JobSnapshot>,
    detail_job: Option<JobId>,
    stdout_offset: u64,
    stderr_offset: u64,
    stdout: VecDeque<u8>,
    stderr: VecDeque<u8>,
    status: String,
    last_refresh: Instant,
}

impl App {
    fn selected_job(&self) -> Option<JobId> {
        self.page.jobs.get(self.selected).map(|job| job.job_id)
    }

    fn select_relative(&mut self, delta: isize) {
        if self.page.jobs.is_empty() {
            self.selected = 0;
            return;
        }
        self.selected = self
            .selected
            .saturating_add_signed(delta)
            .min(self.page.jobs.len() - 1);
    }
}

pub(crate) fn run(
    client: Client,
    selector: JobSelector,
    limit: u32,
    deadline: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let limit = limit.clamp(1, stillyard::MAX_OBSERVATION_PAGE);
    let page = client.list(
        selector.clone(),
        None,
        limit,
        request_deadline(deadline),
        None,
    )?;
    let initial_cursor = page.event_cursor;
    let mut app = App {
        page,
        selected: 0,
        detail: None,
        detail_job: None,
        stdout_offset: 0,
        stderr_offset: 0,
        stdout: VecDeque::with_capacity(LOG_WINDOW_BYTES),
        stderr: VecDeque::with_capacity(LOG_WINDOW_BYTES),
        status: "connected".into(),
        last_refresh: Instant::now(),
    };
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
                match observer.observe(
                    selector.clone(),
                    Some(cursor),
                    stillyard::MAX_OBSERVATION_PAGE,
                    std::time::Duration::from_secs(30),
                    deadline,
                    None,
                ) {
                    Ok(frame) => {
                        cursor = frame.cursor();
                        if matches!(
                            &frame,
                            ObservationFrame::Events { events, .. } if events.is_empty()
                        ) && cursor == requested
                        {
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
            Message::Input(Event::Key(key)) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                KeyCode::Up | KeyCode::Char('k') => {
                    app.select_relative(-1);
                    if let Err(error) =
                        refresh_detail(&client, &mut app, request_deadline(deadline))
                    {
                        app.status = format!("stale after detail error: {error}");
                    } else {
                        app.last_refresh = Instant::now();
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.select_relative(1);
                    if let Err(error) =
                        refresh_detail(&client, &mut app, request_deadline(deadline))
                    {
                        app.status = format!("stale after detail error: {error}");
                    } else {
                        app.last_refresh = Instant::now();
                    }
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
                            app.last_refresh = Instant::now();
                        }
                        Err(error) => app.status = format!("stale after refresh error: {error}"),
                    }
                }
                _ => {}
            },
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
                    ObservationFrame::Gap { snapshot, .. } => {
                        replace_page(&mut app, snapshot);
                        refresh_detail(&client, &mut app, refresh_deadline)
                    }
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
                    app.last_refresh = Instant::now();
                }
            }
            Message::Observation(Err(error)) => {
                app.status = format!("stale: observer stopped: {error}");
            }
            Message::Status(status) => app.status = status,
        }
    }
}

fn request_deadline(overall: Instant) -> Instant {
    overall.min(Instant::now() + std::time::Duration::from_secs(2))
}

fn replace_page(app: &mut App, page: JobListPage) {
    let selected = app.selected_job();
    app.page = page;
    app.selected = selected
        .and_then(|job_id| app.page.jobs.iter().position(|job| job.job_id == job_id))
        .unwrap_or(0)
        .min(app.page.jobs.len().saturating_sub(1));
}

fn refresh_page(
    client: &Client,
    app: &mut App,
    selector: &JobSelector,
    limit: u32,
    deadline: Instant,
) -> stillyard::Result<()> {
    let selected = app.selected_job();
    app.page = client.list(selector.clone(), None, limit, deadline, None)?;
    app.selected = selected
        .and_then(|job_id| app.page.jobs.iter().position(|job| job.job_id == job_id))
        .unwrap_or(0)
        .min(app.page.jobs.len().saturating_sub(1));
    Ok(())
}

fn refresh_detail(client: &Client, app: &mut App, deadline: Instant) -> stillyard::Result<()> {
    let selected = app.selected_job();
    if selected != app.detail_job {
        app.detail_job = selected;
        app.stdout.clear();
        app.stderr.clear();
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
    let chunk = client.logs(
        job_id,
        stream,
        *offset,
        LOG_WINDOW_BYTES as u32,
        deadline,
        None,
    )?;
    let gap = chunk.gap;
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

fn draw(frame: &mut ratatui::Frame<'_>, app: &mut App) {
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(44),
            Constraint::Percentage(24),
            Constraint::Percentage(28),
            Constraint::Length(2),
        ])
        .split(frame.area());

    let rows = app.page.jobs.iter().map(|job| {
        let state = match (job.state, job.outcome) {
            (JobState::Final, Some(outcome)) => format!("{outcome:?}"),
            (state, _) => format!("{state:?}"),
        };
        let estimate = job
            .estimate
            .start_in_millis
            .map(|millis| format!("{:.1}s", millis as f64 / 1000.0))
            .unwrap_or_else(|| "?".into());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();
        let elapsed_from = job.started_unix_millis.unwrap_or(job.accepted_unix_millis);
        let elapsed_to = job.finished_unix_millis.unwrap_or(now);
        let elapsed = format!(
            "{:.1}s",
            elapsed_to.saturating_sub(elapsed_from) as f64 / 1000.0
        );
        let claims = format!(
            "cpu:{:?} ram:{:?} cargo:{:?} gpu:{:?} custom:{} fences:{} impacts:{}",
            job.claims.cpu_units,
            job.claims.ram_mb,
            job.claims.cargo_slots,
            job.claims.gpu_slots,
            job.claims.custom.len(),
            job.claims.shared_fences.len() + job.claims.exclusive_fences.len(),
            job.claims.impacts.len()
        );
        Row::new([
            Cell::from(state),
            Cell::from(
                job.queue_rank
                    .map(|rank| rank.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::from(estimate),
            Cell::from(elapsed),
            Cell::from(claims),
            Cell::from(job.job_id.entity_uuid().to_string()),
            Cell::from(
                job.blocker
                    .as_ref()
                    .map(|blocker| blocker.code.clone())
                    .unwrap_or_else(|| "-".into()),
            ),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(13),
            Constraint::Length(5),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(34),
            Constraint::Length(36),
            Constraint::Min(16),
        ],
    )
    .header(
        Row::new([
            "state", "rank", "start", "elapsed", "claims", "job", "blocker",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title(" Stillyard queue ")
            .borders(Borders::ALL),
    )
    .row_highlight_style(Style::default().bg(Color::DarkGray))
    .highlight_symbol("▶ ");
    let mut table_state =
        TableState::default().with_selected((!app.page.jobs.is_empty()).then_some(app.selected));
    frame.render_stateful_widget(table, areas[0], &mut table_state);

    let detail = app.detail.as_ref().map_or_else(
        || Text::from("No Job selected"),
        |job| {
            let mut lines = vec![
                Line::from(format!("Job: {}", job.job_id)),
                Line::from(format!(
                    "State: {:?}  Outcome: {:?}",
                    job.state, job.outcome
                )),
                Line::from(format!(
                    "Parent: {:?}  Batch: {:?} / {:?}",
                    job.parent, job.batch_id, job.batch_member
                )),
            ];
            for attempt in &job.attempts {
                lines.push(Line::from(format!(
                    "Attempt {}: {:?} ({} invocation(s))",
                    attempt.attempt_index,
                    attempt.verdict,
                    attempt.invocations.len()
                )));
                for invocation in &attempt.invocations {
                    lines.push(Line::from(format!(
                        "  {:?}[{}] {:?}, exit {:?}/{:?}, containment {:?}, incident {:?}",
                        invocation.role,
                        invocation.role_index,
                        invocation.state,
                        invocation.root_exit_code,
                        invocation.exit_classification,
                        invocation.containment.state,
                        invocation.containment.incident_id
                    )));
                }
            }
            Text::from(lines)
        },
    );
    frame.render_widget(
        Paragraph::new(detail)
            .block(Block::default().title(" Detail ").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        areas[1],
    );

    let logs = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(areas[2]);
    frame.render_widget(
        Paragraph::new(terminal_text(app.stdout.make_contiguous()))
            .block(Block::default().title(" stdout ").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        logs[0],
    );
    frame.render_widget(
        Paragraph::new(terminal_text(app.stderr.make_contiguous()))
            .block(Block::default().title(" stderr ").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        logs[1],
    );
    frame.render_widget(
        Paragraph::new(format!(
            "↑/↓ navigate  r refresh  q detach    {}",
            app.status
        )),
        areas[3],
    );
}

fn terminal_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .chars()
        .map(|character| match character {
            '\n' | '\r' | '\t' => character,
            character if character.is_control() => '\u{fffd}',
            character => character,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::terminal_text;

    #[test]
    fn terminal_rendering_replaces_controls_without_touching_line_structure() {
        assert_eq!(
            terminal_text(b"ok\x1b[31m\n\xff"),
            "ok\u{fffd}[31m\n\u{fffd}"
        );
    }
}
