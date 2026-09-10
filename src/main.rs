use std::fs::File;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand, ValueEnum, error::ErrorKind};
use stillyard::{
    AttemptVerdict, BatchSpec, Client, EnsureOptions, EnsureOutcome, EnsureReport, EnsuredBatch,
    EnsuredJob, ExitSource, JobId, JobOutcome, JobSnapshot, JobSpec, JobTreePage, LogStream,
    RecoveryResult, SubmitOptions, WaitOutcome, WaitReport,
};
use uuid::Uuid;

#[cfg(windows)]
mod bootstrap_controller;
mod tui;

#[derive(Debug, Parser)]
#[command(name = "stillyard", version, about)]
struct Cli {
    /// Select a daemon instance. Explicit endpoints are connect-only and never auto-start.
    #[arg(long, global = true)]
    endpoint: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[cfg(target_os = "linux")]
    /// Prepare a stopped WSL store or apply its owner-only pairing configuration.
    WslInstall {
        /// Select an isolated store; requires --endpoint as well.
        #[arg(long)]
        store: Option<PathBuf>,
        /// Owner-only JSON configuration from explicit Windows domain registration.
        #[arg(long)]
        configuration: Option<PathBuf>,
    },
    #[cfg(target_os = "linux")]
    #[command(hide = true)]
    LinuxExecutorStub {
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        control: PathBuf,
    },
    /// Run the per-user scheduler daemon in the foreground.
    Daemon {
        /// Internal marker used by client auto-start.
        #[arg(long, hide = true)]
        background_child: bool,
        /// Use an isolated canonical store root instead of the per-user default.
        #[arg(long)]
        store: Option<PathBuf>,
    },
    /// Atomically submit or recover one Job/Batch without a shell-side state machine.
    Ensure {
        /// JSON JobSpec path, or '-' for stdin.
        #[arg(long, required_unless_present = "batch", conflicts_with = "batch")]
        spec: Option<PathBuf>,
        /// Atomic BatchSpec JSON path, or '-' for stdin.
        #[arg(long, conflicts_with = "spec")]
        batch: Option<PathBuf>,
        /// Stable logical operation identity.
        #[arg(long)]
        idempotency_key: Uuid,
        /// Optional small atomic projection of the durable Submission receipt.
        #[arg(long)]
        result_file: Option<PathBuf>,
        /// Wait until final or the client deadline; never cancels accepted work.
        #[arg(long)]
        wait: bool,
        /// Replay committed canonical stdout/stderr for a final single Job.
        #[arg(long, requires = "wait")]
        passthrough: bool,
        /// Emit no scheduler JSON; requires a result file.
        #[arg(long, requires = "result_file")]
        silent: bool,
        /// Client-only waiting deadline.
        #[arg(long, default_value_t = 86_400)]
        deadline_seconds: u64,
    },
    /// Submit a JobSpec JSON document and print its receipt immediately.
    Submit {
        /// JSON spec path, or '-' for stdin.
        #[arg(long, required_unless_present = "batch", conflicts_with = "batch")]
        spec: Option<PathBuf>,
        /// Atomic BatchSpec JSON path, or '-' for stdin.
        #[arg(long, conflicts_with = "spec")]
        batch: Option<PathBuf>,
        /// Stable operation identity; generated when omitted.
        #[arg(long)]
        idempotency_key: Option<Uuid>,
        /// Durable receipt path for managed callers.
        #[arg(long)]
        result_file: Option<PathBuf>,
        /// Wait for the terminal snapshot after printing the receipt.
        #[arg(long)]
        wait: bool,
        /// Stream committed canonical stdout/stderr while waiting (single Job only).
        #[arg(long, requires = "wait")]
        passthrough: bool,
        /// Emit no scheduler JSON; requires a durable result file.
        #[arg(long, requires = "result_file")]
        silent: bool,
        /// Overall client deadline.
        #[arg(long, default_value_t = 86_400)]
        deadline_seconds: u64,
    },
    /// Recover a previously prepared or completed submission without creating work.
    Recover {
        #[arg(long)]
        result_file: PathBuf,
        #[arg(long)]
        wait: bool,
        #[arg(long, requires = "wait")]
        passthrough: bool,
        #[arg(long)]
        silent: bool,
        #[arg(long, default_value_t = 86_400)]
        deadline_seconds: u64,
    },
    /// Print the current durable state of one job.
    Status {
        job_id: JobId,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// List a bounded page of retained Jobs.
    List {
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[arg(long)]
        cursor: Option<stillyard::JobListCursor>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        tree: bool,
        #[arg(long, default_value_t = 100, requires = "tree")]
        root_limit: u32,
        #[arg(long, default_value_t = 256, requires = "tree")]
        node_limit: u32,
        #[arg(long, requires = "tree")]
        depth: Option<u32>,
        #[arg(long, requires = "tree", conflicts_with = "json")]
        ascii: bool,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Show the retained managed family containing one Job.
    Tree {
        job_id: JobId,
        #[arg(long, default_value_t = 256)]
        node_limit: u32,
        #[arg(long)]
        depth: Option<u32>,
        #[arg(long)]
        json: bool,
        #[arg(long, conflicts_with = "json")]
        ascii: bool,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Read durable scheduler events after a cursor.
    Events {
        #[arg(long)]
        since: Option<stillyard::EventCursor>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long, default_value_t = 256)]
        limit: u32,
        #[arg(long, required = true)]
        json: bool,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Wait for one job to become terminal.
    Wait {
        job_id: JobId,
        /// Stream committed canonical stdout/stderr while waiting.
        #[arg(long)]
        passthrough: bool,
        #[arg(long, default_value_t = 86_400)]
        deadline_seconds: u64,
    },
    /// Read committed bytes from a job's canonical log.
    Logs {
        job_id: JobId,
        /// Read canonical stdout (the default).
        #[arg(long, conflicts_with_all = ["stderr", "stream"])]
        stdout: bool,
        /// Read canonical stderr.
        #[arg(long, conflicts_with_all = ["stdout", "stream"])]
        stderr: bool,
        /// Backward-compatible explicit stream spelling.
        #[arg(long, value_enum, conflicts_with_all = ["stdout", "stderr"])]
        stream: Option<StreamArg>,
        #[arg(long, visible_alias = "since", default_value_t = 0)]
        offset: u64,
        /// Continue until the selected canonical stream reaches EOF.
        #[arg(long)]
        follow: bool,
        #[arg(long, default_value_t = 1_048_576)]
        limit: u32,
        /// Emit the complete LogChunk JSON instead of its byte payload.
        #[arg(long)]
        json: bool,
        #[arg(long)]
        deadline_seconds: Option<u64>,
    },
    /// Open the disposable event-driven terminal monitor.
    Watch {
        #[arg(long, conflicts_with_all = ["batch", "labels"])]
        job: Option<JobId>,
        #[arg(long, conflicts_with_all = ["job", "labels"])]
        batch: Option<stillyard::BatchId>,
        #[arg(long = "label", conflicts_with_all = ["job", "batch"])]
        labels: Vec<String>,
        #[arg(long, default_value_t = 200)]
        limit: u32,
        #[arg(long, default_value_t = 86_400)]
        deadline_seconds: u64,
    },
    /// Print daemon identity and queue counts.
    DaemonStatus {
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Pair and inspect machine executors through the selected authority.
    Machine {
        #[command(subcommand)]
        command: MachineCommand,
    },
    /// Transitional WSL bootstrap with durable Windows admission protection.
    Bootstrap {
        #[command(subcommand)]
        command: BootstrapCommand,
    },
    /// Inspect or explicitly administer the durable machine admission interlock.
    Authority {
        #[command(subcommand)]
        command: AuthorityCommand,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Print the daemon-authenticated submission context for this process.
    Context {
        /// Emit the public SubmissionContext JSON document.
        #[arg(long, required = true)]
        json: bool,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Inspect daemon, host, store, configuration, and unresolved containment evidence.
    Doctor {
        #[command(subcommand)]
        command: Option<DoctorCommand>,
        #[arg(long)]
        incident_cursor: Option<stillyard::ContainmentIncidentCursor>,
        #[arg(long)]
        incident_limit: Option<u32>,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Cancel explicitly named Jobs without selecting children or dependency successors.
    Cancel {
        #[arg(required = true)]
        jobs: Vec<JobId>,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Print generated public schemas.
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StreamArg {
    Stdout,
    Stderr,
}

impl From<StreamArg> for LogStream {
    fn from(value: StreamArg) -> Self {
        match value {
            StreamArg::Stdout => Self::Stdout,
            StreamArg::Stderr => Self::Stderr,
        }
    }
}

#[derive(Debug, Subcommand)]
enum SchemaCommand {
    /// Print the versioned JobSpec/BatchSpec schema.
    Spec,
    /// Print the versioned host resource-capacity schema.
    Config,
    /// Print typed ensure/wait/primary-result records.
    ManagedExecution,
    /// Print scoped machine-resource accounting and authority identities.
    MachineScheduling,
    /// Typed participant protocol, tickets and allocation views.
    MachineProtocol,
}

#[derive(Debug, Subcommand)]
enum MachineCommand {
    /// Serve bounded binary protocol frames on stdin/stdout until the transport closes.
    Bridge,
    /// Retire a failed manager identity; outstanding rights need explicit risk acceptance in the spec.
    RetireDomain {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Preview exact outstanding rights before owner-audited domain retirement.
    ClearancePreview { domain: Uuid },
    /// Reconstruct lost coordinator inventory and verify native cleanup; never force-clear.
    Recover,
    /// Read ordered machine allocation events; cursor is a retained page cursor JSON file.
    Events {
        #[arg(long)]
        cursor: Option<PathBuf>,
        #[arg(long, default_value_t = 128)]
        limit: u32,
    },
    /// Exchange one sequenced protocol operation through the installed bridge.
    Exchange {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Explicitly pair an executor from an owner-controlled registration file.
    Pair {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Begin authentication through this installed bridge executable.
    ConnectBegin {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Complete a pending handshake; the file contains challenge and tag.
    ConnectFinish {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Inspect a participant without exposing its pairing secret.
    Participant { domain: Uuid },
}

#[derive(Debug, Subcommand)]
enum BootstrapCommand {
    /// Run a bounded Linux descriptor from a native Job with durable authority protection.
    Run {
        #[arg(long)]
        spec: PathBuf,
        /// Native acceptance companion in this primary's Job; requires a finite Job timeout.
        #[cfg(windows)]
        #[arg(long, hide = true)]
        native_controller: Option<PathBuf>,
        /// Arguments for the native companion, following --.
        #[cfg(windows)]
        #[arg(last = true, requires = "native_controller")]
        controller_args: Vec<std::ffi::OsString>,
    },
    /// Inspect the recorded Linux seal and release a retained bootstrap obligation.
    Reconcile { operation_id: uuid::Uuid },
}

#[derive(Debug, Subcommand)]
enum AuthorityCommand {
    /// Inspect the durable admission blocker and retained audit records.
    Status,
    /// Declare a new authority only after verifying there is no outstanding work.
    Initialize {
        #[arg(long, required = true)]
        confirm_no_outstanding_work: bool,
    },
    /// Stop new admission after all existing Leases have safely completed.
    Hold {
        id: uuid::Uuid,
        #[arg(long)]
        reason: String,
    },
    /// Accept the risk of releasing a hold without automatic cleanup proof.
    ForceRelease {
        id: uuid::Uuid,
        #[arg(long, required = true)]
        force: bool,
        #[arg(long)]
        reason: String,
    },
}

#[derive(Debug, Subcommand)]
enum DoctorCommand {
    /// Explicitly accept the risk of an uncertain containment whose root is proven absent.
    ClearContainment {
        containment_id: stillyard::ContainmentId,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let display_only = matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            );
            let _ = error.print();
            std::process::exit(if display_only { 0 } else { 64 });
        }
    };
    if let Err(error) = execute(cli) {
        eprintln!("stillyard: {error}");
        std::process::exit(error_exit_code(error.as_ref()));
    }
}

fn execute(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let Cli { endpoint, command } = cli;
    match command {
        #[cfg(target_os = "linux")]
        Command::WslInstall {
            store,
            configuration,
        } => {
            print_json(&stillyard::configure_wsl_attachment(
                store,
                endpoint,
                configuration,
            )?)?;
        }
        #[cfg(target_os = "linux")]
        Command::LinuxExecutorStub { spec, control } => {
            stillyard::run_linux_executor_stub(&spec, &control)?;
        }
        Command::Daemon {
            background_child,
            store,
        } => {
            let _ = background_child;
            stillyard::run_daemon_instance(store, endpoint)?;
        }
        Command::Ensure {
            spec,
            batch,
            idempotency_key,
            result_file,
            wait,
            passthrough,
            silent,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let selected_endpoint = endpoint.as_deref().ok_or_else(|| {
                stillyard::Error::InvalidSpec(
                    "stillyard ensure requires an explicit --endpoint".into(),
                )
            })?;
            let client = connect_client(Some(selected_endpoint), deadline)?;
            let mut options = EnsureOptions::new(idempotency_key);
            if let Some(result_file) = result_file {
                options = options.with_result_file(result_file);
            }
            if wait {
                options = options.with_wait_for_completion();
            }
            if let Some(path) = batch {
                if passthrough {
                    return Err("--passthrough is valid only for a single Job".into());
                }
                let input = read_input(&path)?;
                require_current_spec_version(&input)?;
                let spec: BatchSpec = serde_json::from_slice(&input)?;
                let outcome = client.ensure_batch(spec, &options, deadline, None)?;
                let report = ensure_batch_report(outcome);
                if !silent {
                    print_json(&report)?;
                }
                exit_for_code(report.exit_code);
            } else {
                let path = spec.expect("clap requires --spec or --batch");
                let input = read_input(&path)?;
                require_current_spec_version(&input)?;
                let spec: JobSpec = serde_json::from_slice(&input)?;
                let mut outcome = client.ensure_job(spec, &options, deadline, None)?;
                if passthrough {
                    if let EnsureOutcome::Final(ensured) = &outcome {
                        let streamed = client.wait_with_passthrough_outcome(
                            ensured.receipt.job_id,
                            &mut 0,
                            &mut 0,
                            &mut io::stdout(),
                            &mut io::stderr(),
                            deadline,
                            None,
                        )?;
                        if let WaitOutcome::Final { snapshot, .. } = streamed {
                            if let EnsureOutcome::Final(ensured) = &mut outcome {
                                ensured.snapshot = Some(snapshot);
                            }
                        }
                    }
                }
                let report = ensure_job_report(outcome);
                if !silent {
                    if passthrough {
                        print_json_stderr(&report)?;
                    } else {
                        print_json(&report)?;
                    }
                }
                exit_for_code(report.exit_code);
            }
        }
        Command::Submit {
            spec,
            batch,
            idempotency_key,
            result_file,
            wait,
            passthrough,
            silent,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let mut options = SubmitOptions::new(idempotency_key.unwrap_or_else(Uuid::now_v7));
            if let Some(result_file) = result_file {
                options = options.with_result_file(result_file);
            }
            if wait {
                options = options.with_wait_for_completion();
            }
            if let Some(path) = batch {
                if passthrough {
                    return Err("--passthrough is valid only for a single Job".into());
                }
                let input = read_input(&path)?;
                require_current_spec_version(&input)?;
                let spec: BatchSpec = serde_json::from_slice(&input)?;
                let receipt = client.submit_batch(spec, &options, deadline, None)?;
                if !silent {
                    print_json(&receipt)?;
                }
                if wait {
                    let mut worst = (0_u8, 0_i32);
                    for member in receipt.jobs {
                        let snapshot = client.wait(member.receipt.job_id, deadline, None)?;
                        if !silent {
                            print_json(&snapshot)?;
                        }
                        let candidate = snapshot_exit_rank(&snapshot);
                        if candidate.0 > worst.0 {
                            worst = candidate;
                        }
                    }
                    if worst.1 != 0 {
                        std::process::exit(worst.1);
                    }
                }
            } else {
                let path = spec.expect("clap requires --spec or --batch");
                let input = read_input(&path)?;
                require_current_spec_version(&input)?;
                let spec: JobSpec = serde_json::from_slice(&input)?;
                let receipt = client.submit(spec, &options, deadline, None)?;
                if !silent {
                    if passthrough {
                        print_json_stderr(&receipt)?;
                    } else {
                        print_json(&receipt)?;
                    }
                }
                if wait {
                    let snapshot = if passthrough {
                        client.wait_with_passthrough(
                            receipt.job_id,
                            &mut 0,
                            &mut 0,
                            &mut io::stdout(),
                            &mut io::stderr(),
                            deadline,
                            None,
                        )?
                    } else {
                        client.wait(receipt.job_id, deadline, None)?
                    };
                    if !silent {
                        if passthrough {
                            print_json_stderr(&snapshot)?;
                        } else {
                            print_json(&snapshot)?;
                        }
                    }
                    exit_for_snapshot(&snapshot);
                }
            }
        }
        Command::Recover {
            result_file,
            wait,
            passthrough,
            silent,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let mut recovery = client.recover_result_file(&result_file, deadline, None)?;
            while wait && matches!(recovery, RecoveryResult::Received { .. }) {
                std::thread::sleep(Duration::from_millis(100));
                recovery = client.recover_result_file(&result_file, deadline, None)?;
            }
            if !silent {
                if passthrough {
                    print_json_stderr(&recovery)?;
                } else {
                    print_json(&recovery)?;
                }
            }
            if wait {
                match &recovery {
                    RecoveryResult::Accepted(receipt) => {
                        let snapshot = if passthrough {
                            client.wait_with_passthrough(
                                receipt.job_id,
                                &mut 0,
                                &mut 0,
                                &mut io::stdout(),
                                &mut io::stderr(),
                                deadline,
                                None,
                            )?
                        } else {
                            client.wait(receipt.job_id, deadline, None)?
                        };
                        if !silent {
                            if passthrough {
                                print_json_stderr(&snapshot)?;
                            } else {
                                print_json(&snapshot)?;
                            }
                        }
                        exit_for_snapshot(&snapshot);
                    }
                    RecoveryResult::AcceptedBatch(batch) => {
                        if passthrough {
                            return Err("--passthrough is valid only for a single Job".into());
                        }
                        let mut worst = (0_u8, 0_i32);
                        for member in &batch.jobs {
                            let snapshot = client.wait(member.receipt.job_id, deadline, None)?;
                            if !silent {
                                print_json(&snapshot)?;
                            }
                            let candidate = snapshot_exit_rank(&snapshot);
                            if candidate.0 > worst.0 {
                                worst = candidate;
                            }
                        }
                        if worst.1 != 0 {
                            std::process::exit(worst.1);
                        }
                    }
                    _ => {}
                }
            }
            exit_for_recovery(&recovery);
        }
        Command::Status {
            job_id,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            print_json(&client.status(job_id, deadline, None)?)?;
        }
        Command::List {
            labels,
            limit,
            cursor,
            json,
            tree,
            root_limit,
            node_limit,
            depth,
            ascii,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let selector = labels_selector(&labels)?;
            if tree {
                if cursor.is_some() {
                    return Err("--cursor is valid only for flat list".into());
                }
                let page = client.tree(
                    selector, None, root_limit, node_limit, depth, deadline, None,
                )?;
                if json {
                    print_json(&page)?;
                } else {
                    print_job_tree(&page, ascii);
                }
            } else {
                let page = client.list(selector, cursor, limit, deadline, None)?;
                if json {
                    print_json(&page)?;
                } else {
                    print_job_list(&page);
                }
            }
        }
        Command::Tree {
            job_id,
            node_limit,
            depth,
            json,
            ascii,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let page = client.tree_for_job(job_id, node_limit, depth, deadline, None)?;
            if json {
                print_json(&page)?;
            } else {
                print_job_tree(&page, ascii);
            }
        }
        Command::Events {
            since,
            labels,
            limit,
            json,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let frame = client.observe(
                labels_selector(&labels)?,
                since,
                limit,
                Duration::ZERO,
                deadline,
                None,
            )?;
            let _ = json;
            print_json(&frame)?;
        }
        Command::Wait {
            job_id,
            passthrough,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let outcome = if passthrough {
                client.wait_with_passthrough_outcome(
                    job_id,
                    &mut 0,
                    &mut 0,
                    &mut io::stdout(),
                    &mut io::stderr(),
                    deadline,
                    None,
                )?
            } else {
                client.wait_outcome(job_id, deadline, None)
            };
            let report = wait_report(outcome);
            if passthrough {
                print_json_stderr(&report)?;
            } else {
                print_json(&report)?;
            }
            exit_for_code(report.exit_code);
        }
        Command::Logs {
            job_id,
            stdout: _,
            stderr,
            stream,
            offset,
            follow,
            limit,
            json,
            deadline_seconds,
        } => {
            let deadline = deadline(logs_deadline_seconds(follow, deadline_seconds));
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let stream = if stderr {
                LogStream::Stderr
            } else {
                stream.unwrap_or(StreamArg::Stdout).into()
            };
            if follow {
                for chunk in client.follow_logs(job_id, stream, offset, deadline, None)? {
                    let chunk = chunk?;
                    if let Some(gap) = chunk
                        .gap
                        .as_deref()
                        .filter(|_| chunk.next_offset == chunk.offset && !json)
                    {
                        return Err(stillyard::Error::Protocol(gap.to_owned()).into());
                    }
                    if json {
                        print_json(&chunk)?;
                    } else {
                        io::stdout().write_all(&chunk.bytes)?;
                        io::stdout().flush()?;
                    }
                }
            } else {
                let chunk = client.logs(job_id, stream, offset, limit, deadline, None)?;
                if json {
                    print_json(&chunk)?;
                } else {
                    io::stdout().write_all(&chunk.bytes)?;
                }
            }
        }
        Command::Watch {
            job,
            batch,
            labels,
            limit,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let selector = if let Some(job_id) = job {
                stillyard::JobSelector::Jobs {
                    job_ids: vec![job_id],
                }
            } else if let Some(batch_id) = batch {
                stillyard::JobSelector::Batch { batch_id }
            } else {
                labels_selector(&labels)?
            };
            tui::run(client, selector, limit, deadline)?;
        }
        Command::DaemonStatus { deadline_seconds } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            print_json(&client.daemon_status(deadline, None)?)?;
        }
        Command::Machine { command } => {
            let limit = deadline(10);
            let selected = endpoint.as_deref().ok_or_else(|| {
                stillyard::Error::InvalidSpec(
                    "machine protocol operations require an explicit --endpoint".into(),
                )
            })?;
            let client = connect_client(Some(selected), limit)?;
            match command {
                MachineCommand::Bridge => {
                    #[cfg(windows)]
                    stillyard::machine::bridge::run(&client)?;
                    #[cfg(not(windows))]
                    return Err(stillyard::Error::UnsupportedPlatform(std::env::consts::OS).into());
                }
                MachineCommand::RetireDomain { spec } => {
                    print_json(&client.retire_machine_domain(read_machine_input(&spec)?, limit)?)?
                }
                MachineCommand::ClearancePreview { domain } => print_json(
                    &client
                        .machine_clearance_preview(stillyard::ExecutionDomainId(domain), limit)?,
                )?,
                MachineCommand::Recover => print_json(&client.machine_recover(limit)?)?,
                MachineCommand::Pair { spec } => {
                    print_json(&client.pair_machine_domain(read_machine_input(&spec)?, limit)?)?
                }
                MachineCommand::Events {
                    cursor,
                    limit: page_limit,
                } => {
                    let cursor = cursor
                        .as_ref()
                        .map(|path| read_machine_input(path))
                        .transpose()?;
                    print_json(&client.machine_events(cursor, page_limit, limit)?)?;
                }
                MachineCommand::Exchange { spec } => {
                    print_json(&client.machine_exchange(read_machine_input(&spec)?, limit)?)?
                }
                MachineCommand::ConnectBegin { spec } => {
                    print_json(&client.machine_connect_begin(read_machine_input(&spec)?, limit)?)?
                }
                MachineCommand::ConnectFinish { spec } => {
                    #[derive(serde::Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Response {
                        challenge: stillyard::machine::ConnectChallenge,
                        tag: [u8; 32],
                    }
                    let response: Response = read_machine_input(&spec)?;
                    print_json(&client.machine_connect_finish(
                        response.challenge,
                        response.tag,
                        limit,
                    )?)?;
                }
                MachineCommand::Participant { domain } => print_json(
                    &client.machine_participant(stillyard::ExecutionDomainId(domain), limit)?,
                )?,
            }
        }
        Command::Authority {
            command,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            let snapshot = match command {
                AuthorityCommand::Status => client.authority_status(deadline, None)?,
                AuthorityCommand::Initialize {
                    confirm_no_outstanding_work: true,
                } => client.initialize_authority_without_outstanding_work(deadline, None)?,
                AuthorityCommand::Hold { id, reason } => {
                    client.hold_authority(id, reason, deadline, None)?
                }
                AuthorityCommand::ForceRelease {
                    id,
                    reason,
                    force: true,
                } => client.force_release_authority(id, reason, deadline, None)?,
                _ => {
                    return Err(stillyard::Error::InvalidSpec(
                        "authority mutation requires explicit owner confirmation".into(),
                    )
                    .into());
                }
            };
            print_json(&snapshot)?;
        }
        Command::Bootstrap { command } => {
            let client = connect_client(endpoint.as_deref(), deadline(10))?;
            match command {
                BootstrapCommand::Run {
                    spec,
                    #[cfg(windows)]
                    native_controller,
                    #[cfg(windows)]
                    controller_args,
                } => {
                    let bytes = std::fs::read(spec)?;
                    if bytes.len() > 8192 {
                        return Err(stillyard::Error::InvalidSpec(
                            "bootstrap descriptor exceeds 8192 bytes".into(),
                        )
                        .into());
                    }
                    let work: stillyard::BootstrapWork = serde_json::from_slice(&bytes)?;
                    #[cfg(windows)]
                    let controller = native_controller
                        .as_ref()
                        .map(|executable| {
                            bootstrap_controller::Controller::start(
                                &client,
                                executable,
                                &controller_args,
                                Duration::from_secs(work.timeout_seconds.saturating_add(30)),
                            )
                        })
                        .transpose()?;
                    let proof = client.run_wsl_bootstrap(work)?;
                    let code = match proof.termination.as_str() {
                        "exited" => {
                            if proof.root_exit_code < 0 {
                                128 + proof.root_exit_code.saturating_abs().min(127)
                            } else {
                                proof.root_exit_code.min(255)
                            }
                        }
                        "canceled" => 130,
                        "timed_out" => 124,
                        _ => 125,
                    };
                    #[cfg(windows)]
                    let code = if code == 0 {
                        controller
                            .map(bootstrap_controller::Controller::finish)
                            .transpose()?
                            .unwrap_or(0)
                    } else {
                        drop(controller);
                        code
                    };
                    if code != 0 {
                        std::process::exit(code);
                    }
                }
                BootstrapCommand::Reconcile { operation_id } => {
                    print_json(&client.reconcile_wsl_bootstrap(operation_id)?)?
                }
            }
        }
        Command::Context {
            json: _,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            print_json(&client.submission_context(deadline, None)?)?;
        }
        Command::Doctor {
            command,
            incident_cursor,
            incident_limit,
            json,
            deadline_seconds,
        } => match command {
            None => {
                let deadline = deadline(deadline_seconds);
                let client = connect_client(endpoint.as_deref(), deadline)?;
                let snapshot = client.doctor(incident_cursor, incident_limit, deadline, None)?;
                if json {
                    print_json(&snapshot)?;
                } else {
                    print_doctor(&snapshot);
                }
            }
            Some(DoctorCommand::ClearContainment {
                containment_id,
                force,
                json,
                deadline_seconds,
            }) => {
                if !force {
                    return Err(stillyard::Error::InvalidSpec(
                        "doctor clear-containment requires explicit --force".into(),
                    )
                    .into());
                }
                let deadline = deadline(deadline_seconds);
                let client = connect_client(endpoint.as_deref(), deadline)?;
                let request_started_unix_millis = unix_millis();
                let result = client.force_clear_containment(containment_id, deadline, None)?;
                if json {
                    print_json(&result)?;
                } else {
                    println!(
                        "{}",
                        clearance_human_message(&result, request_started_unix_millis)
                    );
                }
            }
        },
        Command::Cancel {
            jobs,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = connect_client(endpoint.as_deref(), deadline)?;
            print_json(&client.cancel(&jobs, deadline, None)?)?;
        }
        Command::Schema { command } => match command {
            SchemaCommand::MachineProtocol => {
                print!("{}", stillyard::machine_protocol_schema_json()?)
            }
            SchemaCommand::MachineScheduling => {
                print!("{}", stillyard::machine_scheduling_schema_json()?)
            }
            SchemaCommand::Spec => print!("{}", stillyard::schema_json()?),
            SchemaCommand::Config => print!("{}", stillyard::config_schema_json()?),
            SchemaCommand::ManagedExecution => {
                print!("{}", stillyard::managed_execution_schema_json()?)
            }
        },
    }
    Ok(())
}

fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn clearance_human_message(
    result: &stillyard::ClearContainmentResult,
    request_started_unix_millis: i64,
) -> String {
    let happened_in_this_call = match result.audit.forced.as_ref() {
        Some(forced) => current_process_matches(&forced.requester),
        None => result.audit.resolved_unix_millis >= request_started_unix_millis,
    };
    let action = if happened_in_this_call {
        "cleared now".to_owned()
    } else {
        match (&result.audit.origin, result.audit.forced.as_ref()) {
            (stillyard::ClearanceOrigin::Automatic, _) => format!(
                "already cleared automatically at {}",
                result.audit.resolved_unix_millis
            ),
            (stillyard::ClearanceOrigin::Forced, Some(forced)) => format!(
                "already force-cleared by {} at {}",
                process_identity_summary(&forced.requester),
                forced.requested_unix_millis
            ),
            _ => format!("already cleared at {}", result.audit.resolved_unix_millis),
        }
    };
    format!(
        "{}: {action}; lease_released={}",
        result.containment_id, result.audit.lease_released
    )
}

fn process_identity_summary(identity: &stillyard::ProcessIdentity) -> String {
    match identity {
        stillyard::ProcessIdentity::Linux {
            pid,
            start_ticks,
            pid_namespace_inode,
            ..
        } => format!("PID {pid}/start {start_ticks}/namespace {pid_namespace_inode}"),
        stillyard::ProcessIdentity::Windows {
            pid,
            creation_filetime_100ns,
            ..
        } => format!("PID {pid}/start {creation_filetime_100ns}"),
        stillyard::ProcessIdentity::Unknown {
            unknown_platform, ..
        } => format!("{unknown_platform} requester identity"),
        _ => "unknown requester identity".into(),
    }
}

#[cfg(windows)]
fn current_process_matches(identity: &stillyard::ProcessIdentity) -> bool {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};

    let stillyard::ProcessIdentity::Windows {
        pid,
        creation_filetime_100ns,
        ..
    } = identity
    else {
        return false;
    };
    if *pid != std::process::id() {
        return false;
    }
    let mut creation: FILETIME = unsafe { std::mem::zeroed() };
    let mut exit: FILETIME = unsafe { std::mem::zeroed() };
    let mut kernel: FILETIME = unsafe { std::mem::zeroed() };
    let mut user: FILETIME = unsafe { std::mem::zeroed() };
    // SAFETY: the current-process pseudohandle is valid and every output is writable.
    if unsafe {
        GetProcessTimes(
            GetCurrentProcess(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    } == 0
    {
        return false;
    }
    let observed = u64::from(creation.dwLowDateTime) | (u64::from(creation.dwHighDateTime) << 32);
    observed == *creation_filetime_100ns
}

#[cfg(not(windows))]
fn current_process_matches(_identity: &stillyard::ProcessIdentity) -> bool {
    false
}

fn connect_client(endpoint: Option<&str>, deadline: Instant) -> Result<Client, stillyard::Error> {
    let mut builder = Client::builder();
    if let Some(endpoint) = endpoint {
        builder = builder.endpoint(endpoint);
    }
    builder.connect(deadline, None)
}

fn deadline(seconds: u64) -> Instant {
    Instant::now() + Duration::from_secs(seconds)
}

fn read_machine_input<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> io::Result<T> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(stillyard::machine::MAX_FRAME_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > stillyard::machine::MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "machine input exceeds byte limit",
        ));
    }
    serde_json::from_slice(&bytes).map_err(Into::into)
}

fn print_doctor(snapshot: &stillyard::DoctorSnapshot) {
    let heading = match &snapshot.overall {
        stillyard::DoctorOverallStatus::Healthy => "healthy",
        stillyard::DoctorOverallStatus::AttentionRequired => "attention required",
        stillyard::DoctorOverallStatus::Unsafe => "unsafe",
        _ => "unknown",
    };
    println!("{heading}");
    println!(
        "daemon {} pid={} generation={} store={}",
        snapshot.daemon.version,
        snapshot.daemon.pid,
        snapshot.daemon.daemon_generation,
        snapshot.store.store_uuid,
    );
    println!(
        "config sha256={} unresolved={}",
        snapshot.daemon.config_sha256, snapshot.incidents.total_unresolved,
    );
    for check in &snapshot.checks {
        if check.status != stillyard::DoctorCheckStatus::Pass {
            println!("{:?} {}: {}", check.status, check.code, check.summary);
        }
    }
    for incident in &snapshot.incidents.incidents {
        println!(
            "incident {} {}: {}",
            incident.containment_id, incident.reason_code, incident.detail
        );
    }
    for boundary in &snapshot.boundaries {
        println!("boundary {}: {}", boundary.code, boundary.statement);
    }
}

fn logs_deadline_seconds(follow: bool, configured: Option<u64>) -> u64 {
    configured.unwrap_or(if follow { 86_400 } else { 10 })
}

fn labels_selector(
    labels: &[String],
) -> Result<stillyard::JobSelector, Box<dyn std::error::Error>> {
    if labels.is_empty() {
        return Ok(stillyard::JobSelector::All);
    }
    let labels = labels
        .iter()
        .map(|label| {
            let (key, value) = label
                .split_once('=')
                .ok_or_else(|| format!("label must be KEY=VALUE: {label:?}"))?;
            if key.is_empty() || value.is_empty() {
                return Err(format!(
                    "label must have a nonempty key and value: {label:?}"
                ));
            }
            Ok(stillyard::Label {
                key: key.to_owned(),
                value: value.to_owned(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(stillyard::JobSelector::Labels { labels })
}

fn print_job_list(page: &stillyard::JobListPage) {
    println!("STATE\tPRIORITY\tEFFECTIVE\tRANK\tRESERVATION\tJOB\tBLOCKER");
    for job in &page.jobs {
        println!(
            "{:?}\t{}\t{}\t{}\t{}\t{}\t{}",
            job.state,
            job.priority,
            job.effective_priority
                .map(|priority| priority.to_string())
                .unwrap_or_else(|| "-".into()),
            job.queue_rank
                .map(|rank| rank.to_string())
                .unwrap_or_else(|| "-".into()),
            job.reservation
                .as_ref()
                .map(|reservation| serde_json::to_string(&reservation.claims)
                    .unwrap_or_else(|_| "?".into()))
                .unwrap_or_else(|| "-".into()),
            job.job_id,
            job.blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str())
                .unwrap_or("-")
        );
    }
    if let Some(cursor) = page.next_cursor {
        println!("next_cursor={cursor}");
    }
    println!("event_cursor={}", page.event_cursor);
}

fn print_job_tree(page: &JobTreePage, ascii: bool) {
    println!("STATE\tRANK\tJOB / COMMAND\tCLAIMS\tNOTE");
    let mut ancestors = Vec::<usize>::new();
    for (index, node) in page.nodes.iter().enumerate() {
        let depth = usize::try_from(node.depth).unwrap_or(usize::MAX);
        ancestors.truncate(depth);
        let mut indent = String::new();
        for ancestor in ancestors.iter().skip(1) {
            indent.push_str(if has_later_tree_sibling(&page.nodes, *ancestor) {
                if ascii { "|  " } else { "│  " }
            } else {
                "   "
            });
        }
        if node.parent_retained == Some(false) {
            indent.push_str("?-- ");
        } else if depth > 0 {
            indent.push_str(if has_later_tree_sibling(&page.nodes, index) {
                if ascii { "|-- " } else { "├─ " }
            } else if ascii {
                "\\-- "
            } else {
                "└─ "
            });
        }
        let claims = serde_json::to_string(&node.summary.claims).unwrap_or_else(|_| "?".into());
        let mut notes = Vec::new();
        if node.parent_retained == Some(false) {
            notes.push(node.summary.parent.map_or_else(
                || "orphan: missing parent".into(),
                |parent| format!("orphan: missing parent {}", parent.job_id),
            ));
        }
        if node.context_only {
            notes.push("context".into());
        }
        if node.descendants_truncated {
            notes.push("truncated".into());
        }
        if let Some(blocker) = &node.summary.blocker {
            notes.push(blocker.code.clone());
        }
        println!(
            "{:?}\t{}\t{}{} {}\t{}\t{}",
            node.summary.state,
            node.summary
                .queue_rank
                .map(|rank| rank.to_string())
                .unwrap_or_else(|| "-".into()),
            indent,
            node.summary.job_id,
            node.summary.command_preview,
            claims,
            notes.join(", ")
        );
        if ancestors.len() == depth {
            ancestors.push(index);
        }
    }
    if page.next_root_cursor.is_some() {
        println!("next_root_cursor=available");
    }
    println!("event_cursor={}", page.event_cursor);
}

fn has_later_tree_sibling(nodes: &[stillyard::JobTreeNode], index: usize) -> bool {
    let depth = nodes[index].depth;
    for candidate in &nodes[index + 1..] {
        if candidate.depth < depth {
            return false;
        }
        if candidate.depth == depth {
            return true;
        }
    }
    false
}

fn require_current_spec_version(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    let version = value
        .as_object()
        .and_then(|object| object.get("spec_version"))
        .and_then(serde_json::Value::as_u64)
        .ok_or("submission document has no integer spec_version")?;
    if version != u64::from(stillyard::SPEC_VERSION) {
        return Err(format!(
            "unsupported spec_version {version}, expected {}",
            stillyard::SPEC_VERSION
        )
        .into());
    }
    Ok(())
}

fn read_input(path: &PathBuf) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    if path.as_os_str() == "-" {
        io::stdin().read_to_end(&mut bytes)?;
    } else {
        File::open(path)?.read_to_end(&mut bytes)?;
    }
    Ok(bytes)
}

fn print_json(value: &impl serde::Serialize) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn print_json_stderr(value: &impl serde::Serialize) -> Result<(), serde_json::Error> {
    eprintln!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn exit_for_snapshot(snapshot: &JobSnapshot) {
    let code = snapshot_exit_code(snapshot);
    if code != 0 {
        std::process::exit(code);
    }
}

fn snapshot_exit_code(snapshot: &JobSnapshot) -> i32 {
    snapshot_cli_exit(snapshot).1
}

fn snapshot_cli_exit(snapshot: &JobSnapshot) -> (ExitSource, i32) {
    let final_primary_verdict = snapshot
        .attempts
        .last()
        .and_then(|attempt| attempt.verdict)
        .is_some_and(|verdict| {
            matches!(
                verdict,
                AttemptVerdict::Succeeded | AttemptVerdict::ProcessFailed
            )
        });
    if final_primary_verdict {
        if let Some(code) = snapshot.root_exit_code {
            if !scheduler_exit_code(code) {
                return (ExitSource::Process, code);
            }
        }
    }
    (
        ExitSource::Scheduler,
        scheduler_snapshot_exit_code(snapshot),
    )
}

fn scheduler_exit_code(code: i32) -> bool {
    matches!(code, 0 | 20..=27 | 64 | 69 | 70)
}

fn scheduler_snapshot_exit_code(snapshot: &JobSnapshot) -> i32 {
    match snapshot.outcome {
        Some(JobOutcome::Succeeded) => 0,
        Some(JobOutcome::Failed) => 20,
        Some(JobOutcome::TimedOut) => 21,
        Some(JobOutcome::Canceled) => 22,
        Some(JobOutcome::Interrupted) => 23,
        Some(JobOutcome::Skipped) => 24,
        None => 25,
        Some(_) => 70,
    }
}

fn wait_report(outcome: WaitOutcome) -> WaitReport {
    let (exit_source, exit_code) = match &outcome {
        WaitOutcome::Pending { .. } => (ExitSource::Scheduler, 25),
        WaitOutcome::Final { snapshot, .. } => snapshot_cli_exit(snapshot),
        WaitOutcome::Unavailable { .. } => (ExitSource::Scheduler, 69),
        WaitOutcome::GapOrUnknown { .. } => (ExitSource::Scheduler, 70),
        _ => (ExitSource::Scheduler, 70),
    };
    WaitReport::new(outcome, exit_source, exit_code)
}

fn ensure_job_report(outcome: EnsureOutcome<EnsuredJob>) -> EnsureReport<EnsuredJob> {
    let (exit_source, exit_code) = match &outcome {
        EnsureOutcome::Accepted(_) => (ExitSource::Scheduler, 0),
        EnsureOutcome::Pending(_) => (ExitSource::Scheduler, 25),
        EnsureOutcome::Final(ensured) => ensured
            .snapshot
            .as_deref()
            .map(snapshot_cli_exit)
            .unwrap_or((ExitSource::Scheduler, 70)),
        EnsureOutcome::Rejected(_) | EnsureOutcome::Conflict { .. } => (ExitSource::Scheduler, 27),
        EnsureOutcome::Unknown => (ExitSource::Scheduler, 70),
        _ => (ExitSource::Scheduler, 70),
    };
    EnsureReport::new(outcome, exit_source, exit_code)
}

fn ensure_batch_report(outcome: EnsureOutcome<EnsuredBatch>) -> EnsureReport<EnsuredBatch> {
    let exit_code = match &outcome {
        EnsureOutcome::Accepted(_) => 0,
        EnsureOutcome::Pending(_) => 25,
        EnsureOutcome::Final(ensured) => ensured
            .snapshots
            .iter()
            .map(snapshot_exit_rank)
            .max_by_key(|candidate| candidate.0)
            .map_or(0, |candidate| candidate.1),
        EnsureOutcome::Rejected(_) | EnsureOutcome::Conflict { .. } => 27,
        EnsureOutcome::Unknown => 70,
        _ => 70,
    };
    EnsureReport::new(outcome, ExitSource::Scheduler, exit_code)
}

fn exit_for_code(code: i32) {
    if code != 0 {
        std::process::exit(code);
    }
}

fn snapshot_exit_rank(snapshot: &JobSnapshot) -> (u8, i32) {
    match snapshot.outcome {
        Some(JobOutcome::Succeeded) => (0, 0),
        Some(JobOutcome::Skipped) => (1, 24),
        Some(JobOutcome::Canceled) => (2, 22),
        Some(JobOutcome::Interrupted) => (3, 23),
        Some(JobOutcome::TimedOut) => (4, 21),
        Some(JobOutcome::Failed) => (5, snapshot_exit_code(snapshot)),
        None => (6, 25),
        Some(_) => (7, 70),
    }
}

fn exit_for_recovery(recovery: &RecoveryResult) {
    let code = match recovery {
        RecoveryResult::Received { .. }
        | RecoveryResult::Accepted(_)
        | RecoveryResult::AcceptedBatch(_) => 0,
        RecoveryResult::Rejected { .. }
        | RecoveryResult::Conflict { .. }
        | RecoveryResult::NotReceived => 27,
        RecoveryResult::Unknown => 70,
        _ => 70,
    };
    if code != 0 {
        std::process::exit(code);
    }
}

fn error_exit_code(error: &(dyn std::error::Error + 'static)) -> i32 {
    if let Some(error) = error.downcast_ref::<stillyard::Error>() {
        return match error {
            stillyard::Error::InvalidSpec(_) => 64,
            stillyard::Error::Unavailable(_)
            | stillyard::Error::ViewUnavailable { .. }
            | stillyard::Error::ViewStale { .. }
            | stillyard::Error::UnsupportedPlatform(_) => 69,
            stillyard::Error::DeadlineElapsed | stillyard::Error::Canceled => 25,
            stillyard::Error::ManagedWaitRejected { .. }
            | stillyard::Error::Rejected { .. }
            | stillyard::Error::IdempotencyConflict { .. } => 27,
            stillyard::Error::NotFound { .. } => 70,
            _ => 70,
        };
    }
    if error.downcast_ref::<serde_json::Error>().is_some() {
        return 64;
    }
    70
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_rejections_have_the_rejection_exit_code() {
        for error in [
            stillyard::Error::Rejected {
                code: "idempotency_conflict".into(),
                detail: "conflict".into(),
            },
            stillyard::Error::ManagedWaitRejected {
                code: "resource_capacity".into(),
                detail: "capacity".into(),
            },
        ] {
            assert_eq!(error_exit_code(&error), 27);
        }
        assert_eq!(
            error_exit_code(&stillyard::Error::NotFound {
                detail: "not found: missing".into(),
            }),
            70
        );
    }

    #[test]
    fn managed_execution_exit_namespace_keeps_success_scheduler_owned() {
        assert!(scheduler_exit_code(0));
        assert!(scheduler_exit_code(25));
        assert!(!scheduler_exit_code(7));
    }

    #[test]
    fn ensure_requires_an_explicit_endpoint() {
        let cli = Cli::try_parse_from([
            "stillyard",
            "ensure",
            "--spec",
            "unused.json",
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000001",
        ])
        .unwrap();
        let error = execute(cli).unwrap_err();
        assert_eq!(error_exit_code(error.as_ref()), 64);
    }

    #[test]
    fn recover_wait_and_wait_passthrough_have_agent_sized_defaults() {
        let recover = Cli::try_parse_from([
            "stillyard",
            "recover",
            "--result-file",
            "operation.json",
            "--wait",
            "--passthrough",
        ])
        .unwrap();
        assert!(matches!(
            recover.command,
            Command::Recover {
                deadline_seconds: 86_400,
                wait: true,
                passthrough: true,
                ..
            }
        ));

        let job_id = format!("{}~{}", Uuid::now_v7(), Uuid::now_v7());
        let wait = Cli::try_parse_from(["stillyard", "wait", &job_id, "--passthrough"]).unwrap();
        assert!(matches!(
            wait.command,
            Command::Wait {
                deadline_seconds: 86_400,
                passthrough: true,
                ..
            }
        ));
    }

    #[test]
    fn observation_cli_keeps_the_public_spellings() {
        let job_id = format!("{}~{}", Uuid::now_v7(), Uuid::now_v7());
        let logs = Cli::try_parse_from([
            "stillyard",
            "logs",
            &job_id,
            "--stderr",
            "--follow",
            "--since",
            "7",
        ])
        .unwrap();
        assert!(matches!(
            logs.command,
            Command::Logs {
                stderr: true,
                follow: true,
                offset: 7,
                deadline_seconds: None,
                ..
            }
        ));
        assert!(Cli::try_parse_from(["stillyard", "events"]).is_err());
        assert!(Cli::try_parse_from(["stillyard", "events", "--json"]).is_ok());
        assert_eq!(logs_deadline_seconds(true, None), 86_400);
        assert_eq!(logs_deadline_seconds(false, None), 10);
        assert_eq!(logs_deadline_seconds(true, Some(7)), 7);
    }

    #[test]
    fn context_cli_is_an_explicit_json_query() {
        assert!(Cli::try_parse_from(["stillyard", "context"]).is_err());
        let context =
            Cli::try_parse_from(["stillyard", "context", "--json", "--deadline-seconds", "7"])
                .unwrap();
        assert!(matches!(
            context.command,
            Command::Context {
                json: true,
                deadline_seconds: 7,
            }
        ));
    }

    #[test]
    fn doctor_cli_exposes_json_paging_and_explicit_force() {
        let store_uuid = Uuid::now_v7();
        let containment = format!("{}~{}", store_uuid, Uuid::now_v7());
        let cursor = format!(
            "v2:{}:{}:{}:{}:7",
            store_uuid,
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7()
        );
        let doctor = Cli::try_parse_from([
            "stillyard",
            "doctor",
            "--json",
            "--incident-limit",
            "32",
            "--incident-cursor",
            &cursor,
        ])
        .unwrap();
        assert!(matches!(
            doctor.command,
            Command::Doctor {
                command: None,
                incident_limit: Some(32),
                json: true,
                ..
            }
        ));
        let clear = Cli::try_parse_from([
            "stillyard",
            "doctor",
            "clear-containment",
            &containment,
            "--force",
            "--json",
        ])
        .unwrap();
        assert!(matches!(
            clear.command,
            Command::Doctor {
                command: Some(DoctorCommand::ClearContainment {
                    force: true,
                    json: true,
                    ..
                }),
                ..
            }
        ));
        let missing_force =
            Cli::try_parse_from(["stillyard", "doctor", "clear-containment", &containment])
                .unwrap();
        let error = execute(missing_force).unwrap_err();
        assert_eq!(error_exit_code(error.as_ref()), 64);
    }

    #[test]
    fn isolated_instance_coordinates_are_global_and_daemon_store_is_explicit() {
        let endpoint = r"\\.\pipe\moot-test-123";
        let before =
            Cli::try_parse_from(["stillyard", "--endpoint", endpoint, "daemon-status"]).unwrap();
        assert_eq!(before.endpoint.as_deref(), Some(endpoint));
        assert!(matches!(before.command, Command::DaemonStatus { .. }));

        let after = Cli::try_parse_from([
            "stillyard",
            "daemon",
            "--store",
            r"C:\temp\moot-store",
            "--endpoint",
            endpoint,
        ])
        .unwrap();
        assert_eq!(after.endpoint.as_deref(), Some(endpoint));
        assert!(matches!(
            after.command,
            Command::Daemon { store: Some(_), .. }
        ));
    }

    #[test]
    fn cli_and_tui_have_no_private_store_read_path() {
        let sources = [include_str!("main.rs"), include_str!("tui.rs")]
            .join("\n")
            .to_ascii_lowercase();
        for forbidden in [
            concat!("rus", "qlite"),
            concat!("store", "paths"),
            concat!("stillyard::", "store"),
            concat!("sqlite", "3"),
            concat!("prag", "ma"),
        ] {
            assert!(
                !sources.contains(forbidden),
                "private-read mutant survived source audit: {forbidden}"
            );
        }
    }
}
