use std::fs::File;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum, error::ErrorKind};
use stillyard::{
    BatchSpec, Client, JobId, JobOutcome, JobSnapshot, JobSpec, LogStream, RecoveryResult,
    SubmitOptions,
};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "stillyard", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the per-user scheduler daemon in the foreground.
    Daemon {
        /// Internal marker used by client auto-start.
        #[arg(long, hide = true)]
        background_child: bool,
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
        #[arg(long, value_enum, default_value_t = StreamArg::Stdout)]
        stream: StreamArg,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 1_048_576)]
        limit: u32,
        /// Emit the complete LogChunk JSON instead of its byte payload.
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 10)]
        deadline_seconds: u64,
    },
    /// Print daemon identity and queue counts.
    DaemonStatus {
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
    match cli.command {
        Command::Daemon { background_child } => {
            let _ = background_child;
            stillyard::run_daemon()?;
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
            let client = Client::connect(deadline, None)?;
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
                let spec: BatchSpec = serde_json::from_slice(&read_input(&path)?)?;
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
                let spec: JobSpec = serde_json::from_slice(&read_input(&path)?)?;
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
            let client = Client::connect(deadline, None)?;
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
            let client = Client::connect(deadline, None)?;
            print_json(&client.status(job_id, deadline, None)?)?;
        }
        Command::Wait {
            job_id,
            passthrough,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = Client::connect(deadline, None)?;
            let snapshot = if passthrough {
                client.wait_with_passthrough(
                    job_id,
                    &mut 0,
                    &mut 0,
                    &mut io::stdout(),
                    &mut io::stderr(),
                    deadline,
                    None,
                )?
            } else {
                client.wait(job_id, deadline, None)?
            };
            if passthrough {
                print_json_stderr(&snapshot)?;
            } else {
                print_json(&snapshot)?;
            }
            exit_for_snapshot(&snapshot);
        }
        Command::Logs {
            job_id,
            stream,
            offset,
            limit,
            json,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = Client::connect(deadline, None)?;
            let chunk = client.logs(job_id, stream.into(), offset, limit, deadline, None)?;
            if json {
                print_json(&chunk)?;
            } else {
                io::stdout().write_all(&chunk.bytes)?;
            }
        }
        Command::DaemonStatus { deadline_seconds } => {
            let deadline = deadline(deadline_seconds);
            let client = Client::connect(deadline, None)?;
            print_json(&client.daemon_status(deadline, None)?)?;
        }
        Command::Cancel {
            jobs,
            deadline_seconds,
        } => {
            let deadline = deadline(deadline_seconds);
            let client = Client::connect(deadline, None)?;
            print_json(&client.cancel(&jobs, deadline, None)?)?;
        }
        Command::Schema { command } => match command {
            SchemaCommand::Spec => print!("{}", stillyard::schema_json()?),
            SchemaCommand::Config => print!("{}", stillyard::config_schema_json()?),
        },
    }
    Ok(())
}

fn deadline(seconds: u64) -> Instant {
    Instant::now() + Duration::from_secs(seconds)
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
    match snapshot.outcome {
        Some(JobOutcome::Succeeded) => 0,
        Some(JobOutcome::Failed) => match snapshot.root_exit_code {
            Some(code) if code != 0 => code,
            Some(_) | None => 20,
        },
        Some(JobOutcome::TimedOut) => 21,
        Some(JobOutcome::Canceled) => 22,
        Some(JobOutcome::Interrupted) => 23,
        Some(JobOutcome::Skipped) => 24,
        None => 25,
        Some(_) => 70,
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
        | RecoveryResult::Conflict
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
            stillyard::Error::Unavailable(_) | stillyard::Error::UnsupportedPlatform(_) => 69,
            stillyard::Error::DeadlineElapsed | stillyard::Error::Canceled => 25,
            stillyard::Error::ManagedWaitRejected { .. } => 27,
            stillyard::Error::Protocol(message)
                if message.starts_with("idempotency_conflict:")
                    || message.starts_with("rejected:") =>
            {
                27
            }
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
}
