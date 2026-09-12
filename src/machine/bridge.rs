//! Persistent, bounded stdin/stdout bridge. Its lifetime never releases a Grant.
use super::*;

#[cfg(target_os = "linux")]
#[allow(dead_code)] // Connected by the attached-manager installation/runtime slice.
pub(crate) mod linux;

pub const BRIDGE_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BridgeRequest {
    pub version: u32,
    pub protocol_version: u32,
    pub request_id: Uuid,
    pub deadline_millis: u32,
    pub command: BridgeCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BridgeCommand {
    ConnectBegin {
        hello: ConnectHello,
    },
    ConnectFinish {
        challenge: Box<ConnectChallenge>,
        tag: [u8; 32],
    },
    Exchange {
        request: Box<Request>,
    },
    Participant {
        domain: ExecutionDomainId,
    },
    AuthorityStatus,
    SchedulingStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BridgeReply {
    pub version: u32,
    pub protocol_version: u32,
    pub request_id: Uuid,
    pub outcome: BridgeOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BridgeOutcome {
    Challenge {
        challenge: Box<ConnectChallenge>,
    },
    Participant {
        participant: ParticipantSnapshot,
    },
    Exchange {
        reply: Box<Reply>,
    },
    Authority {
        authority: Box<crate::AuthoritySnapshot>,
    },
    Scheduling {
        snapshot: Option<crate::MachineSchedulingSnapshot>,
    },
    Error {
        code: String,
        detail: String,
    },
}

/// One in-flight frame, no unbounded read-ahead or worker queue. EOF stops only
/// the transport. A partial/malformed/oversized frame terminates the bridge.
pub fn serve(
    reader: &mut impl Read,
    writer: &mut impl Write,
    mut exchange: impl FnMut(BridgeCommand, std::time::Instant) -> crate::Result<BridgeOutcome>,
) -> std::io::Result<()> {
    loop {
        let mut first = [0_u8; 1];
        let count = loop {
            match reader.read(&mut first) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                result => break result?,
            }
        };
        if count == 0 {
            return Ok(());
        }
        let mut input = first.as_slice().chain(&mut *reader);
        let request: BridgeRequest = read_frame(&mut input)?;
        if request.version != BRIDGE_VERSION
            || request.protocol_version != crate::protocol::PROTOCOL_VERSION
            || request.request_id.is_nil()
            || !(1..=30_000).contains(&request.deadline_millis)
        {
            return Err(invalid("unsupported or invalid bridge frame"));
        }
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(u64::from(request.deadline_millis));
        let outcome = match exchange(request.command, deadline) {
            Ok(outcome) => outcome,
            Err(error) => BridgeOutcome::Error {
                code: match &error {
                    crate::Error::Rejected { code, .. } => code.clone(),
                    crate::Error::DeadlineElapsed => "deadline_elapsed".into(),
                    _ => "bridge_unavailable".into(),
                },
                detail: error.to_string().chars().take(4096).collect(),
            },
        };
        write_frame(
            writer,
            &BridgeReply {
                version: BRIDGE_VERSION,
                protocol_version: crate::protocol::PROTOCOL_VERSION,
                request_id: request.request_id,
                outcome,
            },
        )?;
    }
}

/// Called by the installed native CLI with a connect-only coordinator Client.
#[cfg(windows)]
pub fn run(client: &crate::Client) -> crate::Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    serve(
        &mut stdin.lock(),
        &mut stdout.lock(),
        |command, deadline| {
            Ok(match command {
                BridgeCommand::ConnectBegin { hello } => BridgeOutcome::Challenge {
                    challenge: Box::new(client.machine_connect_begin(hello, deadline)?),
                },
                BridgeCommand::ConnectFinish { challenge, tag } => BridgeOutcome::Participant {
                    participant: client.machine_connect_finish(*challenge, tag, deadline)?,
                },
                BridgeCommand::Exchange { request } => BridgeOutcome::Exchange {
                    reply: Box::new(client.machine_exchange(*request, deadline)?),
                },
                BridgeCommand::Participant { domain } => BridgeOutcome::Participant {
                    participant: client.machine_participant(domain, deadline)?,
                },
                BridgeCommand::AuthorityStatus => BridgeOutcome::Authority {
                    authority: Box::new(client.authority_status(deadline, None)?),
                },
                BridgeCommand::SchedulingStatus => BridgeOutcome::Scheduling {
                    snapshot: client.daemon_status(deadline, None)?.machine_scheduling,
                },
            })
        },
    )
    .map_err(|error| crate::Error::Unavailable(format!("machine bridge transport: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> BridgeRequest {
        BridgeRequest {
            version: BRIDGE_VERSION,
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            request_id: Uuid::now_v7(),
            deadline_millis: 5000,
            command: BridgeCommand::AuthorityStatus,
        }
    }

    #[test]
    fn bridge_preserves_multiple_correlations_and_eof_never_calls_cleanup() {
        let a = request();
        let b = request();
        let mut input = Vec::new();
        write_frame(&mut input, &a).unwrap();
        write_frame(&mut input, &b).unwrap();
        let mut output = Vec::new();
        let mut calls = 0;
        serve(&mut input.as_slice(), &mut output, |_, deadline| {
            calls += 1;
            assert!(deadline > std::time::Instant::now());
            Err(crate::Error::DeadlineElapsed)
        })
        .unwrap();
        assert_eq!(calls, 2);
        let mut output = output.as_slice();
        for expected in [a.request_id, b.request_id] {
            let reply: BridgeReply = read_frame(&mut output).unwrap();
            assert_eq!(reply.request_id, expected);
            assert!(matches!(reply.outcome, BridgeOutcome::Error { .. }));
        }
        assert!(output.is_empty());
    }

    #[test]
    fn bridge_rejects_versions_truncation_and_oversize_before_dispatch() {
        let mut bad = request();
        bad.protocol_version += 1;
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &bad).unwrap();
        for input in [
            encoded,
            vec![1],
            ((MAX_FRAME_BYTES + 1) as u32).to_le_bytes().to_vec(),
        ] {
            let mut output = Vec::new();
            assert!(
                serve(&mut input.as_slice(), &mut output, |_, _| panic!(
                    "invalid bridge frame dispatched"
                ))
                .is_err()
            );
            assert!(output.is_empty());
        }
    }
}
