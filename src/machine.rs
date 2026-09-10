//! Versioned machine-coordination messages. Transport identity and durable
//! protocol transitions are separate from this bounded wire representation.

pub mod clearance;
pub use clearance::*;
pub mod bridge;
pub mod manager;

use std::collections::BTreeMap;
use std::io::{Read, Write};

use hmac::{Hmac, Mac};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{AttemptId, ContainmentId, ExecutionDomainId, InvocationId, InvocationRole, JobId};

pub const WIRE_VERSION: u32 = 1;
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_QUEUED_BYTES: usize = 4 * MAX_FRAME_BYTES;
pub const MAX_IN_FLIGHT: usize = 64;
pub const MAX_DOMAINS: usize = 256;
pub const MAX_DOMAIN_CANDIDATES: usize = 4096;
pub const MAX_MACHINE_CANDIDATES: usize = 65536;
pub const MAX_RECONCILE_PAGE: usize = 256;
pub const MAX_RECONCILE_BYTES: usize = 16 * 1024 * 1024;
pub const OFFER_MILLIS: i64 = 5000;
pub const CANDIDATE_MILLIS: i64 = 30000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResetGate {
    pub reset_id: Uuid,
    pub displaced_store_uuid: Uuid,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorHistory {
    pub store_uuid: Uuid,
    pub daemon_generation: Uuid,
    pub process_identity: crate::ProcessIdentity,
    pub pending_reset: Option<ResetGate>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AllocationKey {
    pub machine_id: Uuid,
    pub authority_epoch: Uuid,
    pub domain_id: ExecutionDomainId,
    pub manager_store_uuid: Uuid,
    pub lease_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionIdentity {
    pub machine_id: Uuid,
    pub authority_epoch: Uuid,
    pub domain_id: ExecutionDomainId,
    pub manager_store_uuid: Uuid,
    pub executor_incarnation: Uuid,
    pub connection_epoch: u64,
}

impl SessionIdentity {
    pub fn owns(&self, key: &AllocationKey) -> bool {
        self.machine_id == key.machine_id
            && self.authority_epoch == key.authority_epoch
            && self.domain_id == key.domain_id
            && self.manager_store_uuid == key.manager_store_uuid
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantRole {
    Executor,
    RuntimeAdapter,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentity {
    pub installation_nonce: Uuid,
    pub domain_id: ExecutionDomainId,
    pub owner_uid: u32,
    pub runtime_registration: String,
    pub role: ParticipantRole,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PairingRegistration {
    pub installation: InstallationIdentity,
    pub manager_store_uuid: Uuid,
    pub parent_domain: ExecutionDomainId,
    pub budgets: BTreeMap<String, u64>,
    pub aliases: BTreeMap<String, String>,
    pub secret: [u8; 32],
}

impl std::fmt::Debug for PairingRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairingRegistration")
            .field("installation", &self.installation)
            .field("manager_store_uuid", &self.manager_store_uuid)
            .field("secret", &"[redacted]")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectHello {
    pub installation_nonce: Uuid,
    pub manager_store_uuid: Uuid,
    pub executor_incarnation: Uuid,
    pub executor_protocol: u32,
    pub executor_nonce: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParticipantSnapshot {
    pub installation: InstallationIdentity,
    pub manager_store_uuid: Uuid,
    pub connection_epoch: u64,
    pub executor_incarnation: Option<Uuid>,
    pub reconciliation_required: bool,
    pub retired_sequence_floor: u64,
    pub accepted_sequence: u64,
}

/// Every identity, nonce and version participates in the authentication tag.
/// The receiving coordinator must also match its pending, single-use challenge.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectChallenge {
    pub wire_version: u32,
    pub coordinator_protocol: u32,
    pub executor_protocol: u32,
    pub coordinator_installation: Uuid,
    pub installation: InstallationIdentity,
    pub session: SessionIdentity,
    pub coordinator_nonce: [u8; 32],
    pub executor_nonce: [u8; 32],
}

/// Pairing material is deliberately not Debug or Serialize. Installation code
/// writes it only to the owner-controlled pairing anchor, never to a snapshot.
pub struct PairingSecret([u8; 32]);

/// Schema root for the versioned participant protocol and allocation views.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ProtocolRecord {
    BridgeRequest(bridge::BridgeRequest),
    BridgeReply(bridge::BridgeReply),
    ClearancePreview(Box<DomainClearancePreview>),
    RetirementRequest(DomainRetirementRequest),
    RetirementReceipt(Box<DomainRetirementReceipt>),
    RetirementAudit(Box<DomainRetirementAudit>),
    Request(Box<Request>),
    Reply(Box<Reply>),
    Hello(ConnectHello),
    Challenge(ConnectChallenge),
    Participant(ParticipantSnapshot),
    NativeAllocation(NativeAllocationSnapshot),
    Events(EventPage),
    Authority(Box<crate::AuthoritySnapshot>),
}

impl PairingSecret {
    pub fn sign_request(&self, request: &mut Request) -> std::io::Result<()> {
        request.validate()?;
        request.authentication = self.request_mac(request)?.finalize().into_bytes().into();
        Ok(())
    }

    pub fn verify_request(&self, request: &Request) -> std::io::Result<()> {
        request.validate()?;
        self.request_mac(request)?
            .verify_slice(&request.authentication)
            .map_err(|_| invalid("machine operation authentication failed"))
    }

    fn request_mac(&self, request: &Request) -> std::io::Result<Hmac<Sha256>> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0)
            .map_err(|_| invalid("invalid pairing secret"))?;
        mac.update(b"stillyard-machine-operation-v1\0");
        mac.update(&serde_json::to_vec(&(
            request.version,
            &request.session,
            request.request_sequence,
            request.operation_id,
            &request.payload_sha256,
        ))?);
        Ok(mac)
    }

    pub fn generate() -> std::io::Result<Self> {
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes).map_err(std::io::Error::other)?;
        Ok(Self(bytes))
    }

    pub fn from_anchor(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn anchor_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn sign_challenge(&self, challenge: &ConnectChallenge) -> std::io::Result<[u8; 32]> {
        let mut mac = self.challenge_mac(challenge)?;
        // Explicit direction binding prevents reflecting a response as a request.
        mac.update(b"executor-response");
        Ok(mac.finalize().into_bytes().into())
    }

    pub fn verify_challenge(
        &self,
        challenge: &ConnectChallenge,
        tag: &[u8; 32],
    ) -> std::io::Result<()> {
        let mut mac = self.challenge_mac(challenge)?;
        mac.update(b"executor-response");
        mac.verify_slice(tag)
            .map_err(|_| invalid("pairing authentication failed"))
    }

    fn challenge_mac(&self, challenge: &ConnectChallenge) -> std::io::Result<Hmac<Sha256>> {
        if challenge.wire_version != WIRE_VERSION {
            return Err(invalid("unsupported machine wire version"));
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0)
            .map_err(|_| invalid("invalid pairing secret"))?;
        mac.update(b"stillyard-machine-connect-v1\0");
        mac.update(&serde_json::to_vec(challenge)?);
        Ok(mac)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Claims {
    pub scalars: BTreeMap<String, u64>,
    pub shared_fences: Vec<String>,
    pub exclusive_fences: Vec<String>,
    pub impacts: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AllocationOwner {
    Work {
        job_id: JobId,
        attempt_id: AttemptId,
    },
    Probe {
        job_id: JobId,
        invocation_id: InvocationId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Candidate {
    pub key: AllocationKey,
    pub owner: AllocationOwner,
    pub revision: u64,
    pub priority: i8,
    pub claims: Claims,
    pub configuration_sha256: String,
    pub observed: Option<crate::ObservedResourcePolicy>,
    pub quiet: Option<crate::QuietPolicy>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InvocationIntent {
    pub invocation_id: InvocationId,
    pub containment_id: ContainmentId,
    pub role: InvocationRole,
    pub role_index: u32,
    pub release_sequence: u64,
    pub executable_sha256: String,
    /// Digest of the manager's durable platform boundary/root identity record.
    pub boundary_sha256: String,
    pub readiness_challenge: Uuid,
    pub previous_cleanup: Option<TicketCleanup>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TicketCleanup {
    pub invocation_id: InvocationId,
    pub release_sequence: u64,
    pub boundary_sha256: String,
    pub proof_sha256: String,
    pub user_code_released: bool,
}

/// This is a domain-scoped manager attestation, accepted only from its paired
/// executor. The platform manager must seal all start rights before sending it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SealedRelease {
    pub key: AllocationKey,
    pub offer_nonce: Uuid,
    pub sealed_sequence: u64,
    pub tickets: Vec<TicketCleanup>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GrantState {
    Offered,
    Expired,
    Armed,
    Uncertain,
    Released,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GrantSnapshot {
    /// Original coordinator queue identity/time, retained independently of SQL.
    pub queue_accepted_unix_millis: i64,
    pub queue_sequence: u64,
    pub uncertainty_reason: Option<String>,
    pub risk_clearance: Option<Uuid>,
    pub grant_id: crate::GrantId,
    pub candidate: Candidate,
    pub offer_nonce: Uuid,
    pub state: GrantState,
    pub offered_unix_millis: i64,
    pub offer_deadline_unix_millis: i64,
    pub armed_unix_millis: Option<i64>,
    pub released_unix_millis: Option<i64>,
    pub tickets: Vec<InvocationIntent>,
    pub sealed_release: Option<SealedRelease>,
}

/// Allocation/Lease association. The historical Rust type name is retained.
/// Native admission shares the Grant and Lease UUID; an attached allocation
/// reports its actual coordinator Grant ID and separately bound local Lease ID.
/// This observation introduces no second resource counter.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeAllocationSnapshot {
    pub grant_id: crate::GrantId,
    pub lease_id: Uuid,
    /// Absent when authority identity/history cannot be established.
    pub key: Option<AllocationKey>,
    pub owner: AllocationOwner,
    pub state: GrantState,
    pub claims: Claims,
}

/// Reset-independent coverage of a born-contained native root before it can
/// execute user code. Native SQL and this record describe the same allocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NativeStartPermission {
    pub allocation: NativeAllocationSnapshot,
    pub invocation_id: InvocationId,
    pub containment_id: ContainmentId,
    pub root_identity: crate::ProcessIdentity,
    pub creator_identity: crate::ProcessIdentity,
    pub daemon_generation: Uuid,
    pub executable_sha256: String,
    pub boundary_kind: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventCursor {
    pub coordinator_store_uuid: Uuid,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AllocationEvent {
    pub cursor: EventCursor,
    pub native: bool,
    pub grant_id: crate::GrantId,
    pub key: Option<AllocationKey>,
    pub owner: AllocationOwner,
    pub state: GrantState,
    pub tickets_issued: u32,
    pub risk_clearance: Option<Uuid>,
    pub claims: Claims,
    pub committed_unix_millis: i64,
}

/// Events are bounded observations, never cleanup or authority proofs. A gap
/// requires refreshing allocation/accounting snapshots before applying deltas.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventPage {
    pub events: Vec<AllocationEvent>,
    pub cursor: EventCursor,
    pub oldest_available: EventCursor,
    pub gap: bool,
    pub more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InvocationTicket {
    pub grant_id: crate::GrantId,
    pub key: AllocationKey,
    pub offer_nonce: Uuid,
    pub intent: InvocationIntent,
    pub configuration_sha256: String,
    pub issued_unix_millis: i64,
    pub host_observation_generation: Uuid,
    pub host_sample_unix_millis: i64,
    pub session: SessionIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    InventoryPage {
        grants: Vec<GrantSnapshot>,
        next: Option<AllocationKey>,
        configuration_sha256: String,
    },
    Acknowledged {
        through_sequence: u64,
    },
    Accepted {
        revision: u64,
    },
    Grant {
        grant: Box<GrantSnapshot>,
    },
    Ticket {
        ticket: Box<InvocationTicket>,
    },
    Released {
        grant_id: crate::GrantId,
        sealed_sequence: u64,
    },
    Inspection {
        grants: Vec<GrantSnapshot>,
        truncated: bool,
    },
    Reconciled {
        end_sequence: u64,
        released: Vec<AllocationKey>,
    },
    Rejected {
        code: String,
        detail: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub session: SessionIdentity,
    pub request_sequence: u64,
    pub operation_id: Uuid,
    pub coordinator_revision: u64,
    pub outcome: Outcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    /// Retain the full debit while local containment/lifetime proof is unknown.
    ReportUncertain {
        key: AllocationKey,
        offer_nonce: Uuid,
        reason: String,
    },
    /// Enumerate every outstanding allocation in bounded pages.
    InspectPage {
        after: Option<AllocationKey>,
        limit: u32,
    },
    /// The manager has durably applied every reply through this sequence.
    Acknowledge {
        through_sequence: u64,
    },
    CandidateUpsert {
        candidate: Candidate,
    },
    Withdraw {
        key: AllocationKey,
        revision: u64,
    },
    Arm {
        key: AllocationKey,
        offer_nonce: Uuid,
    },
    AuthorizeInvocation {
        key: AllocationKey,
        offer_nonce: Uuid,
        intent: InvocationIntent,
    },
    CancelCandidate {
        key: AllocationKey,
        revision: u64,
    },
    Release {
        release: SealedRelease,
    },
    Inspect {
        key: Option<AllocationKey>,
    },
    ReconcileBegin {
        snapshot_id: Uuid,
        begin_sequence: u64,
        end_sequence: u64,
        page_count: u32,
        digest: String,
        configuration_sha256: String,
    },
    ReconcilePage {
        snapshot_id: Uuid,
        index: u32,
        allocations: Vec<ReconcileAllocation>,
    },
    ReconcileCommit {
        snapshot_id: Uuid,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReconcileAllocation {
    pub key: AllocationKey,
    pub offer_nonce: Uuid,
    pub tickets: Vec<InvocationIntent>,
    pub sealed_release: Option<SealedRelease>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub session: SessionIdentity,
    pub request_sequence: u64,
    pub operation_id: Uuid,
    pub payload_sha256: String,
    pub command: Command,
    pub authentication: [u8; 32],
}

impl Request {
    pub fn new(
        session: SessionIdentity,
        sequence: u64,
        operation_id: Uuid,
        command: Command,
    ) -> std::io::Result<Self> {
        Ok(Self {
            version: WIRE_VERSION,
            session,
            request_sequence: sequence,
            operation_id,
            payload_sha256: payload_hash(&command)?,
            command,
            authentication: [0; 32],
        })
    }

    pub fn validate(&self) -> std::io::Result<()> {
        if self.version != WIRE_VERSION {
            return Err(invalid("unsupported machine wire version"));
        }
        if self.request_sequence == 0
            || self.operation_id.is_nil()
            || self.session.connection_epoch == 0
            || self.session.machine_id.is_nil()
            || self.session.authority_epoch.is_nil()
            || self.session.domain_id.0.is_nil()
            || self.session.manager_store_uuid.is_nil()
            || self.session.executor_incarnation.is_nil()
            || self.payload_sha256 != payload_hash(&self.command)?
        {
            return Err(invalid("invalid machine request identity or payload hash"));
        }
        if let Command::ReconcilePage { allocations, .. } = &self.command {
            if allocations.len() > MAX_RECONCILE_PAGE {
                return Err(invalid("reconciliation page exceeds record limit"));
            }
        }
        Ok(())
    }
}

pub fn payload_hash(value: &impl Serialize) -> std::io::Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

/// Check the length before allocating or reading an attacker-selected body.
pub fn read_frame<T: DeserializeOwned>(reader: &mut impl Read) -> std::io::Result<T> {
    let mut prefix = [0; 4];
    reader.read_exact(&mut prefix)?;
    let length = u32::from_le_bytes(prefix) as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err(invalid("machine frame exceeds length bounds"));
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    // serde_json also rejects invalid UTF-8 and trailing content.
    serde_json::from_slice(&body).map_err(Into::into)
}

pub fn write_frame<T: Serialize>(writer: &mut impl Write, value: &T) -> std::io::Result<()> {
    // Bound serialization as well as decoding; a huge local object must not
    // create an unbounded temporary allocation before rejection.
    struct Bounded(Vec<u8>);
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_FRAME_BYTES.saturating_sub(self.0.len()) {
                return Err(invalid("machine frame exceeds length bounds"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut encoded = Bounded(Vec::new());
    serde_json::to_writer(&mut encoded, value)?;
    writer.write_all(&(encoded.0.len() as u32).to_le_bytes())?;
    writer.write_all(&encoded.0)?;
    writer.flush()
}

fn invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionIdentity {
        SessionIdentity {
            machine_id: Uuid::now_v7(),
            authority_epoch: Uuid::now_v7(),
            domain_id: ExecutionDomainId(Uuid::now_v7()),
            manager_store_uuid: Uuid::now_v7(),
            executor_incarnation: Uuid::now_v7(),
            connection_epoch: 1,
        }
    }

    #[test]
    fn framing_rejects_oversize_before_reading_body_and_strictly_decodes() {
        let mut oversized = std::io::Cursor::new(((MAX_FRAME_BYTES + 1) as u32).to_le_bytes());
        assert_eq!(
            read_frame::<Request>(&mut oversized).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(oversized.position(), 4);
        let request =
            Request::new(session(), 1, Uuid::now_v7(), Command::Inspect { key: None }).unwrap();
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &request).unwrap();
        assert_eq!(
            read_frame::<Request>(&mut encoded.as_slice()).unwrap(),
            request
        );
        let mut unknown = serde_json::to_value(&request).unwrap();
        unknown["ignored_authority"] = true.into();
        encoded.clear();
        write_frame(&mut encoded, &unknown).unwrap();
        assert!(read_frame::<Request>(&mut encoded.as_slice()).is_err());
        let mut output = Vec::new();
        assert!(write_frame(&mut output, &"x".repeat(MAX_FRAME_BYTES)).is_err());
        assert!(output.is_empty());
        assert!(read_frame::<Request>(&mut [2, 0, 0, 0, 0xff, 0xff].as_slice()).is_err());
    }

    #[test]
    fn pairing_tag_binds_both_nonces_session_role_owner_and_versions() {
        let key = PairingSecret::generate().unwrap();
        let identity = session();
        let challenge = ConnectChallenge {
            wire_version: WIRE_VERSION,
            coordinator_protocol: crate::protocol::PROTOCOL_VERSION,
            executor_protocol: crate::protocol::PROTOCOL_VERSION,
            coordinator_installation: Uuid::now_v7(),
            installation: InstallationIdentity {
                installation_nonce: Uuid::now_v7(),
                domain_id: identity.domain_id,
                owner_uid: 1000,
                runtime_registration: "fixture".into(),
                role: ParticipantRole::Executor,
            },
            session: identity,
            coordinator_nonce: [1; 32],
            executor_nonce: [2; 32],
        };
        let tag = key.sign_challenge(&challenge).unwrap();
        key.verify_challenge(&challenge, &tag).unwrap();
        let mutations: Vec<fn(&mut ConnectChallenge)> = vec![
            |c| c.coordinator_nonce[0] ^= 1,
            |c| c.executor_nonce[0] ^= 1,
            |c| c.session.connection_epoch += 1,
            |c| c.session.manager_store_uuid = Uuid::now_v7(),
            |c| c.installation.owner_uid += 1,
            |c| c.installation.role = ParticipantRole::RuntimeAdapter,
            |c| c.coordinator_protocol += 1,
            |c| c.executor_protocol += 1,
            |c| c.coordinator_installation = Uuid::now_v7(),
        ];
        for mutate in mutations {
            let mut changed = challenge.clone();
            mutate(&mut changed);
            assert!(key.verify_challenge(&changed, &tag).is_err());
        }
        assert!(
            PairingSecret::generate()
                .unwrap()
                .verify_challenge(&challenge, &tag)
                .is_err()
        );
    }

    #[test]
    fn operation_authentication_binds_session_sequence_identity_and_payload() {
        let secret = PairingSecret::generate().unwrap();
        let mut request =
            Request::new(session(), 1, Uuid::now_v7(), Command::Inspect { key: None }).unwrap();
        assert!(secret.verify_request(&request).is_err());
        secret.sign_request(&mut request).unwrap();
        secret.verify_request(&request).unwrap();
        let mutations: Vec<fn(&mut Request)> = vec![
            |r| r.session.connection_epoch += 1,
            |r| r.session.executor_incarnation = Uuid::now_v7(),
            |r| r.request_sequence += 1,
            |r| r.operation_id = Uuid::now_v7(),
            |r| {
                r.command = Command::ReconcileCommit {
                    snapshot_id: Uuid::now_v7(),
                };
                r.payload_sha256 = payload_hash(&r.command).unwrap();
            },
            |r| r.authentication[0] ^= 1,
        ];
        for mutate in mutations {
            let mut changed = request.clone();
            mutate(&mut changed);
            changed.validate().unwrap();
            assert!(secret.verify_request(&changed).is_err());
        }
        assert!(
            PairingSecret::generate()
                .unwrap()
                .verify_request(&request)
                .is_err()
        );
    }

    #[test]
    fn request_rejects_version_hash_sequence_and_unknown_identity() {
        let request =
            Request::new(session(), 1, Uuid::now_v7(), Command::Inspect { key: None }).unwrap();
        request.validate().unwrap();
        let mut changed = request.clone();
        changed.payload_sha256.push('0');
        assert!(changed.validate().is_err());
        changed = request.clone();
        changed.version += 1;
        assert!(changed.validate().is_err());
        changed = request.clone();
        changed.request_sequence = 0;
        assert!(changed.validate().is_err());
        changed = request;
        changed.session.executor_incarnation = Uuid::nil();
        assert!(changed.validate().is_err());
    }
}
