//! Namespace-hidden servers use a launch-pinned public key, never a shared
//! client signing secret. Managed parentage is still checked by the server OS.
use crate::protocol::{Request, Response};
use crate::{ManagedParent, ProcessIdentity};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io;
use uuid::Uuid;

pub(crate) const ENVIRONMENT: &str = "STILLYARD_SERVER_ATTESTATION";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServerIdentity {
    pub(crate) version: u32,
    pub(crate) store: Uuid,
    pub(crate) generation: Uuid,
    pub(crate) endpoint: String,
    pub(crate) process: ProcessIdentity,
    pub(crate) image_device: u64,
    pub(crate) image_inode: u64,
    pub(crate) image_sha256: String,
    pub(crate) verifying_key: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedServer {
    pub(crate) server: ServerIdentity,
    pub(crate) parent: ManagedParent,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Proof {
    pub(crate) server: ServerIdentity,
    pub(crate) parent: ManagedParent,
    pub(crate) nonce: [u8; 32],
    pub(crate) request_sha256: String,
    pub(crate) response: Box<Response>,
    pub(crate) signature: Vec<u8>,
}

pub(crate) fn request_hash(request: &Request) -> io::Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(request)?)
    ))
}
fn transcript(proof: &Proof) -> io::Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(b"stillyard-linux-managed-server-response-v1\0");
    hash.update(serde_json::to_vec(&(
        &proof.server,
        proof.parent,
        proof.nonce,
        &proof.request_sha256,
        &proof.response,
    ))?);
    Ok(hash.finalize().into())
}

#[cfg(target_os = "linux")]
fn image_identity(path: &std::path::Path) -> io::Result<(u64, u64, String)> {
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    let mut file = std::fs::File::open(path)?;
    let meta = file.metadata()?;
    let mut hash = Sha256::new();
    let mut bytes = [0_u8; 65536];
    let mut total = 0;
    loop {
        let n = file.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        total += n;
        if total > 1024 * 1024 * 1024 {
            return Err(io::Error::other("daemon image exceeds inspection bound"));
        }
        hash.update(&bytes[..n]);
    }
    Ok((meta.dev(), meta.ino(), format!("{:x}", hash.finalize())))
}

#[cfg(target_os = "linux")]
pub(crate) struct Signer {
    identity: ServerIdentity,
    key: ed25519_dalek::SigningKey,
}

#[cfg(target_os = "linux")]
impl Signer {
    #[allow(dead_code)] // Installed by the attached Linux runtime after its gates.
    pub(crate) fn new(store: Uuid, generation: Uuid, endpoint: String) -> io::Result<Self> {
        if store.is_nil() || generation.is_nil() {
            return Err(io::Error::other("nil daemon attestation identity"));
        }
        let (image_device, image_inode, image_sha256) =
            image_identity(std::path::Path::new("/proc/self/exe"))?;
        // SAFETY: geteuid has no preconditions.
        let process = super::linux::Process::open(std::process::id(), unsafe { libc::geteuid() })?;
        let mut secret = [0_u8; 32];
        getrandom::fill(&mut secret).map_err(io::Error::other)?;
        let key = ed25519_dalek::SigningKey::from_bytes(&secret);
        secret.fill(0);
        let identity = ServerIdentity {
            version: 1,
            store,
            generation,
            endpoint,
            process: process.identity,
            image_device,
            image_inode,
            image_sha256,
            verifying_key: key.verifying_key().to_bytes(),
        };
        Ok(Self { identity, key })
    }
    pub(crate) fn context(&self, parent: ManagedParent) -> io::Result<ManagedServer> {
        if parent.invocation_id.store_uuid() != self.identity.store
            || parent.job_id.store_uuid() != self.identity.store
            || parent.attempt_id.store_uuid() != self.identity.store
        {
            return Err(io::Error::other("foreign managed attestation parent"));
        }
        Ok(ManagedServer {
            server: self.identity.clone(),
            parent,
        })
    }
    pub(crate) fn sign(
        &self,
        parent: ManagedParent,
        nonce: [u8; 32],
        request_sha256: String,
        response: Response,
    ) -> io::Result<Proof> {
        use ed25519_dalek::Signer as _;
        self.context(parent)?;
        if nonce == [0; 32] || matches!(response, Response::Attested(_)) {
            return Err(io::Error::other("invalid managed response challenge"));
        }
        let mut proof = Proof {
            server: self.identity.clone(),
            parent,
            nonce,
            request_sha256,
            response: Box::new(response),
            signature: Vec::new(),
        };
        proof.signature = self.key.sign(&transcript(&proof)?).to_bytes().to_vec();
        Ok(proof)
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
pub(crate) struct Trusted {
    pub(crate) context: ManagedServer,
    key: ed25519_dalek::VerifyingKey,
}

#[cfg(target_os = "linux")]
impl Trusted {
    pub(crate) fn from_environment(
        endpoint: &str,
        executable: &std::path::Path,
        parent: Option<ManagedParent>,
    ) -> crate::Result<Option<Self>> {
        let Some(parent) = parent else {
            return Ok(None);
        };
        let Some(encoded) = std::env::var_os(ENVIRONMENT) else {
            return Ok(None);
        };
        let encoded = encoded
            .to_str()
            .ok_or_else(|| crate::Error::Protocol("invalid daemon attestation encoding".into()))?;
        if encoded.len() > 8192 {
            return Err(crate::Error::Protocol(
                "oversized daemon attestation context".into(),
            ));
        }
        Self::new(serde_json::from_str(encoded)?, endpoint, executable, parent)
            .map(Some)
            .map_err(|error| {
                crate::Error::Protocol(format!("invalid inherited daemon identity: {error}"))
            })
    }
    pub(crate) fn new(
        context: ManagedServer,
        endpoint: &str,
        executable: &std::path::Path,
        parent: ManagedParent,
    ) -> io::Result<Self> {
        let identity = &context.server;
        let (device, inode, hash) = image_identity(executable)?;
        if identity.version != 1
            || identity.store != parent.invocation_id.store_uuid()
            || identity.generation.is_nil()
            || context.parent != parent
            || identity.endpoint != endpoint
            || identity.image_device != device
            || identity.image_inode != inode
            || identity.image_sha256 != hash
            || !matches!(identity.process, ProcessIdentity::Linux { .. })
        {
            return Err(io::Error::other(
                "daemon attestation does not match selected image, endpoint and parent",
            ));
        }
        let key = ed25519_dalek::VerifyingKey::from_bytes(&identity.verifying_key)
            .map_err(io::Error::other)?;
        if key.is_weak() {
            return Err(io::Error::other("weak daemon attestation public key"));
        }
        Ok(Self { context, key })
    }
    pub(crate) fn verify(
        &self,
        proof: Proof,
        nonce: [u8; 32],
        request_sha256: &str,
    ) -> io::Result<Response> {
        if proof.server != self.context.server
            || proof.parent != self.context.parent
            || proof.nonce != nonce
            || proof.request_sha256 != request_sha256
            || matches!(*proof.response, Response::Attested(_))
        {
            return Err(io::Error::other(
                "daemon response identity/challenge mismatch",
            ));
        }
        let signature =
            ed25519_dalek::Signature::from_slice(&proof.signature).map_err(io::Error::other)?;
        self.key
            .verify_strict(&transcript(&proof)?, &signature)
            .map_err(io::Error::other)?;
        Ok(*proof.response)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[test]
    fn linux_attestation_rejects_forgery_replay_changed_image_and_generation() {
        let store = Uuid::now_v7();
        let generation = Uuid::now_v7();
        let endpoint = "/tmp/attestation-test";
        let signer = Signer::new(store, generation, endpoint.into()).unwrap();
        let parent = ManagedParent {
            job_id: crate::JobId::from_parts(store, Uuid::now_v7()),
            attempt_id: crate::AttemptId::from_parts(store, Uuid::now_v7()),
            invocation_id: crate::InvocationId::from_parts(store, Uuid::now_v7()),
        };
        let context = signer.context(parent).unwrap();
        let executable = std::env::current_exe().unwrap();
        let trusted = Trusted::new(context.clone(), endpoint, &executable, parent).unwrap();
        assert!(
            Trusted::new(
                context.clone(),
                endpoint,
                std::path::Path::new("/usr/bin/true"),
                parent
            )
            .is_err()
        );
        let hash = request_hash(&Request::Ping {}).unwrap();
        let nonce = [7; 32];
        let proof = || {
            signer
                .sign(
                    parent,
                    nonce,
                    hash.clone(),
                    Response::Pong {
                        protocol_version: crate::protocol::PROTOCOL_VERSION,
                    },
                )
                .unwrap()
        };
        assert!(matches!(
            trusted.verify(proof(), nonce, &hash).unwrap(),
            Response::Pong { .. }
        ));
        assert!(trusted.verify(proof(), [8; 32], &hash).is_err());
        assert!(trusted.verify(proof(), nonce, &"a".repeat(64)).is_err());
        let mut tampered = proof();
        tampered.response = Box::new(Response::Pong {
            protocol_version: 0,
        });
        assert!(trusted.verify(tampered, nonce, &hash).is_err());
        let mut tampered = proof();
        tampered.server.generation = Uuid::now_v7();
        assert!(trusted.verify(tampered, nonce, &hash).is_err());
        let mut tampered = proof();
        tampered.signature[0] ^= 1;
        assert!(trusted.verify(tampered, nonce, &hash).is_err());
        let other = Signer::new(store, generation, endpoint.into()).unwrap();
        assert!(
            trusted
                .verify(
                    other
                        .sign(
                            parent,
                            nonce,
                            hash.clone(),
                            Response::Pong {
                                protocol_version: crate::protocol::PROTOCOL_VERSION
                            }
                        )
                        .unwrap(),
                    nonce,
                    &hash
                )
                .is_err()
        );
    }
}
