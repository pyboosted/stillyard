//! Linux executor components are enabled separately after their live gates.
mod cgroup;
mod input;
mod journal;
mod launch;
pub(crate) mod registry;
pub(super) mod runtime;

pub(crate) use launch::run_stub;

pub(crate) fn initialize_history(
    path: &std::path::Path,
    journal: uuid::Uuid,
    store: uuid::Uuid,
    domain: crate::ExecutionDomainId,
) -> std::io::Result<()> {
    journal::Journal::initialize(
        path,
        journal::Anchor {
            journal,
            store,
            domain,
        },
    )?;
    Ok(())
}

pub(crate) fn installed_registry(
    path: &std::path::Path,
    journal: uuid::Uuid,
    store: uuid::Uuid,
    domain: crate::ExecutionDomainId,
    generation: uuid::Uuid,
    endpoint: String,
) -> std::io::Result<registry::Registry> {
    let history = journal::Journal::open(
        path,
        &journal::Anchor {
            journal,
            store,
            domain,
        },
    )?;
    let registry = registry::Registry::default();
    registry.install(
        history,
        crate::identity::attestation::Signer::new(store, generation, endpoint)?,
    )?;
    Ok(registry)
}
