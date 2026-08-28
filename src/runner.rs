use std::sync::{Arc, Mutex};

use crate::store::{PreparedJob, Store};

#[derive(Clone, Default)]
pub(crate) struct LiveContainments {
    #[cfg(windows)]
    inner: Arc<Mutex<std::collections::HashMap<crate::InvocationId, RegisteredContainment>>>,
}

#[cfg(windows)]
struct ContainmentRegistration {
    invocation_id: crate::InvocationId,
    registry: LiveContainments,
    retire_on_drop: bool,
}

#[cfg(windows)]
enum RegisteredContainment {
    Live(usize),
    Reconciler(usize),
    /// The OS handle is gone only after cleanup was proved. Keep a negative tombstone until the
    /// durable Containment transition commits so unrelated pipe peers never observe a missing
    /// authority for a row that is still transiently `live` in SQLite.
    Retired,
}

#[cfg(windows)]
impl Drop for ContainmentRegistration {
    fn drop(&mut self) {
        if self.retire_on_drop
            && let Ok(mut registry) = self.registry.inner.lock()
        {
            if let Some(containment) = registry.get_mut(&self.invocation_id) {
                *containment = RegisteredContainment::Retired;
            }
        }
    }
}

impl LiveContainments {
    pub(crate) fn clear(&self, invocation_id: crate::InvocationId) {
        #[cfg(windows)]
        if let Ok(mut registry) = self.inner.lock() {
            if let Some(RegisteredContainment::Reconciler(handle)) = registry.remove(&invocation_id)
            {
                // SAFETY: a reconciler entry exclusively owns the transferred Job Object handle.
                unsafe {
                    windows_sys::Win32::Foundation::CloseHandle(
                        handle as windows_sys::Win32::Foundation::HANDLE,
                    )
                };
            }
        }
        #[cfg(not(windows))]
        let _ = invocation_id;
    }

    pub(crate) fn inspect(
        &self,
        invocation_id: crate::InvocationId,
    ) -> std::io::Result<Option<crate::ReconciliationResult>> {
        #[cfg(not(windows))]
        {
            let _ = invocation_id;
            Ok(None)
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::JobObjects::{
                JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
                QueryInformationJobObject,
            };

            let registry = self
                .inner
                .lock()
                .map_err(|_| std::io::Error::other("containment registry mutex poisoned"))?;
            let Some(containment) = registry.get(&invocation_id) else {
                return Ok(None);
            };
            let handle = match containment {
                RegisteredContainment::Live(handle) | RegisteredContainment::Reconciler(handle) => {
                    *handle
                }
                RegisteredContainment::Retired => {
                    return Ok(Some(crate::ReconciliationResult::ProvenEmpty));
                }
            };
            let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
                unsafe { std::mem::zeroed() };
            // SAFETY: the registry lock keeps the owned handle live and accounting is writable.
            if unsafe {
                QueryInformationJobObject(
                    handle as windows_sys::Win32::Foundation::HANDLE,
                    JobObjectBasicAccountingInformation,
                    (&raw mut accounting).cast(),
                    std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    std::ptr::null_mut(),
                )
            } == 0
            {
                return Ok(Some(crate::ReconciliationResult::BoundaryUninspectable));
            }
            Ok(Some(if accounting.ActiveProcesses == 0 {
                crate::ReconciliationResult::ProvenEmpty
            } else {
                crate::ReconciliationResult::BoundaryNotEmpty
            }))
        }
    }

    #[cfg(windows)]
    fn register(
        &self,
        invocation_id: crate::InvocationId,
        handle: windows_sys::Win32::Foundation::HANDLE,
    ) -> std::io::Result<ContainmentRegistration> {
        let mut registry = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::other("containment registry mutex poisoned"))?;
        if registry.contains_key(&invocation_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Invocation containment is already registered",
            ));
        }
        registry.insert(invocation_id, RegisteredContainment::Live(handle as usize));
        Ok(ContainmentRegistration {
            invocation_id,
            registry: self.clone(),
            retire_on_drop: true,
        })
    }

    #[cfg(windows)]
    fn transfer_to_reconciler(
        &self,
        mut registration: ContainmentRegistration,
        invocation_id: crate::InvocationId,
        handle: windows_sys::Win32::Foundation::HANDLE,
    ) -> std::io::Result<()> {
        if !Arc::ptr_eq(&self.inner, &registration.registry.inner)
            || registration.invocation_id != invocation_id
        {
            return Err(std::io::Error::other(
                "containment registration belongs to another daemon instance",
            ));
        }
        let mut registry = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::other("containment registry mutex poisoned"))?;
        match registry.get_mut(&invocation_id) {
            Some(registered @ RegisteredContainment::Live(_)) => {
                *registered = RegisteredContainment::Reconciler(handle as usize);
            }
            _ => {
                return Err(std::io::Error::other(
                    "live containment disappeared before reconciler transfer",
                ));
            }
        }
        registration.retire_on_drop = false;
        Ok(())
    }

    pub(crate) fn contains_process(
        &self,
        invocation_id: crate::InvocationId,
        process_handle: usize,
    ) -> std::io::Result<Option<bool>> {
        #[cfg(not(windows))]
        {
            let _ = (invocation_id, process_handle);
            Ok(None)
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::JobObjects::IsProcessInJob;

            let registry = self
                .inner
                .lock()
                .map_err(|_| std::io::Error::other("containment registry mutex poisoned"))?;
            let Some(containment) = registry.get(&invocation_id) else {
                return Ok(None);
            };
            let job_handle = match containment {
                RegisteredContainment::Live(handle) | RegisteredContainment::Reconciler(handle) => {
                    *handle
                }
                RegisteredContainment::Retired => return Ok(Some(false)),
            };
            let mut member = 0;
            // SAFETY: the registry lock prevents the runner from unregistering and closing the Job
            // Object while both handles are inspected; process_handle is owned by the pipe worker.
            if unsafe {
                IsProcessInJob(
                    process_handle as windows_sys::Win32::Foundation::HANDLE,
                    job_handle as windows_sys::Win32::Foundation::HANDLE,
                    &mut member,
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Some(member != 0))
        }
    }
}

pub(crate) type ReconciliationWake = Arc<dyn Fn() + Send + Sync>;

pub(crate) fn run(
    job: PreparedJob,
    store: Arc<Mutex<Store>>,
    endpoint: Arc<str>,
    live_containments: LiveContainments,
    reconciliation_wake: ReconciliationWake,
) {
    #[cfg(windows)]
    windows::run_with_wake(
        &job,
        &store,
        &endpoint,
        &live_containments,
        &reconciliation_wake,
    );

    #[cfg(not(windows))]
    let _ = (job, store, endpoint, live_containments, reconciliation_wake);
}

#[cfg(windows)]
mod windows;
