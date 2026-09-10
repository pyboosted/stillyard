//! A trusted bridge worker lives outside every user Invocation namespace.
use super::*;
use crate::store::attached::driver::{Driver, ReleaseState};
use std::sync::Weak;

pub(super) fn start(
    store: SharedStore,
    scheduler: Weak<DaemonReactor>,
    releases: Arc<ReleaseState>,
) -> Result<()> {
    std::thread::Builder::new()
        .name("stillyard-coordinator-bridge".into())
        .spawn(move || {
            let mut driver = None;
            let mut reported = None;
            loop {
                let Some(scheduler) = scheduler.upgrade() else {
                    break;
                };
                let step = (|| -> std::result::Result<bool, StoreError> {
                    if driver.is_none() {
                        driver = Some(Driver::connect(&store, Arc::clone(&releases))?);
                    }
                    driver.as_mut().unwrap().step(&store)
                })();
                match step {
                    Ok(changed) => {
                        reported = None;
                        // Refresh local readiness for every candidate before the
                        // next bounded offer inspection. No domain-head shortcut.
                        let interval = store
                            .lock()
                            .ok()
                            .and_then(|store| store.attached_poll_interval().ok())
                            .unwrap_or(Duration::from_secs(1));
                        if changed || interval < Duration::from_secs(1) {
                            scheduler.wake();
                        }
                        if !changed {
                            releases.wait(interval);
                        }
                    }
                    Err(error) => {
                        // Store -> barrier ordering matches the release path. A
                        // known disconnect fences starts before any reconnect.
                        let offline = store
                            .lock()
                            .map_err(|_| "store mutex poisoned".to_owned())
                            .and_then(|store| store.attached_offline().map_err(|e| e.to_string()));
                        let _ = releases.disconnect();
                        driver = None;
                        let detail = offline.err().unwrap_or_else(|| error.to_string());
                        if reported.as_ref() != Some(&detail) {
                            eprintln!("stillyard attached coordinator unavailable: {detail}");
                            reported = Some(detail);
                        }
                        scheduler.wake();
                        std::thread::sleep(Duration::from_secs(1));
                        crate::runtime_metrics::waited(
                            crate::runtime_metrics::Timer::Backoff,
                            true,
                        );
                    }
                }
            }
        })
        .map_err(|e| Error::Unavailable(format!("cannot start coordinator bridge: {e}")))?;
    Ok(())
}
