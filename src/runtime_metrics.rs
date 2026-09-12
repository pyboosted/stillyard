//! Process-local timer expiration counters; reading them creates no timer.
//!
//! These are diagnostics, never scheduling or cleanup authority. Compare two
//! doctor snapshots from the same daemon generation over an idle interval.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy)]
pub(crate) enum Timer {
    Reactor,
    #[cfg(target_os = "linux")]
    Attached,
    Subscriber,
    #[cfg(target_os = "linux")]
    Transport,
    Backoff,
}

#[derive(Default)]
struct Counter(AtomicU64);
impl Counter {
    fn waited(&self, timed_out: bool) {
        if timed_out {
            let _ = self
                .0
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_add(1))
                });
        }
    }
    fn read(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
static REACTOR: Counter = Counter(AtomicU64::new(0));
static ATTACHED: Counter = Counter(AtomicU64::new(0));
static SUBSCRIBER: Counter = Counter(AtomicU64::new(0));
static TRANSPORT: Counter = Counter(AtomicU64::new(0));
static BACKOFF: Counter = Counter(AtomicU64::new(0));

pub(crate) fn waited(timer: Timer, timed_out: bool) {
    match timer {
        Timer::Reactor => &REACTOR,
        #[cfg(target_os = "linux")]
        Timer::Attached => &ATTACHED,
        Timer::Subscriber => &SUBSCRIBER,
        #[cfg(target_os = "linux")]
        Timer::Transport => &TRANSPORT,
        Timer::Backoff => &BACKOFF,
    }
    .waited(timed_out);
}
pub(crate) fn doctor_check() -> crate::DoctorCheck {
    crate::DoctorCheck {
        code: "runtime_timer_expirations".into(),
        status: crate::DoctorCheckStatus::Pass,
        summary: serde_json::json!({
            "reactor":REACTOR.read(),"attached":ATTACHED.read(),
            "subscriber":SUBSCRIBER.read(),"transport":TRANSPORT.read(),
            "backoff":BACKOFF.read()
        })
        .to_string(),
        remediation: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Condvar, Mutex};
    use std::time::{Duration, Instant};

    #[test]
    fn polling_control_is_counted_and_event_notification_is_not_a_timer() {
        let counter = Counter::default();
        let lock = Mutex::new(false);
        let condition = Condvar::new();
        std::thread::scope(|scope| {
            let guard = lock.lock().unwrap();
            scope.spawn(|| {
                *lock.lock().unwrap() = true;
                condition.notify_one();
            });
            let (_guard, result) = condition
                .wait_timeout_while(guard, Duration::from_secs(5), |ready| !*ready)
                .unwrap();
            counter.waited(result.timed_out());
        });
        assert_eq!(counter.read(), 0);
        let begin = Instant::now();
        for _ in 0..5 {
            let (_guard, result) = condition
                .wait_timeout(lock.lock().unwrap(), Duration::from_millis(2))
                .unwrap();
            counter.waited(result.timed_out());
        }
        let elapsed = begin.elapsed().as_secs_f64();
        assert_eq!(counter.read(), 5);
        assert!(
            counter.read() as f64 * 60.0 / elapsed > 6.0,
            "polling control unexpectedly met the attached idle timer budget"
        );
    }
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "run alone as a system Job to measure the real attached wait path"]
    fn attached_wait_polling_mutant_exceeds_idle_budget() {
        use crate::store::attached::driver::ReleaseState;
        fn observed() -> u64 {
            serde_json::from_str::<serde_json::Value>(&doctor_check().summary).unwrap()["attached"]
                .as_u64()
                .unwrap()
        }
        let state = ReleaseState::default();
        let before = observed();
        state.wake();
        state.wait(Duration::from_secs(1));
        assert_eq!(
            observed(),
            before,
            "an event notification counted as a timer"
        );
        // Mutate the real driver's 20s idle wait into an actual 10ms polling
        // loop. Exercise its instrumentation and doctor projection, not a
        // separate Counter that would still pass if driver wiring disappeared.
        let begin = Instant::now();
        for _ in 0..8 {
            state.wait(Duration::from_millis(10));
        }
        let elapsed = begin.elapsed().as_secs_f64();
        let expirations = observed() - before;
        assert_eq!(expirations, 8);
        let per_minute = expirations as f64 * 60.0 / elapsed;
        assert!(
            per_minute > 6.0,
            "the polling mutant incorrectly met the idle budget"
        );
        println!(
            "\n{}",
            serde_json::json!({"control":"attached_wait_polling_mutant", "interval_ms":10,
            "duration_seconds":elapsed, "timer_expirations":expirations,
            "expirations_per_minute":per_minute, "idle_budget_pass":false})
        );
    }
}
