//! Mailbox orchestration only; the Linux subject uses the real executor journal.
//! The installed CLI is the outer Job primary and owns protected bootstrap.
use std::path::PathBuf;
use std::time::{Duration, Instant};
use uuid::Uuid;

pub struct Subject {
    mailbox: PathBuf,
    finished: bool,
}
impl Subject {
    pub fn start() -> Self {
        let mailbox = PathBuf::from(std::env::var_os("MR_WSL_FAULT_MAILBOX_WINDOWS").unwrap());
        assert!(mailbox.is_absolute() && mailbox.is_dir());
        let subject = Self {
            mailbox,
            finished: false,
        };
        subject.publish("controller-ready.json", &true);
        subject
    }
    pub fn receive<T: serde::de::DeserializeOwned>(&self, name: &str) -> T {
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            match std::fs::read(self.mailbox.join(name)) {
                Ok(bytes) => return serde_json::from_slice(&bytes).unwrap(),
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound && Instant::now() < until =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("Linux subject {name}: deadline or mailbox failure: {error}"),
            }
        }
    }
    pub fn publish(&self, name: &str, value: &impl serde::Serialize) {
        let stage = self.mailbox.join(format!(".{}", Uuid::now_v7()));
        std::fs::write(&stage, serde_json::to_vec(value).unwrap()).unwrap();
        std::fs::rename(stage, self.mailbox.join(name)).unwrap();
    }
    pub fn finish(&mut self) {
        let destination = PathBuf::from(std::env::var_os("MR_WSL_FAULT_EVIDENCE").unwrap());
        std::fs::create_dir_all(&destination).unwrap();
        // Configuration includes a fixture pairing secret: never export it.
        // The launcher separately requires the installed primary's bootstrap seal.
        for name in [
            "manager.json",
            "allocation.json",
            "intent.json",
            "ticket.json",
            "live.json",
            "capacity-reduced.json",
            "capacity-restored.json",
            "live-after-capacity-reduction.json",
            "peer-fenced.json",
            "live-after-peer-fence.json",
            "guest-reset-rejected.json",
            "live-after-coordinator-reset.json",
            "seal.json",
        ] {
            std::fs::copy(self.mailbox.join(name), destination.join(name)).unwrap();
        }
        self.finished = true;
    }
}
impl Drop for Subject {
    fn drop(&mut self) {
        if !self.finished {
            // Best effort even during unwinding; outer bootstrap still owns
            // recursive cleanup when either subject fails or stops responding.
            let _ = std::fs::write(self.mailbox.join("abort.json"), b"true");
        }
    }
}
