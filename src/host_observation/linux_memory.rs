//! Guest availability is not a second copy of Windows physical/commit memory.
//! https://docs.kernel.org/filesystems/proc.html documents MemAvailable's scope.
use super::MemoryEvidence;
use std::io::{self, Read};

pub(crate) fn probe_memory() -> io::Result<MemoryEvidence> {
    let mut text = String::new();
    std::fs::File::open("/proc/meminfo")?
        .take(65537)
        .read_to_string(&mut text)?;
    if text.len() > 65536 {
        return Err(io::Error::other("memory counters exceed byte bound"));
    }
    parse(&text)
}
fn parse(text: &str) -> io::Result<MemoryEvidence> {
    let field = |name: &str| -> io::Result<u64> {
        let mut matches = text.lines().filter_map(|line| line.strip_prefix(name));
        let mut fields = matches
            .next()
            .ok_or_else(|| io::Error::other("memory availability counter missing"))?
            .split_whitespace();
        let value = fields
            .next()
            .ok_or_else(|| io::Error::other("missing memory value"))?
            .parse::<u64>()
            .map_err(io::Error::other)?;
        if fields.next() != Some("kB") || fields.next().is_some() || matches.next().is_some() {
            return Err(io::Error::other(
                "invalid memory counter units or duplicate field",
            ));
        }
        Ok(value)
    };
    let total = field("MemTotal:")?;
    let available = field("MemAvailable:")?;
    if total == 0 || available > total {
        return Err(io::Error::other("inconsistent guest memory counters"));
    }
    Ok(MemoryEvidence {
        available_physical_mb: available / 1024,
        commit_headroom_mb: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn linux_memory_does_not_fabricate_windows_commit_headroom() {
        let memory = parse(
            "MemTotal: 4096 kB\nMemAvailable: 2048 kB\nCommitLimit: 0 kB\nCommitted_AS: 999999 kB",
        )
        .unwrap();
        assert_eq!(memory.headroom_mb(), 2);
        assert_eq!(memory.commit_headroom_mb, None);
        assert!(parse("MemTotal: 4096 kB\nMemAvailable: 8192 kB").is_err());
        assert!(parse("MemTotal: 4096 kB\nMemAvailable: 2048 B").is_err());
        assert!(parse("MemTotal: 4096 kB\nMemAvailable: 2048 kB\nMemAvailable: 1024 kB").is_err());
    }
}
