//! Visible PID namespace inventory. An unreadable executable is not absence.
use super::ProcessEvidence;
use std::io::{self, Read};

pub(crate) fn probe_processes() -> io::Result<Vec<ProcessEvidence>> {
    let mut result = Vec::new();
    let mut count = 0;
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        count += 1;
        if count > 16384 {
            return Err(io::Error::other(
                "process inventory exceeds supported bound",
            ));
        }
        // Opening the proc directory pins this identity: a reused PID cannot
        // redirect subsequent reads through /proc/self/fd to its replacement.
        let directory = match std::fs::File::open(entry.path()) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        use std::os::fd::AsRawFd;
        let path = std::path::PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
        let mut status = String::new();
        match std::fs::File::open(path.join("status"))
            .and_then(|f| f.take(65537).read_to_string(&mut status))
        {
            Ok(_) if status.len() <= 65536 => {}
            Ok(_) => return Err(io::Error::other("process status exceeds byte bound")),
            Err(error) if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ESRCH)) => {
                continue;
            }
            Err(error) => return Err(error),
        }
        if status.lines().any(|line| {
            line.strip_prefix("Kthread:")
                .is_some_and(|value| value.trim() == "1")
                || line
                    .strip_prefix("State:")
                    .is_some_and(|value| value.trim_start().starts_with('Z'))
        }) {
            continue;
        }
        let executable = match std::fs::read_link(path.join("exe")) {
            Ok(executable) => executable,
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    "visible process executable unavailable; namespace-wide process coverage is incomplete",
                ));
            }
        };
        let basename = executable
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("non-UTF8 process executable name"))?;
        let basename = basename.strip_suffix(" (deleted)").unwrap_or(basename);
        result.push(ProcessEvidence {
            pid,
            basename: basename.into(),
        });
    }
    result.sort_by_key(|process| process.pid);
    Ok(result)
}
