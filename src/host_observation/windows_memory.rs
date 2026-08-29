use std::mem::size_of;

use windows_sys::Win32::System::ProcessStatus::{GetPerformanceInfo, PERFORMANCE_INFORMATION};
use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

use super::MemoryEvidence;

const MIB: u64 = 1024 * 1024;

pub(crate) fn probe_memory() -> std::io::Result<MemoryEvidence> {
    let mut status = MEMORYSTATUSEX {
        dwLength: size_of::<MEMORYSTATUSEX>() as u32,
        ..Default::default()
    };
    // SAFETY: status has the documented length and is writable.
    if unsafe { GlobalMemoryStatusEx(&raw mut status) } == 0 {
        return Err(std::io::Error::last_os_error());
    }

    let mut performance = PERFORMANCE_INFORMATION {
        cb: size_of::<PERFORMANCE_INFORMATION>() as u32,
        ..Default::default()
    };
    // SAFETY: performance has the documented size and is writable.
    if unsafe {
        GetPerformanceInfo(
            &raw mut performance,
            size_of::<PERFORMANCE_INFORMATION>() as u32,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }

    let commit_pages = performance
        .CommitLimit
        .checked_sub(performance.CommitTotal)
        .ok_or_else(|| std::io::Error::other("CommitTotal exceeds CommitLimit"))?;
    let commit_bytes = (commit_pages as u64)
        .checked_mul(performance.PageSize as u64)
        .ok_or_else(|| std::io::Error::other("commit headroom byte conversion overflow"))?;
    Ok(MemoryEvidence {
        available_physical_mb: status.ullAvailPhys / MIB,
        commit_headroom_mb: commit_bytes / MIB,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_windows_memory_contains_physical_and_commit_headroom() {
        let memory = probe_memory().unwrap();
        assert!(memory.available_physical_mb > 0);
        assert!(memory.commit_headroom_mb > 0);
        assert!(memory.headroom_mb() <= memory.available_physical_mb);
        assert!(memory.headroom_mb() <= memory.commit_headroom_mb);
    }
}
