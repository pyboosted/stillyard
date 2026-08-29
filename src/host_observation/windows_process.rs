use std::mem::size_of;

use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
};

use super::ProcessEvidence;

const MAX_PROCESSES: usize = 4096;

pub(crate) fn probe_processes() -> std::io::Result<Vec<ProcessEvidence>> {
    // SAFETY: no borrowed pointers are passed; the returned snapshot is owned below.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }
    struct Snapshot(windows_sys::Win32::Foundation::HANDLE);
    impl Drop for Snapshot {
        fn drop(&mut self) {
            // SAFETY: this guard uniquely owns the snapshot handle.
            unsafe { CloseHandle(self.0) };
        }
    }
    let snapshot = Snapshot(snapshot);
    let mut entry = PROCESSENTRY32W {
        dwSize: size_of::<PROCESSENTRY32W>() as u32,
        ..Default::default()
    };
    // SAFETY: entry has the documented size and is writable.
    if unsafe { Process32FirstW(snapshot.0, &raw mut entry) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut processes = Vec::new();
    loop {
        if processes.len() == MAX_PROCESSES {
            return Err(std::io::Error::other(
                "process inventory exceeds 4096 entries",
            ));
        }
        let length = entry
            .szExeFile
            .iter()
            .position(|unit| *unit == 0)
            .unwrap_or(entry.szExeFile.len());
        processes.push(ProcessEvidence {
            pid: entry.th32ProcessID,
            basename: String::from_utf16_lossy(&entry.szExeFile[..length]),
        });
        entry.dwSize = size_of::<PROCESSENTRY32W>() as u32;
        // SAFETY: the snapshot and output entry remain valid.
        if unsafe { Process32NextW(snapshot.0, &raw mut entry) } == 0 {
            break;
        }
    }
    Ok(processes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_process_inventory_contains_the_test_process() {
        let processes = probe_processes().unwrap();
        assert!(
            processes
                .iter()
                .any(|process| process.pid == std::process::id())
        );
    }
}
