//! Runtime capability descriptions used by lifecycle records and public diagnostics.
//! A future backend is advertised only after it actually provides the capability.

pub(crate) fn containment_strength() -> &'static str {
    if cfg!(windows) {
        "windows_job_object"
    } else if cfg!(target_os = "linux") {
        "linux_cgroup_v2"
    } else {
        "unsupported"
    }
}

pub(crate) fn containment_check_code() -> &'static str {
    if cfg!(windows) {
        "containment.windows_job_object"
    } else if cfg!(target_os = "linux") {
        "containment.linux_cgroup_v2"
    } else {
        "containment.unsupported"
    }
}

pub(crate) fn containment_summary() -> &'static str {
    if cfg!(windows) {
        "born-contained Windows Job Object capability"
    } else if cfg!(target_os = "linux") {
        "born-contained cgroup v2 executor requires an installed Linux runtime and delegated boundary"
    } else {
        "this platform has no implemented invocation containment backend"
    }
}

pub(crate) fn host_name() -> Option<String> {
    #[cfg(windows)]
    {
        std::env::var("COMPUTERNAME").ok()
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/kernel/hostname")
            .ok()
            .map(|value| value.trim().into())
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        None
    }
}

pub(crate) fn durable_filesystem_name() -> &'static str {
    if cfg!(windows) {
        "local_fixed_ntfs"
    } else if cfg!(target_os = "linux") {
        "local_ext4"
    } else {
        "unsupported"
    }
}

pub(crate) fn session_survival_status() -> crate::DoctorCheckStatus {
    if cfg!(windows) {
        crate::DoctorCheckStatus::Pass
    } else {
        crate::DoctorCheckStatus::Unknown("not_verified".into())
    }
}
