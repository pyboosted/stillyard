# Prepared lifecycle window i

Only native default Jobs `helper-control` and read-only `preflight` were submitted.
Terminate and shutdown JobSpecs remain UNSUBMITTED. No approval.json exists.
The approval.template.json file is deliberately non-authorizing: user_agreement
is null and no_new_submissions is false. After the user agrees, record that exact
agreement and copy running_distributions and the complete sorted
foreign_work_candidates array verbatim from a fresh preflight before.json. The
agreed window must freeze new submissions. A changed inventory fails closed.

This prepared approval scope covers distro terminate, whole-VM shutdown, and
subsequent shared-slot canaries. Sleep/resume, logout/login and reboot need their
own prepared observation tooling and separate window; none is authorized here.

h independent review found no remaining safety blocker or false-success path.
i addresses its concrete robustness findings: preflight keepalive relation,
BOM-safe quiet-list parsing, local fixed-drive paths, actual process wait-state
instead of ambiguous exit code 259, starter remaining deadline and exchange
failure detection. Native helper-control actually rejects a process exited with
259, parses empty/BOM lists and rejects a WSL UNC evidence path. No disruptive
operation was needed for those controls.

Win32 primary API contracts consulted:
- [WaitForSingleObject](https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitforsingleobject): process handle with SYNCHRONIZE, zero-time wait, WAIT_TIMEOUT means nonsignaled.
- [GetDriveTypeW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdrivetypew): reject anything except DRIVE_FIXED; UNC/WSL paths rejected before resolution.

The existing journal checksum_valid field is emitted only after an actual
checksum equality requirement; it does not claim an independent second checksum.
