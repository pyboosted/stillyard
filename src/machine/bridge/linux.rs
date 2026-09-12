//! Linux side of the installed Windows bridge, with absolute pipe deadlines.
use super::*;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

struct TimedIo<T> {
    io: T,
    deadline: Instant,
}
impl<T: AsRawFd> TimedIo<T> {
    fn new(io: T, deadline: Instant) -> io::Result<Self> {
        // SAFETY: borrowed live fd; changing status flags does not transfer it.
        let flags = unsafe { libc::fcntl(io.as_raw_fd(), libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(io.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { io, deadline })
    }
    fn ready(&self, events: i16) -> io::Result<()> {
        loop {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Windows bridge deadline elapsed",
                ));
            }
            let mut fd = libc::pollfd {
                fd: self.io.as_raw_fd(),
                events,
                revents: 0,
            };
            // SAFETY: writable pollfd and finite timeout.
            let result = unsafe {
                libc::poll(
                    &mut fd,
                    1,
                    remaining.as_millis().clamp(1, i32::MAX as u128) as i32,
                )
            };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if result == 0 {
                crate::runtime_metrics::waited(crate::runtime_metrics::Timer::Transport, true);
            }
            if result > 0 {
                if fd.revents & libc::POLLNVAL != 0 {
                    return Err(invalid("invalid Windows bridge pipe"));
                }
                // HUP/ERR is consumed by read/write as EOF or EPIPE.
                return Ok(());
            }
        }
    }
}
impl<T: Read + AsRawFd> Read for TimedIo<T> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            self.ready(libc::POLLIN)?;
            match self.io.read(bytes) {
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    continue;
                }
                result => return result,
            }
        }
    }
}
impl<T: Write + AsRawFd> Write for TimedIo<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            self.ready(libc::POLLOUT)?;
            match self.io.write(bytes) {
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    continue;
                }
                result => return result,
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.io.flush()
    }
}

pub(crate) struct Bridge {
    child: Option<Child>,
    input: TimedIo<ChildStdin>,
    output: TimedIo<ChildStdout>,
    broken: bool,
}

impl Bridge {
    pub(crate) fn spawn(
        executable: &Path,
        expected_sha256: &str,
        endpoint: &str,
        interop_socket: &Path,
    ) -> io::Result<Self> {
        if !executable.is_absolute() || !interop_socket.is_absolute() || expected_sha256.len() != 64
        {
            return Err(invalid(
                "bridge needs explicit installed image and interop binding",
            ));
        }
        // WSL can fall back to another session when WSL_INTEROP names a missing
        // server. Require the configured external keepalive binding explicitly;
        // absence must fence reconnect, not borrow an unrelated terminal's life.
        let socket = std::fs::canonicalize(interop_socket)?;
        let metadata = std::fs::metadata(&socket)?;
        if socket.parent() != Some(Path::new("/run/WSL"))
            || !metadata.file_type().is_socket()
            || metadata.uid() != 0
        {
            return Err(invalid(
                "installed WSL interop binding is not a root-owned runtime socket",
            ));
        }
        let mut image = File::open(executable)?;
        let mut hash = Sha256::new();
        let mut buffer = [0_u8; 65536];
        let mut total = 0_u64;
        loop {
            let n = image.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            total += n as u64;
            if total > 512 * 1024 * 1024 {
                return Err(invalid("installed bridge image exceeds bound"));
            }
            hash.update(&buffer[..n]);
        }
        if format!("{:x}", hash.finalize()) != expected_sha256 {
            return Err(invalid(
                "installed Windows bridge differs from installation anchor",
            ));
        }
        let mut child = Command::new(executable)
            .args(["--endpoint", endpoint, "machine", "bridge"])
            .env_clear()
            .env("WSL_INTEROP", socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| invalid("bridge has no input pipe"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| invalid("bridge has no output pipe"))?;
        let deadline = Instant::now() + Duration::from_secs(10);
        let pipes = TimedIo::new(input, deadline)
            .and_then(|input| Ok((input, TimedIo::new(output, deadline)?)));
        match pipes {
            Ok((input, output)) => Ok(Self {
                child: Some(child),
                input,
                output,
                broken: false,
            }),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                Err(error)
            }
        }
    }

    pub(crate) fn exchange(
        &mut self,
        command: BridgeCommand,
        deadline: Instant,
    ) -> crate::Result<BridgeOutcome> {
        if self.broken {
            return Err(crate::Error::Unavailable(
                "Windows bridge requires reconnect and journal recovery".into(),
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(crate::Error::DeadlineElapsed);
        }
        let request = BridgeRequest {
            version: BRIDGE_VERSION,
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            request_id: Uuid::now_v7(),
            deadline_millis: remaining.as_millis().clamp(1, 30000) as u32,
            command,
        };
        self.input.deadline = deadline;
        self.output.deadline = deadline;
        self.broken = true;
        let result = (|| -> io::Result<BridgeReply> {
            write_frame(&mut self.input, &request)?;
            read_frame(&mut self.output)
        })();
        let reply = result.map_err(|e| {
            if e.kind() == io::ErrorKind::TimedOut {
                crate::Error::DeadlineElapsed
            } else {
                crate::Error::Unavailable(format!("Windows bridge disconnected: {e}"))
            }
        })?;
        if reply.version != BRIDGE_VERSION
            || reply.protocol_version != request.protocol_version
            || reply.request_id != request.request_id
        {
            return Err(crate::Error::Protocol(
                "Windows bridge response correlation/version mismatch".into(),
            ));
        }
        self.broken = false;
        match reply.outcome {
            BridgeOutcome::Error { code, detail } => Err(crate::Error::Rejected { code, detail }),
            outcome => Ok(outcome),
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            // Reaping the transport has no effect on executor/cgroup obligations.
            let _ = std::thread::Builder::new()
                .name("stillyard-bridge-reap".into())
                .spawn(move || {
                    let _ = child.wait();
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;
    #[test]
    fn linux_bridge_deadline_covers_partial_frame_and_writes() {
        let (left, mut right) = UnixStream::pair().unwrap();
        let mut input = TimedIo::new(left, Instant::now() + Duration::from_millis(40)).unwrap();
        right.write_all(&[40, 0]).unwrap();
        let error = read_frame::<BridgeReply>(&mut input).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let mut output = TimedIo::new(right, Instant::now() + Duration::from_millis(40)).unwrap();
        assert_eq!(
            output
                .write_all(&vec![0; 8 * 1024 * 1024])
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
    }
}
