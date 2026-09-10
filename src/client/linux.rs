//! Deadline-bounded, owner-authenticated Unix socket transport.
use super::*;
use crate::protocol::{read_frame, write_frame};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::UnixStream;

fn remaining(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "Unix request deadline elapsed",
        ));
    }
    Ok(remaining)
}

pub(crate) fn connect(endpoint: &str, deadline: Instant) -> io::Result<UnixStream> {
    let metadata = std::fs::symlink_metadata(endpoint)?;
    // SAFETY: geteuid has no preconditions.
    if !metadata.file_type().is_socket()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Unix endpoint must be an owner-only socket",
        ));
    }
    // SAFETY: these are a supported Linux socket family and flags.
    let fd = unsafe {
        libc::socket(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: socket transferred ownership of this new fd.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: all-zero sockaddr_un is valid before filling family and path.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    if endpoint.len() >= address.sun_path.len() {
        return Err(io::Error::other("Unix socket path too long"));
    }
    for (target, byte) in address.sun_path.iter_mut().zip(endpoint.bytes()) {
        *target = byte as libc::c_char;
    }
    let length =
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + endpoint.len() + 1) as libc::socklen_t;
    loop {
        remaining(deadline)?;
        // SAFETY: address contains a NUL-terminated path and its correct length.
        if unsafe { libc::connect(fd.as_raw_fd(), (&raw const address).cast(), length) } == 0 {
            break;
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EAGAIN) => {
                std::thread::sleep(remaining(deadline)?.min(Duration::from_millis(5)));
                continue;
            }
            Some(libc::EINPROGRESS) => {
                let mut pfd = libc::pollfd {
                    fd: fd.as_raw_fd(),
                    events: libc::POLLOUT,
                    revents: 0,
                };
                loop {
                    let millis = remaining(deadline)?.as_millis().clamp(1, i32::MAX as u128) as i32;
                    // SAFETY: one initialized pollfd and a bounded timeout.
                    let result = unsafe { libc::poll(&mut pfd, 1, millis) };
                    if result == 0 {
                        continue;
                    }
                    if result < 0 {
                        let error = io::Error::last_os_error();
                        if error.kind() == io::ErrorKind::Interrupted {
                            continue;
                        }
                        return Err(error);
                    }
                    let mut error = 0_i32;
                    let mut length = std::mem::size_of_val(&error) as libc::socklen_t;
                    // SAFETY: error and length are writable and socket is live.
                    if unsafe {
                        libc::getsockopt(
                            fd.as_raw_fd(),
                            libc::SOL_SOCKET,
                            libc::SO_ERROR,
                            (&raw mut error).cast(),
                            &mut length,
                        )
                    } != 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                    if error != 0 {
                        return Err(io::Error::from_raw_os_error(error));
                    }
                    break;
                }
                break;
            }
            _ => return Err(error),
        }
    }
    let stream = UnixStream::from(fd);
    stream.set_nonblocking(false)?;
    Ok(stream)
}

pub(crate) struct DeadlineStream {
    pub(crate) stream: UnixStream,
    pub(crate) deadline: Instant,
}
impl Read for DeadlineStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.stream
            .set_read_timeout(Some(remaining(self.deadline)?))?;
        self.stream.read(bytes)
    }
}
impl Write for DeadlineStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.stream
            .set_write_timeout(Some(remaining(self.deadline)?))?;
        self.stream.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

fn io_error(error: io::Error) -> Error {
    if matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    ) {
        Error::DeadlineElapsed
    } else {
        Error::Unavailable(error.to_string())
    }
}

pub(super) fn request(
    endpoint: &str,
    executable: &Path,
    request: &Request,
    deadline: Instant,
) -> Result<Response> {
    validate_endpoint(endpoint)?;
    let stream = connect(endpoint, deadline).map_err(io_error)?;
    let peer = crate::identity::linux::Process::from_socket(&stream)
        .map_err(|e| Error::Protocol(format!("cannot authenticate Unix peer: {e}")))?;
    peer.verify_executable(executable)
        .map_err(|e| Error::Protocol(format!("cannot authenticate Unix server image: {e}")))?;
    let mut stream = DeadlineStream { stream, deadline };
    write_frame(&mut stream, request).map_err(io_error)?;
    read_frame(&mut stream).map_err(io_error)
}

pub(crate) fn attested_request(
    endpoint: &str,
    trusted: &crate::identity::attestation::Trusted,
    request: &Request,
    deadline: Instant,
) -> Result<Response> {
    validate_endpoint(endpoint)?;
    let stream = connect(endpoint, deadline).map_err(io_error)?;
    let owner = crate::identity::linux::pin_socket_owner(&stream).map_err(io_error)?;
    let mut nonce = [0_u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| Error::Unavailable(e.to_string()))?;
    let request_sha256 = crate::identity::attestation::request_hash(request).map_err(io_error)?;
    #[derive(serde::Serialize)]
    struct Attested<'a> {
        operation: &'static str,
        daemon_generation: uuid::Uuid,
        parent: ManagedParent,
        nonce: [u8; 32],
        request: &'a Request,
    }
    let frame = Attested {
        operation: "attested",
        daemon_generation: trusted.context.server.generation,
        parent: trusted.context.parent,
        nonce,
        request,
    };
    let mut stream = DeadlineStream { stream, deadline };
    write_frame(&mut stream, &frame).map_err(io_error)?;
    let response = read_frame(&mut stream).map_err(io_error)?;
    owner.ensure_alive().map_err(io_error)?;
    let Response::Attested(proof) = response else {
        return Err(Error::Protocol(
            "namespace-hidden server returned no authenticated response".into(),
        ));
    };
    trusted
        .verify(*proof, nonce, &request_sha256)
        .map_err(|error| Error::Protocol(format!("daemon response authentication failed: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;

    #[test]
    fn socket_exchange_checks_owner_image_and_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ipc.sock");
        let listener = UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let input: Request = read_frame(&mut socket).unwrap();
            assert!(matches!(input, Request::Ping {}));
            write_frame(
                &mut socket,
                &Response::Pong {
                    protocol_version: PROTOCOL_VERSION,
                },
            )
            .unwrap();
            let (mut socket, _) = listener.accept().unwrap();
            let _: Request = read_frame(&mut socket).unwrap();
            std::thread::sleep(Duration::from_millis(250));
        });
        let executable = std::env::current_exe().unwrap();
        assert!(matches!(
            request(
                path.to_str().unwrap(),
                &executable,
                &Request::Ping {},
                Instant::now() + Duration::from_secs(2)
            )
            .unwrap(),
            Response::Pong { .. }
        ));
        assert!(matches!(
            request(
                path.to_str().unwrap(),
                &executable,
                &Request::Ping {},
                Instant::now() + Duration::from_millis(50)
            ),
            Err(Error::DeadlineElapsed)
        ));
        server.join().unwrap();
    }

    #[test]
    fn socket_mode_and_foreign_image_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ipc.sock");
        let _listener = UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
        let executable = std::env::current_exe().unwrap();
        assert!(
            request(
                path.to_str().unwrap(),
                &executable,
                &Request::Ping {},
                Instant::now() + Duration::from_secs(1)
            )
            .is_err()
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(matches!(
            request(
                path.to_str().unwrap(),
                Path::new("/usr/bin/true"),
                &Request::Ping {},
                Instant::now() + Duration::from_secs(1)
            ),
            Err(Error::Protocol(_))
        ));
    }
}
