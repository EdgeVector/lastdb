//! Nonblocking Unix socket operations share one overall request deadline.

use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::{ffi::OsStrExt, net::UnixStream};
use std::path::Path;
use std::time::{Duration, Instant};

pub(super) fn connect(path: &Path, deadline: Instant) -> Result<UnixStream, String> {
    remaining(deadline)?;
    let address = address(path)?;
    // SAFETY: socket creates a fresh owned descriptor or returns -1.
    let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if raw < 0 {
        return Err("cannot create owner request socket".into());
    }
    // SAFETY: this frame takes the fresh descriptor's only ownership.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    // SAFETY: the descriptor is live; F_SETFD takes this integer flag argument.
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err("cannot protect owner request socket".into());
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        let enabled: libc::c_int = 1;
        let length = libc::socklen_t::try_from(std::mem::size_of_val(&enabled))
            .map_err(|_| "invalid owner socket option length")?;
        // SAFETY: the option points to a live integer of the supplied length.
        if unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                (&raw const enabled).cast(),
                length,
            )
        } < 0
        {
            return Err("cannot protect owner request writes".into());
        }
    }
    let stream = UnixStream::from(fd);
    stream
        .set_nonblocking(true)
        .map_err(|_| "cannot set owner socket nonblocking")?;
    remaining(deadline)?;
    let length = libc::socklen_t::try_from(std::mem::size_of_val(&address))
        .map_err(|_| "invalid owner socket address length")?;
    // SAFETY: address is initialized, correctly sized, and lives through connect.
    let result = unsafe { libc::connect(stream.as_raw_fd(), (&raw const address).cast(), length) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if !matches!(
            error.raw_os_error(),
            Some(libc::EINPROGRESS | libc::EALREADY)
        ) {
            return Err("cannot connect to existing owner daemon".into());
        }
        wait(&stream, libc::POLLOUT, deadline)?;
        if stream
            .take_error()
            .map_err(|_| "cannot inspect owner connection")?
            .is_some()
        {
            return Err("owner socket connection failed".into());
        }
    }
    remaining(deadline)?;
    stream
        .peer_addr()
        .map_err(|_| "owner socket did not connect")?;
    Ok(stream)
}

fn address(path: &Path) -> Result<libc::sockaddr_un, String> {
    let path = path.as_os_str().as_bytes();
    // SAFETY: all-zero is valid for the integer fields and byte array.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if path.is_empty() || path.len() >= address.sun_path.len() || path.contains(&0) {
        return Err("invalid owner socket path length".into());
    }
    address.sun_family = libc::AF_UNIX
        .try_into()
        .map_err(|_| "invalid Unix socket family")?;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        address.sun_len = u8::try_from(std::mem::size_of_val(&address))
            .map_err(|_| "invalid owner socket address length")?;
    }
    // SAFETY: the checked destination has room for every byte and a trailing NUL.
    unsafe {
        std::ptr::copy_nonoverlapping(
            path.as_ptr(),
            address.sun_path.as_mut_ptr().cast(),
            path.len(),
        );
    }
    Ok(address)
}

pub(super) fn write_all(
    stream: &mut UnixStream,
    mut bytes: &[u8],
    deadline: Instant,
) -> Result<(), String> {
    while !bytes.is_empty() {
        remaining(deadline)?;
        match stream.write(bytes) {
            Ok(0) => return Err("owner socket closed during request".into()),
            Ok(count) => bytes = &bytes[count..],
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                wait(stream, libc::POLLOUT, deadline)?;
            }
            Err(_) => return Err("owner status request failed".into()),
        }
    }
    remaining(deadline).map(|_| ())
}

pub(super) fn read(
    stream: &mut UnixStream,
    bytes: &mut [u8],
    deadline: Instant,
) -> Result<usize, String> {
    loop {
        remaining(deadline)?;
        match stream.read(bytes) {
            Ok(count) => return Ok(count),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                wait(stream, libc::POLLIN, deadline)?;
            }
            Err(_) => return Err("owner status reply failed".into()),
        }
    }
}

fn remaining(deadline: Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| "owner status deadline expired".into())
}

fn wait(stream: &UnixStream, events: libc::c_short, deadline: Instant) -> Result<(), String> {
    loop {
        let left = remaining(deadline)?;
        let timeout = i32::try_from(left.as_millis()).unwrap_or(i32::MAX).max(1);
        let mut pending = libc::pollfd {
            fd: stream.as_raw_fd(),
            events,
            revents: 0,
        };
        // SAFETY: one initialized pollfd contains a descriptor held by this call.
        let count = unsafe { libc::poll(&raw mut pending, 1, timeout) };
        remaining(deadline)?;
        if count > 0 {
            if pending.revents & libc::POLLNVAL != 0 {
                return Err("invalid owner socket descriptor".into());
            }
            return Ok(());
        }
        if count < 0 && std::io::Error::last_os_error().kind() != ErrorKind::Interrupted {
            return Err("owner socket wait failed".into());
        }
    }
}
