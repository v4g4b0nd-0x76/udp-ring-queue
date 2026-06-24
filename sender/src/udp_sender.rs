
//! Sends pre-built UDP datagrams to a connected peer.
//!
//! Each buffer passed to [`UdpSender`] is one complete on-the-wire datagram.
//! `send_batch` / `sendmmsg` issue multiple datagrams in one syscall — they do
//! **not** merge buffers into a single packet. MTU sizing is enforced upstream
//! in `FrameEncoder` (payload ≤ `MAX_UDP_FRAME_PAYLOAD`, wire ≤ `MAX_UDP_DATAGRAM`).

use std::{
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket},
};

use shared::{MAX_UDP_DATAGRAM};
use socket2::{Domain, Protocol, Socket, Type};

#[cfg(target_os = "linux")]
use libc::{iovec, mmsghdr, sendmmsg};

const SEND_BUFFER_SIZE: usize = 16 * 1024 * 1024;

pub struct UdpSender {
    socket: UdpSocket,
}

impl UdpSender {
    pub fn new(target_ip: u32, target_port: u16) -> io::Result<Self> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.set_send_buffer_size(SEND_BUFFER_SIZE)?;

        let socket: UdpSocket = socket.into();
        let addr = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from(target_ip),
            target_port,
        ));
        socket.connect(&addr)?;

        Ok(Self { socket })
    }

    #[inline]
    pub fn send(&self, buf: &[u8]) -> bool {
        if !datagram_fits_mtu(buf) {
            return false;
        }
        self.socket.send(buf).is_ok()
    }

    pub fn send_batch(&self, bufs: &[Vec<u8>]) {
        if bufs.is_empty() {
            return;
        }

        #[cfg(target_os = "linux")]
        if sendmmsg_batch(&self.socket, bufs) {
            return;
        }

        for buf in bufs {
            let _ = self.send(buf);
        }
    }
}

#[inline]
fn datagram_fits_mtu(buf: &[u8]) -> bool {
    debug_assert!(
        buf.len() <= MAX_UDP_DATAGRAM,
        "datagram exceeds MTU: {} > {MAX_UDP_DATAGRAM}",
        buf.len()
    );
    buf.len() <= MAX_UDP_DATAGRAM
}

#[cfg(target_os = "linux")]
fn sendmmsg_batch(socket: &UdpSocket, bufs: &[Vec<u8>]) -> bool {
    let fd = socket.as_raw_fd();

    for chunk in bufs.chunks(UDP_IO_BATCH) {
        let mut iovecs = Vec::with_capacity(chunk.len());
        let mut msgs = Vec::with_capacity(chunk.len());

        for buf in chunk {
            if !datagram_fits_mtu(buf) {
                continue;
            }
            iovecs.push(iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            });
            msgs.push(unsafe { std::mem::zeroed::<mmsghdr>() });
        }

        if msgs.is_empty() {
            continue;
        }

        for (i, iov) in iovecs.iter_mut().enumerate() {
            msgs[i].msg_hdr.msg_iov = iov as *mut iovec;
            msgs[i].msg_hdr.msg_iovlen = 1;
        }

        let sent = unsafe { sendmmsg(fd, msgs.as_mut_ptr(), msgs.len() as u32, 0) };
        if sent <= 0 {
            return false;
        }
    }

    true
}
