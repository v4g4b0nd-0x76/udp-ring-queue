use libc::{EAGAIN, MSG_DONTWAIT, iovec, mmsghdr, recvmmsg};
use shared::{MAX_UDP_DATAGRAM, UDPFrame, types::DecodedLogItems};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io,
    net::{Ipv4Addr, SocketAddrV4, UdpSocket},
    os::fd::AsRawFd,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam_queue::SegQueue;

use crate::pipeline::{BufferPool, LogStore, Packet, PacketBatch, RecvMmsg, Stats, StatsSnapshot, log_interval_stats};

pub fn spawn_stats_reporter(stats: Arc<Stats>, interval_secs: u64) -> JoinHandle<()> {
    thread::Builder::new()
        .name("stats-reporter".into())
        .spawn(move || {
            let mut prev = StatsSnapshot::default();
            let interval = Duration::from_secs(interval_secs.max(1));
            loop {
                thread::sleep(interval);
                log_interval_stats(&stats, &mut prev, interval_secs);
            }
        })
        .expect("failed to spawn stats reporter")
}

fn make_socket(port: u16) -> io::Result<UdpSocket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.set_recv_buffer_size(64 * 1024 * 1024)?;
    sock.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())?;
    Ok(sock.into())
}


pub fn spawn_receivers(
    port: u16,
    receiver_threads: usize,
    recv_batch: usize,
    out: Arc<SegQueue<PacketBatch>>,
    pool: Arc<BufferPool>,
) -> io::Result<Vec<JoinHandle<()>>> {
    let mut handles = Vec::with_capacity(receiver_threads);
    for thread_idx in 0..receiver_threads {
        let sock = make_socket(port)?;
        let out = Arc::clone(&out);
        let pool = Arc::clone(&pool);

        handles.push(
            thread::Builder::new()
                .name(format!("udp-recv-{thread_idx}"))
                .spawn(move || receiver_loop(sock, recv_batch, out, pool))?,
        );
    }
    Ok(handles)
}

fn receiver_loop(
    sock: UdpSocket,
    batch: usize,
    out: Arc<SegQueue<PacketBatch>>,
    pool: Arc<BufferPool>,
) {
    let fd = sock.as_raw_fd();
    let mut recv = RecvMmsg::new(batch, &pool);
    let mut pending = Vec::with_capacity(batch * 2);

    loop {
        recv_into_scratch(fd, batch, &pool, &mut recv, &mut pending, false);
        while recv_into_scratch(fd, batch, &pool, &mut recv, &mut pending, true) {}

        if !pending.is_empty() {
            out.push(PacketBatch {
                packets: std::mem::take(&mut pending),
            });
            pending = Vec::with_capacity(batch * 2);
        }
    }
}

fn recv_into_scratch(
    fd: i32,
    batch: usize,
    pool: &BufferPool,
    recv: &mut RecvMmsg,
    scratch: &mut Vec<Packet>,
    nonblocking: bool,
) -> bool {
    let n = unsafe { recvmmsg_batch(fd, recv.msgs.as_mut_ptr(), batch as u32, nonblocking) };

    if n <= 0 {
        if nonblocking {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(EAGAIN) {
                return false;
            }
        }
        return false;
    }

    for i in 0..n as usize {
        let len = recv.msgs[i].msg_len as usize;
        if len == 0 || len > MAX_UDP_DATAGRAM {
            continue;
        }
        let buf = std::mem::replace(&mut recv.bufs[i], pool.acquire());
        scratch.push((buf, len));
        recv.refresh_slot(i);
    }
    true
}

unsafe fn recvmmsg_batch(fd: i32, msgs: *mut mmsghdr, batch: u32, nonblocking: bool) -> i32 {
    #[cfg(not(all(target_env = "musl", target_os = "linux")))]
    {
        let flags = if nonblocking { MSG_DONTWAIT } else { 0 };
        unsafe { recvmmsg(fd, msgs, batch, flags, std::ptr::null_mut()) }
    }
}

fn decode_datagram(buf: &[u8]) -> Option<DecodedLogItems> {
    let frame = UDPFrame::decode(buf)?;
    frame.decode_items()
}

pub fn spawn_processors(
    processor_threads: usize,
    queue: Arc<SegQueue<PacketBatch>>,
    store: Arc<LogStore>,
    pool: Arc<BufferPool>,
) -> Vec<JoinHandle<()>> {
    let n = processor_threads.max(1);
    (0..n)
        .map(|thread_idx| {
            let queue = Arc::clone(&queue);
            let store = Arc::clone(&store);
            let pool = Arc::clone(&pool);
            thread::Builder::new()
                .name(format!("udp-processor-{thread_idx}"))
                .spawn(move || processor_loop(queue, store, pool))
                .expect("failed to spawn udp processor thread")
        })
        .collect()
}

fn processor_loop(queue: Arc<SegQueue<PacketBatch>>, store: Arc<LogStore>, pool: Arc<BufferPool>) {
    loop {
        while let Some(batch) = queue.pop() {
            for (buf, len) in batch.packets {
                if let Some(decoded) = decode_datagram(&buf[..len]) {
                    store.push_decoded(decoded);
                }
                pool.release(buf);
            }
        }
        thread::yield_now();
        std::thread::sleep(Duration::from_micros(50));
    }
}

