use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crossbeam_queue::ArrayQueue;
use parking_lot::Mutex;
use shared::{
    MAX_UDP_DATAGRAM,
    types::{DecodedLogItems, UnifiedIp},
};
use libc::{ iovec, mmsghdr, recvmmsg};


pub struct Stats {
    pub unified_ip: Arc<StreamCounter>,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            unified_ip: Arc::new(StreamCounter::default()),
        }
    }
}

#[derive(Default)]
pub struct StreamCounter {
    received: AtomicU64,
    flushed: AtomicU64,
    evicted: AtomicU64,
}

impl StreamCounter {
    #[inline]
    pub fn add_received(&self, n: u64) {
        self.received.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_flushed(&self, n: u64) {
        self.flushed.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_evicted(&self, n: u64) {
        self.evicted.fetch_add(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.received.load(Ordering::Relaxed),
            self.flushed.load(Ordering::Relaxed),
            self.evicted.load(Ordering::Relaxed),
        )
    }
}

// Stream Ring Buffer

pub struct RingBuffer<T> {
    slots: Box<[Option<T>]>,
    head: usize,
    len: usize,
}

impl<T> RingBuffer<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let slots = (0..capacity)
            .map(|_| None)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            slots,
            head: 0,
            len: 0,
        }
    }

    pub fn push(&mut self, item: T) {
        let cap = self.slots.len();
        if cap == 0 {
            drop(item);
            return;
        }
        if self.len >= cap {
            self.drop_oldest();
        }
        let idx = (self.head + self.len) % cap;
        self.slots[idx] = Some(item);
        self.len += 1;
    }

    pub fn drop_oldest(&mut self) {
        let _ = self.pop_front();
    }

    fn pop_front(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let cap = self.slots.len();
        let idx = self.head;
        let item = self.slots[idx].take()?;
        self.head = (self.head + 1) % cap;
        self.len -= 1;
        Some(item)
    }

    pub fn drain_batch(&mut self, max: usize, out: &mut Vec<T>) {
        out.clear();
        while out.len() < max {
            match self.pop_front() {
                Some(item) => out.push(item),
                None => break,
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

struct StreamBufferInner<T> {
    ring: RingBuffer<T>,
    export_batch: VecDeque<T>,
}

impl<T> StreamBufferInner<T> {
    fn backlog_len(&self) -> usize {
        self.ring.len() + self.export_batch.len()
    }
}

pub struct StreamBuffer<T> {
    inner: Mutex<StreamBufferInner<T>>,
    capacity: usize,
}

impl<T> StreamBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(StreamBufferInner {
                ring: RingBuffer::with_capacity(capacity),
                export_batch: VecDeque::new(),
            }),
            capacity,
        }
    }

    /// Always accepts the item. When elastic backlog is full, drops oldest queued docs first.
    pub fn push(&self, item: T, counter: &StreamCounter) {
        let mut inner = self.inner.lock();
        while inner.backlog_len() >= self.capacity {
            evict_oldest(&mut inner, counter);
        }
        inner.ring.push(item);
        counter.add_received(1);
    }

    pub fn push_batch<I: IntoIterator<Item = T>>(&self, items: I, counter: &StreamCounter) {
        let mut inner = self.inner.lock();
        let mut received = 0u64;
        for item in items {
            while inner.backlog_len() >= self.capacity {
                evict_oldest(&mut inner, counter);
            }
            inner.ring.push(item);
            received += 1;
        }
        if received > 0 {
            counter.add_received(received);
        }
    }

    pub fn take_batch_for_export(&self, max: usize) -> Vec<T> {
        let mut inner = self.inner.lock();
        while inner.export_batch.len() < max {
            let need = max - inner.export_batch.len();
            let mut drained = Vec::new();
            inner.ring.drain_batch(need, &mut drained);
            if drained.is_empty() {
                break;
            }
            inner.export_batch.extend(drained);
        }
        let take = max.min(inner.export_batch.len());
        inner.export_batch.drain(..take).collect()
    }

    pub fn restore_export_batch(&self, batch: Vec<T>) {
        if batch.is_empty() {
            return;
        }
        let mut inner = self.inner.lock();
        let mut merged = VecDeque::with_capacity(batch.len() + inner.export_batch.len());
        merged.extend(batch);
        merged.extend(inner.export_batch.drain(..));
        inner.export_batch = merged;
    }

    pub fn backlog_len(&self) -> usize {
        self.inner.lock().backlog_len()
    }
}

fn evict_oldest<T>(inner: &mut StreamBufferInner<T>, counter: &StreamCounter) {
    if inner.export_batch.pop_front().is_some() {
        counter.add_evicted(1);
        return;
    }
    if inner.ring.len() > 0 {
        inner.ring.drop_oldest();
        counter.add_evicted(1);
    }
}

pub struct LogStore {
    pub unified_ip: Arc<StreamBuffer<UnifiedIp>>,
    stats: Arc<Stats>,
}

impl LogStore {
    pub fn new(stats: Arc<Stats>) -> Self {
        Self {
            stats,
            unified_ip: Arc::new(StreamBuffer::new(100_000)), // the maximum capacity before logging after reaching this capacity the old data would be evicted
        }
    }

    pub fn push_decoded(&self, decoded: DecodedLogItems) {
        match decoded {
            DecodedLogItems::UnifiedIp(items) => {
                self.unified_ip.push_batch(items, &self.stats.unified_ip);
            }
        }
    }
}

pub type PacketBuf = Box<[u8; MAX_UDP_DATAGRAM]>;
pub type Packet = (PacketBuf, usize);

pub struct PacketBatch {
    pub packets: Vec<Packet>,
}

pub struct BufferPool {
    free: ArrayQueue<PacketBuf>,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        let free = ArrayQueue::new(capacity);
        for _ in 0..capacity {
            let _ = free.push(Box::new([0u8; MAX_UDP_DATAGRAM]));
        }
        Self { free }
    }

    pub fn acquire(&self) -> PacketBuf {
        self.free
            .pop()
            .unwrap_or_else(|| Box::new([0u8; MAX_UDP_DATAGRAM]))
    }

    pub fn release(&self, buf: PacketBuf) {
        let _ = self.free.push(buf);
    }
}

// stats

#[derive(Default, Clone, Copy)]
struct StreamSnapshot {
    received: u64,
    flushed: u64,
    evicted: u64,
}

#[derive(Default)]
pub struct StatsSnapshot {
    unified_ip: StreamSnapshot,
}

pub fn log_interval_stats(stats: &Stats, prev: &mut StatsSnapshot, interval_secs: u64) {
    eprintln!("stats interval_secs={interval_secs}");
    log_stream_delta("unified_ip", &stats.unified_ip, &mut prev.unified_ip);
}

fn log_stream_delta(stream: &str, counter: &Arc<StreamCounter>, prev: &mut StreamSnapshot) {
    let (received, flushed, evicted) = counter.snapshot();
    let received_delta = received.saturating_sub(prev.received);
    let flushed_delta = flushed.saturating_sub(prev.flushed);
    let evicted_delta = evicted.saturating_sub(prev.evicted);
    let backlog = received.saturating_sub(flushed).saturating_sub(evicted);
    eprintln!(
        "stats stream={stream} received={received_delta} flushed={flushed_delta} \
         evicted={evicted_delta} backlog={backlog}"
    );
    prev.received = received;
    prev.flushed = flushed;
    prev.evicted = evicted;
}


// RecvMmsg

pub struct RecvMmsg {
    pub bufs: Vec<PacketBuf>,
    pub iovecs: Vec<iovec>,
    pub msgs: Vec<mmsghdr>,
}

impl RecvMmsg {
    pub fn new(batch: usize, pool: &BufferPool) -> Self {
        let mut bufs = Vec::with_capacity(batch);
        let mut iovecs = vec![unsafe { std::mem::zeroed::<iovec>() }; batch];
        let mut msgs = vec![unsafe { std::mem::zeroed::<mmsghdr>() }; batch];

        for _ in 0..batch {
            bufs.push(pool.acquire());
        }

        for i in 0..batch {
            iovecs[i].iov_base = bufs[i].as_mut_ptr() as *mut libc::c_void;
            iovecs[i].iov_len = MAX_UDP_DATAGRAM as _;
            msgs[i].msg_hdr.msg_iov = &mut iovecs[i] as *mut iovec;
            msgs[i].msg_hdr.msg_iovlen = 1;
            msgs[i].msg_hdr.msg_name = std::ptr::null_mut();
            msgs[i].msg_hdr.msg_namelen = 0;
        }

        Self { bufs, iovecs, msgs }
    }

    pub fn refresh_slot(&mut self, idx: usize) {
        self.iovecs[idx].iov_base = self.bufs[idx].as_mut_ptr() as *mut libc::c_void;
        self.iovecs[idx].iov_len = MAX_UDP_DATAGRAM as _;
        self.msgs[idx].msg_hdr.msg_iov = &mut self.iovecs[idx] as *mut iovec;
    }
}

