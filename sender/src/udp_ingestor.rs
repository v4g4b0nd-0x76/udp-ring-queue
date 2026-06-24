use std::{
    sync::Arc,
    thread::{self, JoinHandle},
};

use crossbeam_queue::ArrayQueue;
use shared::{
    FrameType, MAX_UDP_DATAGRAM, MAX_UDP_FRAME_PAYLOAD, UDP_IO_BATCH, UDPFrame,
    pending_frames_hint,
    types::{LogItem, UnifiedIp},
};

use crate::udp_sender::UdpSender;

pub struct UDPIngestor {
    unified_ip_queue: Arc<ArrayQueue<Vec<UnifiedIp>>>,
    _writer_handles: Vec<JoinHandle<()>>,
}

struct FrameEncoder {
    frame_type: FrameType,
    payload: Vec<u8>,
    count: u32,
    wire: Vec<u8>,
    pending: Vec<Vec<u8>>,
}

impl FrameEncoder {
    fn new(frame_type: FrameType) -> Self {
        let pending_cap = pending_frames_hint(UDP_IO_BATCH, frame_type.typical_wire_bytes()).max(1);

        Self {
            frame_type,
            payload: Vec::with_capacity(MAX_UDP_FRAME_PAYLOAD),
            count: 0,
            wire: Vec::with_capacity(MAX_UDP_DATAGRAM),
            pending: Vec::with_capacity(pending_cap),
        }
    }

    fn queue_frame(&mut self, payload: &[u8], count: u32) {
        debug_assert!(payload.len() <= MAX_UDP_FRAME_PAYLOAD);

        let frame = UDPFrame {
            frame_type: self.frame_type,
            count,
            payload,
        };
        self.wire.clear();
        frame.encode(&mut self.wire);

        if self.wire.len() > MAX_UDP_DATAGRAM {
            debug_assert!(
                false,
                "frame exceeds MTU: {} bytes (payload {} + header)",
                self.wire.len(),
                payload.len()
            );
            return;
        }

        self.pending.push(std::mem::replace(
            &mut self.wire,
            Vec::with_capacity(MAX_UDP_DATAGRAM),
        ));
    }

    fn append_item<T: LogItem>(&mut self, item: &T) {
        let start_len = self.payload.len();
        item.encode(&mut self.payload);

        if self.payload.len() > MAX_UDP_FRAME_PAYLOAD {
            if self.count > 0 {
                self.payload.truncate(start_len);
                let count = self.count;
                let payload = std::mem::take(&mut self.payload);
                self.queue_frame(&payload, count);
            }
            self.payload.clear();
            self.count = 0;
            item.encode(&mut self.payload);
            if self.payload.len() <= MAX_UDP_FRAME_PAYLOAD {
                let payload = std::mem::take(&mut self.payload);
                self.queue_frame(&payload, 1);
            }
            return;
        }

        self.count += 1;
    }

    fn flush(&mut self, sender: &UdpSender) {
        if self.count > 0 {
            let count = self.count;
            let payload = std::mem::take(&mut self.payload);
            self.count = 0;
            self.queue_frame(&payload, count);
        }
        sender.send_batch(&self.pending);
        self.pending.clear();
    }
}


fn writer_loop_unified_ip(queue: Arc<ArrayQueue<Vec<UnifiedIp>>>, sender: UdpSender) {
    let mut encoder = FrameEncoder::new(FrameType::UnifiedIp);

    loop {
        let Some(batch) = queue.pop() else {
            thread::yield_now();
            continue;
        };

        encoder.payload.clear();
        encoder.count = 0;

        for item in batch.iter() {
            encoder.append_item(item);
        }

        encoder.flush(&sender);
    }
}

fn spawn_writer<F>(name: &'static str, f: F) -> JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
{
    thread::Builder::new()
        .name(name.to_string())
        .spawn(f)
        .expect("failed to spawn udp writer thread")
}

fn new_udp_sender(name: &'static str, target_ip: u32, target_port: u16) -> UdpSender {
    UdpSender::new(target_ip, target_port)
        .unwrap_or_else(|err| panic!("failed to create udp sender for {name}: {err}"))
}

impl UDPIngestor {
    pub fn new(target_ip: u32, target_port: u16) -> Self {
        let unified_ip_queue: Arc<ArrayQueue<Vec<UnifiedIp>>> =
            Arc::new(ArrayQueue::new(100_000)); // the avg size you know migh
        // be enqued before sending to
        // recever(this process happens
        // every few hundred milisec)

        let writer_handles = vec![spawn_writer("udp-writer-uniformed-ip", {
            let queue = Arc::clone(&unified_ip_queue);
            move || {
                let sender = new_udp_sender("udp-writer-processed-ip", target_ip, target_port);
                writer_loop_unified_ip(queue, sender);
            }
        })];

        Self {
            unified_ip_queue,
            _writer_handles: writer_handles,
        }
    }

    pub fn push_unified_ip(&self, items: Vec<UnifiedIp>) {
        self.unified_ip_queue.force_push(items);
    }
}
