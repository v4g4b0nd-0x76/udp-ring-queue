use crate::types::{DecodedLogItems, LogItem, UnifiedIp};

pub mod types;


pub struct UDPFrame<'a> {
    pub frame_type: FrameType,
    pub count: u32,
    pub payload: &'a [u8],
}

impl<'a> UDPFrame<'a> {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.push(self.frame_type as u8);
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(self.payload);
    }

    pub fn decode(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < UDP_FRAME_HEADER_SIZE {
            return None;
        }
        let frame_type = FrameType::from_u8(buf[0])?;
        let count = u32::from_le_bytes(buf[1..UDP_FRAME_HEADER_SIZE].try_into().ok()?);
        let payload = &buf[UDP_FRAME_HEADER_SIZE..];
        Some(Self {
            frame_type,
            count,
            payload,
        })
    }

    pub fn decode_items(&self) -> Option<DecodedLogItems> {
        let mut buf = self.payload;
        let count = self.count as usize;
        match self.frame_type {
            FrameType::UnifiedIp => {
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    items.push(UnifiedIp::decode(&mut buf));
                }
                Some(DecodedLogItems::UnifiedIp(items))
            }
        }
    }
}





#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameType {
    UnifiedIp,
}

impl FrameType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::UnifiedIp),
            _ => None,
        }
    }

    pub const fn typical_wire_bytes(self) -> usize {
        match self {
            Self::UnifiedIp => UNIFIED_IP_WIRE_BYTES, // Estimated frame size for this FrameType }
        }
    }

    pub const fn items_per_frame(self) -> usize {
        crate::items_per_frame(self.typical_wire_bytes())
    }
}

pub const fn pending_frames_hint(items: usize, wire_bytes: usize) -> usize {
    let per_frame = items_per_frame(wire_bytes);
    if per_frame == 0 {
        return items;
    }
    items.div_ceil(per_frame)
}

pub const UDP_IO_BATCH_TARGET_BYTES: usize = 256 * 1024;

/// Datagrams per `sendmmsg` on the sender process.
pub const UDP_IO_BATCH: usize = UDP_IO_BATCH_TARGET_BYTES / MAX_UDP_DATAGRAM;
pub const UDP_FRAME_HEADER_SIZE: usize = 5;
pub const MAX_UDP_DATAGRAM: usize = 1472;
pub const MAX_UDP_FRAME_PAYLOAD: usize = MAX_UDP_DATAGRAM - UDP_FRAME_HEADER_SIZE;
pub const fn items_per_frame(wire_bytes: usize) -> usize {
    if wire_bytes == 0 {
        return 0;
    }
    MAX_UDP_FRAME_PAYLOAD / wire_bytes
}
pub const UDP_RECV_BATCH_TARGET_BYTES: usize = 512 * 1024;

pub const UDP_RECV_BATCH: usize = UDP_RECV_BATCH_TARGET_BYTES / MAX_UDP_DATAGRAM;

pub const UNIFIED_IP_WIRE_BYTES: usize = 0; // base on fields you shall manually calculate the
// possible frame size the calculation always is not
// correct especially if you are using complex or
// dynamic data types located in heap(so if possible use
// fix size parameters to have best result)
