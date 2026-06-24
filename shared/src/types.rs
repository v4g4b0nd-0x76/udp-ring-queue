use serde::Serialize;

#[derive(Debug)]
pub enum DecodedLogItems {
    UnifiedIp(Vec<UnifiedIp>),
}

pub trait LogItem {
    fn encode(&self, buf: &mut Vec<u8>);
    fn decode(buf: &mut &[u8]) -> Self
    where
        Self: Sized;
}


#[derive(Clone, Debug, Serialize)]
pub struct UnifiedIp {
    pub dest_ip: u32,
    pub tcp_packet_count: u64,
    pub tcp_byte_sum: u64,
    pub tcp_src_ip: u64,
    pub udp_packet_count: u64,
    pub udp_byte_sum: u64,
    pub udp_src_ip: u64,
    pub icmp_packet_count: u64,
    pub icmp_byte_sum: u64,
    pub icmp_src_ip: u64,
    pub timestamp: i64,

}
impl LogItem for UnifiedIp {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.dest_ip.to_le_bytes());
        buf.extend_from_slice(&self.tcp_packet_count.to_le_bytes());
        buf.extend_from_slice(&self.tcp_byte_sum.to_le_bytes());
        buf.extend_from_slice(&self.tcp_src_ip.to_le_bytes());
        buf.extend_from_slice(&self.udp_packet_count.to_le_bytes());
        buf.extend_from_slice(&self.udp_byte_sum.to_le_bytes());
        buf.extend_from_slice(&self.udp_src_ip.to_le_bytes());
        buf.extend_from_slice(&self.icmp_packet_count.to_le_bytes());
        buf.extend_from_slice(&self.icmp_byte_sum.to_le_bytes());
        buf.extend_from_slice(&self.icmp_src_ip.to_le_bytes());
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
    }

    fn decode(buf: &mut &[u8]) -> Self {
        let mut r = |n: usize| {
            let (a, b) = buf.split_at(n);
            *buf = b;
            a
        };

        let dest_ip = u32::from_le_bytes(r(4).try_into().unwrap());
        let tcp_packet_count = u64::from_le_bytes(r(8).try_into().unwrap());
        let tcp_byte_sum = u64::from_le_bytes(r(8).try_into().unwrap());
        let tcp_src_ip = u64::from_le_bytes(r(8).try_into().unwrap());
        let udp_packet_count = u64::from_le_bytes(r(8).try_into().unwrap());
        let udp_byte_sum = u64::from_le_bytes(r(8).try_into().unwrap());
        let udp_src_ip = u64::from_le_bytes(r(8).try_into().unwrap());
        let icmp_packet_count = u64::from_le_bytes(r(8).try_into().unwrap());
        let icmp_byte_sum = u64::from_le_bytes(r(8).try_into().unwrap());
        let icmp_src_ip = u64::from_le_bytes(r(8).try_into().unwrap());
        let timestamp = i64::from_le_bytes(r(8).try_into().unwrap());

        Self {
            dest_ip,
            tcp_packet_count,
            tcp_byte_sum,
            tcp_src_ip,
            udp_packet_count,
            udp_byte_sum,
            udp_src_ip,
            icmp_packet_count,
            icmp_byte_sum,
            icmp_src_ip,
            timestamp,
        }
    }
}

