use std::{net::Ipv4Addr, thread, time::Duration};

use crate::udp_ingestor::UDPIngestor;
use rand::{Rng, RngExt};
use shared::types::UnifiedIp;

mod udp_ingestor;
mod udp_sender;

fn main() {
    let ingestor = UDPIngestor::new(ip_to_u32("127.0.0.1"), 8080);
    let mut mock_data: Vec<UnifiedIp> = Vec::with_capacity(1_000_000);
    produce_mock_data(&mut mock_data);
    mock_data.chunks(50_000).into_iter().for_each(|chunk| {
        ingestor.push_unified_ip(chunk.to_vec());
        thread::sleep(Duration::from_millis(50)); // let sender send data 
    });
}

fn produce_mock_data(mock_data: &mut Vec<UnifiedIp>) {
    let add_count = 1_000_000;
    let mut rng = rand::rng();
    mock_data.reserve(add_count);

    for _ in 0..add_count {
        mock_data.push(UnifiedIp {
            dest_ip: rng.next_u32(),
            tcp_packet_count: rng.random_range(0..10_000),
            tcp_byte_sum: rng.random_range(0..5_000_000),
            tcp_src_ip: rng.next_u64(),
            udp_packet_count: rng.random_range(0..5_000),
            udp_byte_sum: rng.random_range(0..2_500_000),
            udp_src_ip: rng.next_u64(),
            icmp_packet_count: rng.random_range(0..500),
            icmp_byte_sum: rng.random_range(0..50_000),
            icmp_src_ip: rng.next_u64(),
            timestamp: rng.random_range(1700000000..1735000000),
        });
    }
}

fn ip_to_u32(ip: &str) -> u32 {
    ip.parse::<Ipv4Addr>().ok().map(u32::from).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_mock_data() {
        const MOCK_LEN: usize = 1_000_000;
        let mut mock_data = Vec::with_capacity(MOCK_LEN);
        produce_mock_data(&mut mock_data);
        assert_eq!(mock_data.len(), MOCK_LEN);
    }
}
