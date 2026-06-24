use std::{sync::Arc, thread::JoinHandle};

use crossbeam_queue::SegQueue;
use shared::UDP_RECV_BATCH;

use crate::{logger::spawn_log_exporters, pipeline::{BufferPool, LogStore, PacketBatch, Stats}, workers::{spawn_processors, spawn_receivers, spawn_stats_reporter}};


mod pipeline;
mod workers;
mod logger;
const RECV_THR: usize = 4; // the number of logical thread to thread_park for receiving udp packets
const PROCESSOR_THR: usize = 4; // the number of logical thread to thread_park for processing udp packets
const PORT: u16 = 8080;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let stats = Arc::new(Stats::default());
    let store = Arc::new(LogStore::new( Arc::clone(&stats)));


    let pool_capacity = UDP_RECV_BATCH
        .saturating_mul(RECV_THR)
        .saturating_mul(4)
        .max(UDP_RECV_BATCH * 2);
    let pool = Arc::new(BufferPool::new(pool_capacity));
    let packet_queue: Arc<SegQueue<PacketBatch>> = Arc::new(SegQueue::new());

    spawn_stats_reporter(Arc::clone(&stats), 5); // the 5 is interval second to print and refresh the stats
        spawn_log_exporters(
        &store,
        Arc::clone(&stats),
        25_000, // the batch to index the logs
        4,
    );


    let mut blocking_handles: Vec<JoinHandle<()>> =
        Vec::with_capacity(RECV_THR+PROCESSOR_THR);
    blocking_handles.extend(spawn_receivers(
        PORT,
        RECV_THR,
        UDP_RECV_BATCH,
        Arc::clone(&packet_queue),
        Arc::clone(&pool),
    )?);
    blocking_handles.extend(spawn_processors(
        PROCESSOR_THR,
        Arc::clone(&packet_queue),
        Arc::clone(&store),
        Arc::clone(&pool),
    ));

    for handle in blocking_handles {
        handle.join().expect("worker thread panicked");
    }


    Ok(())
}
