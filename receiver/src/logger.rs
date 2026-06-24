use std::{fmt::Display, sync::Arc, time::Duration};

use serde::Serialize;
use shared::types::UnifiedIp;
use tokio::sync::Semaphore;

use crate::pipeline::{LogStore, Stats, StreamBuffer, StreamCounter};

pub fn spawn_log_exporters(
    store: &LogStore,
    stats: Arc<Stats>,
    batch_size: usize,
    export_concurrency: usize,
) {
    let slots = [ExportSlot::UnifiedIp(ExportSlotInner::new(
        "unfied_ip",
        Arc::clone(&store.unified_ip),
        Arc::clone(&stats.unified_ip),
    ))];

    let sem = Arc::new(Semaphore::new(export_concurrency.max(1)));
    for slot in slots {
        let sem = Arc::clone(&sem);
        tokio::spawn(stream_exporter_loop(slot, batch_size, sem));
    }
}

struct ExportSlotInner<T> {
    stream: &'static str,
    buffer: Arc<StreamBuffer<T>>,
    counter: Arc<StreamCounter>,
}

impl<T> ExportSlotInner<T> {
    fn new(
        stream: &'static str,
        buffer: Arc<StreamBuffer<T>>,
        counter: Arc<StreamCounter>,
    ) -> Self {
        Self {
            stream,
            buffer,
            counter,
        }
    }

    fn backlog(&self) -> usize {
        self.buffer.backlog_len()
    }
}

enum ExportSlot {
    UnifiedIp(ExportSlotInner<UnifiedIp>),
}

impl ExportSlot {
    fn backlog(&self) -> usize {
        match self {
            Self::UnifiedIp(s) => s.backlog(),
        }
    }

    async fn flush_batch(&mut self, batch_size: usize) -> Result<usize, String> {
        match self {
            Self::UnifiedIp(s) => flush_stream_slot(s,  batch_size).await,
        }
    }

    fn stream(&self) -> &'static str {
        match self {
            Self::UnifiedIp(s) => s.stream,
        }
    }

    fn counter(&self) -> &Arc<StreamCounter> {
        match self {
            Self::UnifiedIp(s) => &s.counter,
        }
    }
}

async fn flush_stream_slot<T>(
    slot: &mut ExportSlotInner<T>,
    batch_size: usize,
) -> Result<usize, String>
where
    T: Serialize + Send + Sync + Clone + 'static,
{
    let batch = slot.buffer.take_batch_for_export(batch_size);
    if batch.is_empty() {
        return Ok(0);
    }

    let doc_count = batch.len();

    match log_data(&batch).await {
        Ok(n) => {
            slot.counter.add_flushed(n as u64);
            Ok(n)
        }
        Err(err) => {
            slot.buffer.restore_export_batch(batch);
            Err(format!(
                "stream={} docs={doc_count} err={err}",
                slot.stream
            ))
        }
    }
}
async fn log_data<T: serde::Serialize + Send + Sync + Clone + 'static>(
    docs: &[T],
) -> LoggerResult<usize> {
    let len = docs.len();
    drop(docs);
    Ok(len)
}

pub type LoggerResult<T> = Result<T, LoggerError>;
#[derive(Debug)]
pub enum LoggerError {
    Serialize,
}

impl Display for LoggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoggerError::Serialize => write!(f, "serialize error"),
        }
    }
} 

async fn stream_exporter_loop(mut slot: ExportSlot, batch_size: usize, sem: Arc<Semaphore>) {
    let retry_backoff = Duration::from_millis(250);
    let idle_poll = Duration::from_millis(1);

    loop {
        if slot.backlog() == 0 {
            tokio::time::sleep(idle_poll).await;
            continue;
        }

        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .expect("export semaphore closed");

        let result = slot.flush_batch(batch_size).await;
        drop(permit);

        match result {
            Ok(0) => {
                tokio::task::yield_now().await;
            }
            Ok(_) => {}
            Err(err) => {
                let counter = slot.counter();
                let stream = slot.stream();
                let (received, flushed, evicted) = counter.snapshot();
                eprintln!(
                    "log flush failed stream={stream} \
                     received={received} flushed={flushed} evicted={evicted} backlog={} {err}",
                    received.saturating_sub(flushed).saturating_sub(evicted)
                );
                tokio::time::sleep(retry_backoff).await;
            }
        }
    }
}
