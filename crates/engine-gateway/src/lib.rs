//! Command gateway: accepts inbound order flow, assigns sequence numbers,
//! writes to WAL, dispatches to matching engine, and returns events.
//!
//! The gateway is the synchronous entry point for a single symbol partition.
//! It enforces backpressure via a configurable queue depth limit.

use engine_core::matcher::MatchingEngine;
use engine_replay::{WalWriter, write_snapshot};
use engine_types::*;
use std::collections::VecDeque;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("Queue full: backpressure limit of {limit} reached")]
    QueueFull { limit: usize },
    #[error("WAL error: {0}")]
    Wal(#[from] engine_replay::ReplayError),
    #[error("Symbol mismatch: expected {expected}, got {got}")]
    SymbolMismatch { expected: Symbol, got: Symbol },
}

/// Configuration for the gateway.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub symbol: Symbol,
    pub wal_path: PathBuf,
    pub snapshot_path: PathBuf,
    /// Maximum number of pending commands before rejecting new submissions.
    pub max_queue_depth: usize,
    /// Take a snapshot every N commands.
    pub snapshot_interval: u64,
}

/// The gateway for a single symbol partition.
pub struct Gateway {
    config: GatewayConfig,
    engine: MatchingEngine,
    wal: WalWriter,
    next_seq_no: SeqNo,
    commands_since_snapshot: u64,
    pending: VecDeque<Command>,
}

impl Gateway {
    /// Create a new gateway. For recovery, use `Gateway::recover()`.
    pub fn new(config: GatewayConfig) -> Result<Self, GatewayError> {
        let engine = MatchingEngine::new(config.symbol);
        let wal = WalWriter::open(&config.wal_path)?;
        Ok(Self {
            config,
            engine,
            wal,
            next_seq_no: 1,
            commands_since_snapshot: 0,
            pending: VecDeque::new(),
        })
    }

    /// Recover a gateway from snapshot + WAL.
    pub fn recover(config: GatewayConfig) -> Result<(Self, Vec<EngineEvent>), GatewayError> {
        let snap = if config.snapshot_path.exists() {
            Some(config.snapshot_path.as_path())
        } else {
            None
        };

        let (engine, events) =
            engine_replay::recover(snap, &config.wal_path, config.symbol)?;

        let next_seq = engine.book.last_seq_no + 1;
        let wal = WalWriter::open(&config.wal_path)?;

        Ok((
            Self {
                config,
                engine,
                wal,
                next_seq_no: next_seq,
                commands_since_snapshot: 0,
                pending: VecDeque::new(),
            },
            events,
        ))
    }

    /// Submit a new order. The gateway assigns the sequence number.
    pub fn submit_new(&mut self, order: NewOrder) -> Result<Vec<EngineEvent>, GatewayError> {
        let cmd = Command {
            seq_no: self.next_seq_no,
            timestamp: now_nanos(),
            payload: CommandPayload::New(order),
        };
        self.process_command(cmd)
    }

    /// Submit a cancel request.
    pub fn submit_cancel(&mut self, cancel: CancelOrder) -> Result<Vec<EngineEvent>, GatewayError> {
        let cmd = Command {
            seq_no: self.next_seq_no,
            timestamp: now_nanos(),
            payload: CommandPayload::Cancel(cancel),
        };
        self.process_command(cmd)
    }

    /// Submit a replace request.
    pub fn submit_replace(&mut self, replace: ReplaceOrder) -> Result<Vec<EngineEvent>, GatewayError> {
        let cmd = Command {
            seq_no: self.next_seq_no,
            timestamp: now_nanos(),
            payload: CommandPayload::Replace(replace),
        };
        self.process_command(cmd)
    }

    /// Core processing: WAL append, match, maybe snapshot.
    fn process_command(&mut self, cmd: Command) -> Result<Vec<EngineEvent>, GatewayError> {
        // Backpressure check.
        if self.pending.len() >= self.config.max_queue_depth {
            return Err(GatewayError::QueueFull {
                limit: self.config.max_queue_depth,
            });
        }

        // WAL append (before processing for crash safety).
        self.wal.append(&cmd)?;
        self.next_seq_no += 1;

        // Process.
        let result = self.engine.process(&cmd);
        self.commands_since_snapshot += 1;

        // Periodic snapshot.
        if self.commands_since_snapshot >= self.config.snapshot_interval {
            write_snapshot(&self.engine, &self.config.snapshot_path)?;
            self.commands_since_snapshot = 0;
        }

        Ok(result.events)
    }

    /// Access the underlying engine (read-only).
    pub fn engine(&self) -> &MatchingEngine {
        &self.engine
    }

    /// Current sequence number (next to be assigned).
    pub fn next_seq_no(&self) -> SeqNo {
        self.next_seq_no
    }
}

/// Monotonic timestamp. In production this would use a real clock.
fn now_nanos() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as Timestamp
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_config(name: &str) -> GatewayConfig {
        let dir = std::env::temp_dir().join("engine_gateway_test");
        fs::create_dir_all(&dir).unwrap();
        GatewayConfig {
            symbol: 1,
            wal_path: dir.join(format!("{name}.wal.jsonl")),
            snapshot_path: dir.join(format!("{name}.snap.json")),
            max_queue_depth: 1000,
            snapshot_interval: 100,
        }
    }

    fn cleanup(config: &GatewayConfig) {
        let _ = fs::remove_file(&config.wal_path);
        let _ = fs::remove_file(&config.snapshot_path);
    }

    #[test]
    fn gateway_basic_flow() {
        let config = test_config("basic");
        cleanup(&config);

        let mut gw = Gateway::new(config.clone()).unwrap();
        let events = gw
            .submit_new(NewOrder {
                order_id: 1,
                symbol: 1,
                side: Side::Buy,
                order_type: OrderType::Limit,
                tif: TimeInForce::GTC,
                condition: OrderCondition::None,
                price: 100,
                qty: 10,
                stop_price: 0,
            })
            .unwrap();

        assert!(!events.is_empty());
        assert_eq!(gw.next_seq_no(), 2);

        cleanup(&config);
    }

    #[test]
    fn gateway_recovery() {
        let config = test_config("recovery");
        cleanup(&config);

        // First session: submit some orders.
        {
            let mut gw = Gateway::new(config.clone()).unwrap();
            gw.submit_new(NewOrder {
                order_id: 1, symbol: 1, side: Side::Sell,
                order_type: OrderType::Limit, tif: TimeInForce::GTC,
                condition: OrderCondition::None, price: 100, qty: 10, stop_price: 0,
            }).unwrap();
            gw.submit_new(NewOrder {
                order_id: 2, symbol: 1, side: Side::Buy,
                order_type: OrderType::Limit, tif: TimeInForce::GTC,
                condition: OrderCondition::None, price: 99, qty: 5, stop_price: 0,
            }).unwrap();
        }

        // Recover.
        let (gw, _events) = Gateway::recover(config.clone()).unwrap();
        assert_eq!(gw.engine().book.orders.len(), 2);
        assert_eq!(gw.next_seq_no(), 3);

        cleanup(&config);
    }
}
