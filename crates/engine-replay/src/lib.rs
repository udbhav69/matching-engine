//! Write-ahead log, snapshots, and deterministic replay/recovery.
//!
//! # Design
//!
//! Every inbound [`Command`] is appended to a WAL file before processing.
//! The WAL is a newline-delimited JSON file (one JSON object per line).
//!
//! Periodically, the engine takes a snapshot: a serialized [`OrderBook`]
//! written to a separate file. The snapshot includes the `last_seq_no` so
//! recovery can skip already-processed commands.
//!
//! Recovery:
//! 1. Load the latest snapshot (if any).
//! 2. Replay all WAL entries with `seq_no > snapshot.last_seq_no`.
//! 3. The resulting book state and event stream must be identical to the
//!    original run (determinism guarantee).

use engine_core::matcher::MatchingEngine;
use engine_types::*;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Bincode error: {0}")]
    Bincode(String),
    #[error("Sequence gap: expected > {expected}, got {got}")]
    SequenceGap { expected: SeqNo, got: SeqNo },
}

/// Write-ahead log writer.
pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    count: u64,
}

impl WalWriter {
    /// Open (or create) a WAL file for appending.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReplayError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            writer,
            path,
            count: 0,
        })
    }

    /// Append a command to the WAL. Must be called *before* processing.
    pub fn append(&mut self, cmd: &Command) -> Result<(), ReplayError> {
        serde_json::to_writer(&mut self.writer, cmd)?;
        self.writer.write_all(b"\n")?;
        self.count += 1;
        // Flush every command for durability. In production you might
        // batch-flush or use fdatasync periodically.
        self.writer.flush()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

/// Read and iterate over WAL entries.
pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReplayError> {
        let file = File::open(path.as_ref())?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }
}

impl Iterator for WalReader {
    type Item = Result<Command, ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => None, // EOF
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    return self.next();
                }
                Some(serde_json::from_str(trimmed).map_err(ReplayError::from))
            }
            Err(e) => Some(Err(ReplayError::from(e))),
        }
    }
}

/// Write a snapshot of the engine's order book.
pub fn write_snapshot(
    engine: &MatchingEngine,
    path: impl AsRef<Path>,
) -> Result<(), ReplayError> {
    let json = serde_json::to_vec(&engine.book)?;
    fs::write(path.as_ref(), json)?;
    Ok(())
}

/// Load a snapshot into a new engine.
pub fn load_snapshot(path: impl AsRef<Path>) -> Result<MatchingEngine, ReplayError> {
    let data = fs::read(path.as_ref())?;
    let book: engine_core::book::OrderBook = serde_json::from_slice(&data)?;
    Ok(MatchingEngine { book })
}

/// Replay WAL entries onto an engine, returning all events produced.
///
/// Only replays commands with `seq_no > engine.book.last_seq_no`.
pub fn replay_wal(
    engine: &mut MatchingEngine,
    wal_path: impl AsRef<Path>,
) -> Result<Vec<EngineEvent>, ReplayError> {
    let reader = WalReader::open(wal_path)?;
    let start_seq = engine.book.last_seq_no;
    let mut all_events = Vec::new();

    for entry in reader {
        let cmd = entry?;
        if cmd.seq_no <= start_seq {
            continue;
        }
        let result = engine.process(&cmd);
        all_events.extend(result.events);
    }

    Ok(all_events)
}

/// Full recovery: load snapshot (if exists), then replay WAL.
pub fn recover(
    snapshot_path: Option<&Path>,
    wal_path: &Path,
    symbol: Symbol,
) -> Result<(MatchingEngine, Vec<EngineEvent>), ReplayError> {
    let mut engine = match snapshot_path {
        Some(p) if p.exists() => load_snapshot(p)?,
        _ => MatchingEngine::new(symbol),
    };

    let events = if wal_path.exists() {
        replay_wal(&mut engine, wal_path)?
    } else {
        Vec::new()
    };

    Ok((engine, events))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use engine_types::*;

    // Helper to create a temp dir (using std since tempfile may not be a dep).
    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("engine_replay_test");
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    fn make_cmd(seq_no: SeqNo) -> Command {
        Command {
            seq_no,
            timestamp: seq_no * 1000,
            payload: CommandPayload::New(NewOrder {
                order_id: seq_no,
                symbol: 1,
                side: if seq_no % 2 == 0 { Side::Buy } else { Side::Sell },
                order_type: OrderType::Limit,
                tif: TimeInForce::GTC,
                condition: OrderCondition::None,
                price: 100,
                qty: 10,
                stop_price: 0,
            }),
        }
    }

    #[test]
    fn wal_write_and_read_back() {
        let path = temp_path("test_wal.jsonl");
        let _ = fs::remove_file(&path);

        {
            let mut wal = WalWriter::open(&path).unwrap();
            for i in 1..=5 {
                wal.append(&make_cmd(i)).unwrap();
            }
        }

        let reader = WalReader::open(&path).unwrap();
        let cmds: Vec<Command> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(cmds.len(), 5);
        assert_eq!(cmds[0].seq_no, 1);
        assert_eq!(cmds[4].seq_no, 5);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn snapshot_and_restore() {
        let snap_path = temp_path("test_snap.json");
        let _ = fs::remove_file(&snap_path);

        let mut engine = MatchingEngine::new(1);
        engine.process(&make_cmd(1));
        engine.process(&make_cmd(2));

        write_snapshot(&engine, &snap_path).unwrap();

        let restored = load_snapshot(&snap_path).unwrap();
        assert_eq!(restored.book.last_seq_no, engine.book.last_seq_no);
        assert_eq!(restored.book.orders.len(), engine.book.orders.len());

        let _ = fs::remove_file(&snap_path);
    }

    #[test]
    fn deterministic_replay_matches_original() {
        let wal_path = temp_path("test_replay_wal.jsonl");
        let _ = fs::remove_file(&wal_path);

        // Original run.
        let mut engine1 = MatchingEngine::new(1);
        let mut original_events = Vec::new();
        {
            let mut wal = WalWriter::open(&wal_path).unwrap();
            for i in 1..=10 {
                let cmd = make_cmd(i);
                wal.append(&cmd).unwrap();
                let result = engine1.process(&cmd);
                original_events.extend(result.events);
            }
        }

        // Replay from scratch.
        let mut engine2 = MatchingEngine::new(1);
        let replayed_events = replay_wal(&mut engine2, &wal_path).unwrap();

        // Both must produce identical event streams.
        assert_eq!(original_events, replayed_events);
        // And identical book state.
        assert_eq!(engine1.book.last_seq_no, engine2.book.last_seq_no);
        assert_eq!(engine1.book.orders.len(), engine2.book.orders.len());

        let _ = fs::remove_file(&wal_path);
    }

    #[test]
    fn recovery_from_snapshot_plus_wal() {
        let snap_path = temp_path("test_recovery_snap.json");
        let wal_path = temp_path("test_recovery_wal.jsonl");
        let _ = fs::remove_file(&snap_path);
        let _ = fs::remove_file(&wal_path);

        // Process 5 commands, snapshot, then process 5 more.
        let mut engine = MatchingEngine::new(1);
        let mut all_events = Vec::new();
        {
            let mut wal = WalWriter::open(&wal_path).unwrap();
            for i in 1..=5 {
                let cmd = make_cmd(i);
                wal.append(&cmd).unwrap();
                let result = engine.process(&cmd);
                all_events.extend(result.events);
            }
            write_snapshot(&engine, &snap_path).unwrap();
            for i in 6..=10 {
                let cmd = make_cmd(i);
                wal.append(&cmd).unwrap();
                let result = engine.process(&cmd);
                all_events.extend(result.events);
            }
        }

        // Recover.
        let (recovered_engine, _recovered_events) =
            recover(Some(snap_path.as_ref()), &wal_path, 1).unwrap();

        // Verify final state matches.
        assert_eq!(recovered_engine.book.last_seq_no, engine.book.last_seq_no);
        assert_eq!(
            recovered_engine.book.orders.len(),
            engine.book.orders.len()
        );

        let _ = fs::remove_file(&snap_path);
        let _ = fs::remove_file(&wal_path);
    }
}
