# Matching Engine

A production-oriented Rust limit order matching engine inspired by [Liquibook](https://github.com/enewhuis/liquibook), implemented natively in Rust with deterministic behavior and strong correctness guarantees.

## Architecture

```
crates/
  engine-types/    Domain types: Price, Qty, OrderId, Side, events, commands
  engine-core/     Order book (BTreeMap + FIFO queues) and matching engine
  engine-feed/     Market data tracking: BBO change detection, depth snapshots
  engine-replay/   WAL (newline-delimited JSON), snapshots, deterministic replay
  engine-gateway/  Command gateway with sequence assignment and backpressure
  bench/           Criterion benchmarks for throughput/latency
  sim/             Simulation binary: mixed order flow with event printing
```

### Core Design

**Single-threaded per symbol partition.** Each symbol gets its own `MatchingEngine` instance running a deterministic command loop. Parallelism is achieved across partitions (symbols), not within.

**Integer arithmetic only.** Prices are `u64` ticks, quantities are `u64` lots. No floating-point math anywhere in the matching path, ensuring determinism across platforms.

**Explicit sequencing.** Every command is stamped with a monotonic `seq_no`. The engine state is a pure function of the ordered command stream. Wall-clock timestamps are carried for audit but never influence matching.

## Matching Rules

| Rule | Description |
|------|-------------|
| **Price-time priority** | Best price first, then FIFO within a price level |
| **Trade price** | Always the maker's (resting) price |
| **Limit orders** | Rest on book if no match; cross at the resting price |
| **Market orders** | Match at any price; never rest on book |
| **IOC** | Fill what you can, cancel the remainder immediately |
| **FOK** | Pre-check available qty; fill entirely or reject |
| **AON** | Pre-check available qty; fill entirely or rest (GTC) / reject (IOC) |
| **Stop** | Accepted and held pending; triggered by trades at/through stop price |
| **Cancel** | Remove resting order; reject if not found or already filled |
| **Replace** | Change price/reduce qty; order re-inserted at back of new level |

## Events

The engine emits the following events for each processed command:

- `OrderAccepted` - order passed validation
- `OrderRejected { reason }` - validation failure (duplicate ID, zero qty, etc.)
- `Trade { maker, taker, price, qty }` - partial or full fill
- `OrderCanceled { remaining_qty }` - explicit cancel or IOC/market remainder
- `CancelRejected { reason }` - cancel failed
- `OrderReplaced { new_price, new_qty }` - successful amend
- `ReplaceRejected { reason }` - amend failed
- `BboChanged { bid, ask }` - best bid/offer snapshot after every command
- `DepthChanged { side, levels }` - price level depth updates

## Engine Invariants

These invariants are enforced at all times and verified by property tests:

1. **No invalid resting quantities** - every open order has `open_qty() > 0`
2. **Book never crossed** - `best_bid < best_ask` when both sides have orders
3. **FIFO preserved** - within each price level, orders match in insertion order
4. **Accounting consistency** - `filled_qty <= original_qty` for every order
5. **Side consistency** - orders on the bid side are `Side::Buy`, asks are `Side::Sell`
6. **Aggregate qty consistency** - each level's `total_qty` equals the sum of its orders' open quantities

## Replay Guarantees

Determinism is the core design principle:

1. Same command sequence -> identical event stream (verified by property tests)
2. **WAL**: every command appended before processing (crash safety)
3. **Snapshots**: periodic serialization of complete book state
4. **Recovery**: load latest snapshot, replay WAL entries with `seq_no > snapshot.last_seq_no`
5. **Verification**: recovery produces identical final state to original run

## Benchmarks

Run benchmarks: `cargo bench --bench matching`

Results on Apple Silicon (M-series):

| Benchmark | Throughput |
|-----------|-----------|
| Insert (no match), 100K orders | ~22.5M ops/sec |
| Insert (all match), 10K orders | ~15.7M ops/sec |
| Random flow, 10K orders | ~8M ops/sec |
| Cancel, 5K orders | ~13.5M ops/sec |

## Test Matrix

| Feature | Unit Tests | Property Tests |
|---------|-----------|---------------|
| Book side ordering (bid/ask) | 2 | - |
| FIFO within price level | 1 | - |
| Insert/remove/depth | 3 | - |
| Limit order matching | 4 | random_limit_orders_maintain_invariants |
| Market order matching | 3 | - |
| Price-time priority | 2 | - |
| Multi-level sweep | 1 | - |
| IOC | 2 | - |
| FOK | 2 | - |
| Cancel | 3 | random_orders_and_cancels |
| Replace | 4 | - |
| Validation (reject) | 3 | - |
| BBO events | 2 | - |
| Depth events | 1 | - |
| WAL write/read | 1 | - |
| Snapshot/restore | 1 | - |
| Deterministic replay | 1 | deterministic_replay |
| Recovery (snap+WAL) | 1 | - |
| Gateway flow | 2 | - |
| **Total** | **42+** | **3** |

## Quick Start

```bash
# Run all tests
cargo test --workspace

# Run simulation
cargo run -p engine-sim

# Run benchmarks
cargo bench --bench matching

# Check with clippy
cargo clippy --workspace
```

## Project Constraints

- **Rust stable only** - no nightly features
- **No `unsafe`** - zero unsafe blocks
- **No floating-point** - integer ticks and lots throughout
- **Deterministic** - explicit `seq_no` sequencing, no wall-clock dependencies
- **Single-threaded core** - one engine per symbol partition
- **Clean APIs** - well-documented public interfaces with serde support
