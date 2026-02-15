# Matching Engine Architecture

## System Diagram

```
                          ┌─────────────────────────────────────────────────┐
                          │              INBOUND COMMANDS                   │
                          │  NewOrder / CancelOrder / ReplaceOrder          │
                          └──────────────────────┬──────────────────────────┘
                                                 │
                                                 ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                          GATEWAY (engine-gateway)                              │
│                                                                                │
│  ┌──────────────┐   ┌──────────────────┐   ┌──────────────────────────────┐   │
│  │  Backpressure │   │  Seq# Assignment │   │  WAL Append (before match)  │   │
│  │  Queue Check  │──▶│  next_seq_no++   │──▶│  wal.append(&cmd)           │   │
│  └──────────────┘   └──────────────────┘   └──────────┬───────────────────┘   │
│                                                        │                       │
│                                                        ▼                       │
│                                            ┌──────────────────────┐            │
│                                            │  Periodic Snapshot   │            │
│                                            │  (every N commands)  │            │
│                                            └──────────────────────┘            │
└────────────────────────────────────────────────┬───────────────────────────────┘
                                                 │
                                                 ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                      MATCHING ENGINE (engine-core)                             │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                        MatchingEngine::process()                         │  │
│  │                                                                          │  │
│  │   ┌─────────────────┐  ┌──────────────────┐  ┌────────────────────┐     │  │
│  │   │  process_new()  │  │ process_cancel() │  │ process_replace()  │     │  │
│  │   │                 │  │                  │  │                    │     │  │
│  │   │  • Validate     │  │  • Find order    │  │  • Validate        │     │  │
│  │   │  • FOK/AON pre- │  │  • Remove from   │  │  • Remove old      │     │  │
│  │   │    check        │  │    book side     │  │  • Insert at new   │     │  │
│  │   │  • match_order()│  │  • Mark closed   │  │    price           │     │  │
│  │   │  • Rest or      │  │  • Emit Canceled │  │  • Re-match if     │     │  │
│  │   │    cancel       │  │    or Rejected   │  │    crossing        │     │  │
│  │   │    remainder    │  │                  │  │  • Emit Replaced   │     │  │
│  │   └────────┬────────┘  └──────────────────┘  │    or Rejected    │     │  │
│  │            │                                  └────────────────────┘     │  │
│  │            ▼                                                             │  │
│  │   ┌──────────────────────────────────────────────────────────────┐       │  │
│  │   │                    match_order() Loop                        │       │  │
│  │   │                                                              │       │  │
│  │   │   while aggressor.open_qty > 0:                              │       │  │
│  │   │     1. Peek best price on opposite side                      │       │  │
│  │   │     2. Check price compatibility (limit orders)              │       │  │
│  │   │     3. Pop front maker from best level (FIFO)                │       │  │
│  │   │     4. fill_qty = min(aggressor.open, maker.open)            │       │  │
│  │   │     5. Update maker state (filled_qty += fill_qty)           │       │  │
│  │   │     6. Update book side aggregate qty                        │       │  │
│  │   │     7. Re-insert partial maker at front of level             │       │  │
│  │   │     8. Update aggressor state                                │       │  │
│  │   │     9. Emit Trade event                                      │       │  │
│  │   └──────────────────────────────────────────────────────────────┘       │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                         ORDER BOOK (book.rs)                             │  │
│  │                                                                          │  │
│  │    BIDS (sorted best-first)          ASKS (sorted best-first)            │  │
│  │   ┌──────────────────────┐         ┌──────────────────────┐              │  │
│  │   │  Price 102 (best bid)│         │  Price 103 (best ask)│              │  │
│  │   │  ┌──┬──┬──┐         │         │  ┌──┬──┐             │              │  │
│  │   │  │O1│O4│O7│  qty=30 │         │  │O2│O5│    qty=20   │              │  │
│  │   │  └──┴──┴──┘  FIFO→  │         │  └──┴──┘    FIFO→    │              │  │
│  │   ├──────────────────────┤         ├──────────────────────┤              │  │
│  │   │  Price 101           │         │  Price 104           │              │  │
│  │   │  ┌──┬──┐             │         │  ┌──┐                │              │  │
│  │   │  │O3│O6│    qty=25   │         │  │O8│       qty=15   │              │  │
│  │   │  └──┴──┘    FIFO→    │         │  └──┘       FIFO→    │              │  │
│  │   ├──────────────────────┤         ├──────────────────────┤              │  │
│  │   │  Price 100           │         │  Price 105           │              │  │
│  │   │  ┌──┐                │         │  ┌──┬──┬──┐          │              │  │
│  │   │  │O9│       qty=10   │         │  │OA│OB│OC│ qty=35   │              │  │
│  │   │  └──┘       FIFO→    │         │  └──┴──┴──┘ FIFO→    │              │  │
│  │   └──────────────────────┘         └──────────────────────┘              │  │
│  │                                                                          │  │
│  │   BTreeMap<u64, PriceLevel>        BTreeMap<u64, PriceLevel>             │  │
│  │   key = MAX - price (desc)         key = price (asc)                     │  │
│  │                                                                          │  │
│  │   ┌──────────────────────────────────────────────────────────┐           │  │
│  │   │  orders: HashMap<OrderId, OrderState>                    │           │  │
│  │   │  Flat lookup for all orders (open + closed)              │           │  │
│  │   └──────────────────────────────────────────────────────────┘           │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────┬───────────────────────────────┘
                                                 │
                                                 ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                          OUTBOUND EVENTS                                       │
│                                                                                │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────┐ ┌───────────────────────┐  │
│  │ OrderAccepted│ │ OrderRejected│ │    Trade     │ │    OrderCanceled      │  │
│  │              │ │  {reason}    │ │ {maker,taker │ │  {remaining_qty}      │  │
│  │              │ │              │ │  price, qty} │ │                       │  │
│  └──────────────┘ └──────────────┘ └─────────────┘ └───────────────────────┘  │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────┐ ┌───────────────────────┐  │
│  │CancelRejected│ │OrderReplaced │ │ ReplaceRej. │ │    BboChanged         │  │
│  │  {reason}    │ │{new_price,   │ │  {reason}   │ │ {bid_p, bid_q,        │  │
│  │              │ │ new_qty}     │ │             │ │  ask_p, ask_q}        │  │
│  └──────────────┘ └──────────────┘ └─────────────┘ └───────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
                                                 │
                              ┌──────────────────┼──────────────────┐
                              ▼                  ▼                  ▼
                    ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐
                    │  engine-feed │   │ engine-replay │   │    Consumers     │
                    │              │   │               │   │   (sim, bench,   │
                    │ BBO tracking │   │  WAL + Snap   │   │    your app)     │
                    │ Depth change │   │  Recovery     │   │                  │
                    │  detection   │   │  Replay       │   │                  │
                    └──────────────┘   └──────────────┘   └──────────────────┘
```

## Data Flow

```
Command ──▶ Gateway ──▶ WAL ──▶ MatchingEngine ──▶ Events
               │                      │
               │                 ┌────┴────┐
               │                 ▼         ▼
               │            OrderBook    OrderState
               │           (BTreeMap)   (HashMap)
               │
               └──▶ Snapshot (periodic)
                        │
                        ▼
              ┌──────────────────┐
              │   Recovery Path  │
              │                  │
              │  Snapshot ──▶ Engine (restored state)
              │     +                    │
              │  WAL tail ──▶ replay ────┘
              │                    │
              │              identical events
              └──────────────────┘
```

## Order Type Decision Tree

```
                         ┌──────────────┐
                         │  New Order   │
                         └──────┬───────┘
                                │
                    ┌───────────┴───────────┐
                    │    Validate           │
                    │  • qty > 0?           │
                    │  • unique order_id?   │
                    │  • limit price > 0?   │
                    └───────────┬───────────┘
                                │ pass
                    ┌───────────┴───────────┐
                    │   OrderAccepted       │
                    └───────────┬───────────┘
                                │
               ┌────────────────┼────────────────┐
               │                │                 │
         ┌─────▼─────┐   ┌─────▼──────┐   ┌─────▼──────┐
         │  Stop?     │   │   FOK?     │   │   AON?     │
         │  Rest as   │   │  Pre-check │   │  Pre-check │
         │  pending   │   │  full qty  │   │  full qty  │
         └────────────┘   │  available │   │  available │
                          └─────┬──────┘   └─────┬──────┘
                                │                 │
                                ▼                 ▼
                    ┌───────────────────────────────────┐
                    │         match_order()              │
                    │  Sweep opposite side price-by-     │
                    │  price, order-by-order (FIFO)      │
                    └───────────────┬───────────────────┘
                                    │
                       ┌────────────┴────────────┐
                       │    Remaining qty > 0?   │
                       └────────────┬────────────┘
                              yes   │
                 ┌──────────────────┼──────────────────┐
                 │                  │                    │
           ┌─────▼──────┐   ┌──────▼───────┐   ┌──────▼───────┐
           │   Market    │   │    IOC       │   │  GTC Limit   │
           │  Cancel     │   │  Cancel      │   │  Rest on     │
           │  remainder  │   │  remainder   │   │  book        │
           └─────────────┘   └──────────────┘   └──────────────┘
```

## Crate Dependency Graph

```
                    ┌────────────────┐
                    │  engine-types  │  ◀── Pure domain types, no deps
                    └───────┬────────┘
                            │
              ┌─────────────┼──────────────┐
              │             │              │
       ┌──────▼──────┐ ┌───▼────────┐ ┌───▼──────────┐
       │ engine-core │ │engine-feed │ │              │
       │ (book +     │ │(BBO/depth  │ │              │
       │  matcher)   │ │ tracking)  │ │              │
       └──────┬──────┘ └────────────┘ │              │
              │                        │              │
       ┌──────▼──────┐                │              │
       │engine-replay│                │              │
       │(WAL + snap) │                │              │
       └──────┬──────┘                │              │
              │                        │              │
       ┌──────▼──────────────────────▼──────────────▼─┐
       │            engine-gateway                      │
       │    (sequencing + backpressure + snapshot)       │
       └──────┬─────────────────────────────────────────┘
              │
     ┌────────┼────────┐
     │        │        │
 ┌───▼──┐ ┌──▼───┐ ┌──▼────┐
 │ sim  │ │bench │ │ your  │
 │      │ │      │ │ app   │
 └──────┘ └──────┘ └───────┘
```

---

# File-by-File Documentation

---

## `crates/engine-types/src/lib.rs`

The **foundation crate**: every other crate depends on this. Contains zero business logic -- only type definitions, serialization, and display formatting.

### Type Aliases

| Type | Underlying | Purpose |
|------|-----------|---------|
| `Price` | `u64` | Price in integer ticks. `0` = market (no limit). |
| `Qty` | `u64` | Quantity in integer lots. |
| `OrderId` | `u64` | Unique order identifier per partition. |
| `SeqNo` | `u64` | Monotonic sequence number stamped by the engine. |
| `Symbol` | `u32` | Symbol identifier (interned for cache-friendliness). |
| `Timestamp` | `u64` | Nanoseconds since epoch (audit only, never used for matching). |

### Enums

| Enum | Variants | Purpose |
|------|----------|---------|
| `Side` | `Buy`, `Sell` | Order side. Has `opposite()` method. |
| `TimeInForce` | `GTC`, `IOC`, `FOK` | How long the order lives. |
| `OrderType` | `Limit`, `Market` | Whether the order has a price limit. |
| `OrderCondition` | `None`, `AON`, `Stop` | Special execution conditions. |
| `CommandPayload` | `New(NewOrder)`, `Cancel(CancelOrder)`, `Replace(ReplaceOrder)` | Discriminated union of inbound commands. |
| `RejectReason` | `UnknownOrder`, `DuplicateOrderId`, `InvalidPrice`, `InvalidQuantity`, `FOKNotFillable`, `AONNotFillable`, `ReplaceQtyIncrease`, `NothingToCancel`, `NoLiquidity`, `UnknownSymbol`, `InternalError` | Why an order/action was rejected. |
| `EngineEvent` | `OrderAccepted`, `OrderRejected`, `OrderCanceled`, `CancelRejected`, `OrderReplaced`, `ReplaceRejected`, `Trade(Fill)`, `BboChanged(Bbo)`, `DepthChanged` | All possible events emitted by the engine. |

### Structs

| Struct | Key Fields | Purpose |
|--------|-----------|---------|
| `NewOrder` | `order_id`, `symbol`, `side`, `order_type`, `tif`, `condition`, `price`, `qty`, `stop_price` | Full specification of a new order. |
| `CancelOrder` | `order_id`, `symbol` | Request to cancel a resting order. |
| `ReplaceOrder` | `order_id`, `symbol`, `new_price`, `new_qty` | Request to amend price/reduce qty. |
| `Command` | `seq_no`, `timestamp`, `payload` | Sequenced envelope wrapping any command. |
| `Fill` | `maker_order_id`, `taker_order_id`, `price`, `qty`, `maker_filled_completely`, `taker_filled_completely` | One fill between two orders. |
| `Bbo` | `bid_price`, `bid_qty`, `ask_price`, `ask_qty` | Best bid/offer snapshot. |
| `DepthLevel` | `price`, `qty`, `order_count` | One row in the depth-of-book. |
| `OrderState` | `order_id`, `symbol`, `side`, `order_type`, `tif`, `condition`, `price`, `original_qty`, `filled_qty`, `stop_price`, `is_open` | Full lifecycle state of an order. |

### Key Methods

| Method | Description |
|--------|-------------|
| `Side::opposite()` | Returns `Buy` for `Sell` and vice versa. |
| `OrderState::open_qty()` | `original_qty - filled_qty` (saturating). |
| `OrderState::from_new_order()` | Construct initial state from a `NewOrder`. |

### Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `MARKET_PRICE` | `0` | Sentinel: "no price limit" for market orders. |
| `MAX_DEPTH_LEVELS` | `10` | Default depth levels tracked for market data. |

---

## `crates/engine-core/src/book.rs`

The **order book data structure**. Uses a `BTreeMap` per side for price-level ordering and a `VecDeque` per level for FIFO time priority.

### Structs

#### `PriceLevel`
```
Fields: orders (VecDeque<OrderId>), total_qty (Qty)
```
A single price level. Orders are queued FIFO. `total_qty` is the cached sum of all open quantities at this price.

#### `BookSide`
```
Fields: levels (BTreeMap<u64, PriceLevel>), side (SideTag)
```
One side of the book. The BTreeMap key encoding ensures `iter()` always yields best-to-worst:
- **Bids**: key = `u64::MAX - price` (highest price sorts first)
- **Asks**: key = `price` (lowest price sorts first)

#### `OrderBook`
```
Fields: symbol, bids (BookSide), asks (BookSide), orders (HashMap<OrderId, OrderState>), last_seq_no
```
The complete book for one symbol. The `orders` HashMap provides O(1) lookup by ID for cancel/replace operations.

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `BookSide::new_bids()` | `() -> Self` | Create bid side with inverted sort keys. |
| `BookSide::new_asks()` | `() -> Self` | Create ask side with natural sort keys. |
| `BookSide::key()` | `(price) -> u64` | Encode a price into the BTreeMap sort key. |
| `BookSide::price_from_key()` | `(key) -> Price` | Decode a sort key back to actual price. |
| `BookSide::insert()` | `(price, order_id, qty)` | Add order at back of price level (FIFO). |
| `BookSide::remove()` | `(price, order_id, qty) -> bool` | Remove specific order from a level. Cleans up empty levels. |
| `BookSide::reduce_qty()` | `(price, delta)` | Subtract from level's aggregate qty after a partial fill. |
| `BookSide::pop_front()` | `() -> Option<(Price, OrderId)>` | Pop the front order at the best price. Used during matching. |
| `BookSide::best()` | `() -> Option<(Price, &PriceLevel)>` | Peek at the best level without modification. |
| `BookSide::is_empty()` | `() -> bool` | True if no orders on this side. |
| `BookSide::level_count()` | `() -> usize` | Number of distinct price levels. |
| `BookSide::total_qty()` | `() -> Qty` | Sum of all quantities across all levels. |
| `BookSide::depth()` | `(n) -> Vec<DepthLevel>` | Top N levels as a snapshot. |
| `OrderBook::new()` | `(symbol) -> Self` | Create empty book. |
| `OrderBook::side_mut()` | `(Side) -> &mut BookSide` | Get the book side matching the given order side. |
| `OrderBook::opposite_side_mut()` | `(Side) -> &mut BookSide` | Get the opposite side (where resting orders are matched). |
| `OrderBook::opposite_side()` | `(Side) -> &BookSide` | Read-only version of above. |
| `OrderBook::bbo()` | `() -> Bbo` | Current best bid/offer snapshot. |
| `OrderBook::is_uncrossed()` | `() -> bool` | Invariant check: best bid < best ask. |

---

## `crates/engine-core/src/matcher.rs`

The **matching engine**: the single-threaded core loop that processes commands against the order book and emits events.

### Structs

#### `MatchResult`
```
Fields: events (Vec<EngineEvent>)
```
The output of processing one command.

#### `MatchingEngine`
```
Fields: book (OrderBook)
```
The engine for a single symbol partition.

### Public Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `MatchingEngine::new()` | `(symbol) -> Self` | Create a new engine with an empty book. |
| `MatchingEngine::process()` | `(&mut self, &Command) -> MatchResult` | **Core entry point.** Dispatches to new/cancel/replace handlers. Always emits a BboChanged event at the end. |

### Private Methods

| Method | Description |
|--------|-------------|
| `process_new_order()` | **Validation** (zero qty, zero price on limit, duplicate ID) -> **Accept** -> **Stop handling** (rest as pending) -> **FOK pre-check** (reject if insufficient liquidity) -> **AON pre-check** (reject or rest) -> **match_order()** -> **Post-match**: market orders cancel remainder, IOC cancels remainder, GTC limits rest on book. |
| `match_order()` | **The matching loop.** Iterates: peek best opposite price -> check price compatibility -> pop front maker -> compute fill_qty = min(aggressor open, maker open) -> update maker state -> update book side qty -> re-insert partial maker at front -> emit Trade. Stops when aggressor is fully filled or no compatible liquidity. |
| `available_qty_at_or_better()` | Scans opposite side levels to sum available qty at or better than the given price. Used by FOK and AON to pre-check before matching. |
| `process_cancel()` | Looks up order -> verifies it's open -> removes from book side -> marks closed -> emits OrderCanceled. Rejects with UnknownOrder or NothingToCancel. |
| `process_replace()` | Looks up order -> validates (no qty increase, valid price, must leave open qty) -> removes from book -> updates price/qty -> re-inserts at new price -> if the new price crosses the opposite side, runs match_order() to fill. Emits OrderReplaced or ReplaceRejected. |

### Property Tests (proptests module)

| Test | What it verifies |
|------|-----------------|
| `random_limit_orders_maintain_invariants` | Random sequences of 1-200 limit orders always leave the book in a valid state (uncrossed, no zero-qty open orders, correct aggregate qtys, correct side assignments). |
| `random_orders_and_cancels` | Insert 10-100 orders then randomly cancel: invariants hold after every operation. |
| `deterministic_replay` | Two runs of the same command sequence produce identical event streams. |

---

## `crates/engine-feed/src/lib.rs`

**Market data change detection.** Sits downstream of the engine and filters events to only emit when something actually changed.

### Struct: `MarketDataTracker`

```
Fields: symbol, last_bbo (Option<Bbo>), last_bid_depth (Vec<DepthLevel>), last_ask_depth (Vec<DepthLevel>)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `new()` | `(symbol) -> Self` | Create tracker with no prior state. |
| `update_bbo()` | `(Bbo) -> Option<EngineEvent>` | Returns `Some(BboChanged)` only if the BBO actually changed from last known. |
| `update_depth()` | `(bid_levels, ask_levels) -> Vec<EngineEvent>` | Returns `DepthChanged` events for each side that actually changed. |
| `last_bbo()` | `() -> Option<&Bbo>` | Peek at last known BBO. |

---

## `crates/engine-replay/src/lib.rs`

**Durability and recovery.** Provides write-ahead logging, snapshotting, and deterministic replay.

### Enum: `ReplayError`

| Variant | Meaning |
|---------|---------|
| `Io(io::Error)` | File system error. |
| `Json(serde_json::Error)` | Serialization/deserialization error. |
| `Bincode(String)` | Binary serialization error. |
| `SequenceGap { expected, got }` | Gap in sequence numbers during replay. |

### Struct: `WalWriter`

```
Fields: writer (BufWriter<File>), path (PathBuf), count (u64)
```

| Method | Description |
|--------|-------------|
| `open(path)` | Open or create a WAL file for appending. |
| `append(&cmd)` | Serialize command as JSON, write newline, flush. Must be called **before** processing for crash safety. |
| `path()` | Get the WAL file path. |
| `count()` | Number of commands appended this session. |

### Struct: `WalReader`

Implements `Iterator<Item = Result<Command, ReplayError>>`. Reads newline-delimited JSON entries.

| Method | Description |
|--------|-------------|
| `open(path)` | Open a WAL file for reading. |
| `next()` | Yield the next command or EOF. |

### Free Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `write_snapshot()` | `(&MatchingEngine, path)` | Serialize the full OrderBook to JSON. |
| `load_snapshot()` | `(path) -> MatchingEngine` | Deserialize an OrderBook from a snapshot file. |
| `replay_wal()` | `(&mut engine, wal_path) -> Vec<EngineEvent>` | Replay all WAL entries with `seq_no > engine.book.last_seq_no`. Returns all events produced. |
| `recover()` | `(snap_path, wal_path, symbol) -> (MatchingEngine, Vec<EngineEvent>)` | Full recovery: load snapshot (if exists), then replay WAL tail. |

### Recovery Protocol

```
1. If snapshot exists:
     engine = load_snapshot(snapshot_path)
   Else:
     engine = MatchingEngine::new(symbol)

2. For each WAL entry where seq_no > engine.book.last_seq_no:
     events.extend(engine.process(&cmd))

3. Result: engine state + event stream identical to original run
```

---

## `crates/engine-gateway/src/lib.rs`

**Synchronous entry point** for a single symbol partition. Handles sequence numbering, WAL writes, periodic snapshots, and backpressure.

### Enum: `GatewayError`

| Variant | Meaning |
|---------|---------|
| `QueueFull { limit }` | Backpressure: too many pending commands. |
| `Wal(ReplayError)` | WAL I/O error. |
| `SymbolMismatch { expected, got }` | Command targets wrong symbol. |

### Struct: `GatewayConfig`

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | `Symbol` | Which symbol this gateway handles. |
| `wal_path` | `PathBuf` | Path to the WAL file. |
| `snapshot_path` | `PathBuf` | Path to the snapshot file. |
| `max_queue_depth` | `usize` | Backpressure limit. |
| `snapshot_interval` | `u64` | Take a snapshot every N commands. |

### Struct: `Gateway`

| Method | Signature | Description |
|--------|-----------|-------------|
| `new(config)` | `-> Result<Self>` | Create a fresh gateway (empty book). |
| `recover(config)` | `-> Result<(Self, Vec<EngineEvent>)>` | Recover from snapshot + WAL, returning replayed events. |
| `submit_new(NewOrder)` | `-> Result<Vec<EngineEvent>>` | Submit a new order. Assigns seq_no, writes WAL, processes, maybe snapshots. |
| `submit_cancel(CancelOrder)` | `-> Result<Vec<EngineEvent>>` | Submit a cancel. Same pipeline. |
| `submit_replace(ReplaceOrder)` | `-> Result<Vec<EngineEvent>>` | Submit a replace. Same pipeline. |
| `engine()` | `-> &MatchingEngine` | Read-only access to the engine. |
| `next_seq_no()` | `-> SeqNo` | The next sequence number that will be assigned. |

### Internal Pipeline (`process_command`)

```
1. Check backpressure (queue depth < max)
2. WAL append (crash safety: persist BEFORE processing)
3. Increment next_seq_no
4. engine.process(&cmd) -> events
5. If commands_since_snapshot >= snapshot_interval:
     write_snapshot()
     reset counter
6. Return events
```

---

## `crates/bench/benches/matching.rs`

**Criterion benchmarks** measuring throughput and latency of the matching engine.

| Benchmark | What it measures | Parameters |
|-----------|-----------------|------------|
| `bench_insert_no_match` | Insert limit orders that don't cross (pure book-building). | 1K, 10K, 100K orders |
| `bench_insert_all_match` | Insert alternating buy/sell at same price (every order trades). | 10K orders |
| `bench_random_flow` | Random side, random price (95-105), random qty (1-20). | 10K orders |
| `bench_cancel` | Insert 5K orders then cancel all of them. | 5K + 5K operations |

### Results (Apple Silicon)

| Benchmark | Time | Throughput |
|-----------|------|-----------|
| insert_no_match/1K | ~48 us | ~20.8M ops/sec |
| insert_no_match/10K | ~438 us | ~22.8M ops/sec |
| insert_no_match/100K | ~4.45 ms | ~22.5M ops/sec |
| insert_all_match/10K | ~637 us | ~15.7M ops/sec |
| random_flow/10K | ~1.25 ms | ~8.0M ops/sec |
| cancel/5K+5K | ~371 us | ~27.0M ops/sec |

---

## `crates/sim/src/main.rs`

**Simulation binary** that exercises the engine with realistic mixed order flow.

### `main()`

Runs three phases:

| Phase | Description |
|-------|-------------|
| **Phase 1: Build book** | Places 3 bid levels (97-99) and 3 ask levels (101-103) with 50-200 qty each. Prints initial book state. |
| **Phase 2: Aggressive flow** | Submits 20 random orders: mix of Limit/Market, GTC/IOC/FOK, random prices (96-104), random qty (5-30). Prints every event. |
| **Phase 3: Cancel** | Cancels up to 3 remaining open orders. Prints final book state and statistics. |

### Helper Functions

| Function | Description |
|----------|-------------|
| `print_events()` | Pretty-prints all engine events with `[ACCEPTED]`, `[TRADE]`, `[BBO]`, etc. tags. |
| `print_book_summary()` | Prints BBO, top 5 bid/ask depth levels, and uncrossed status. |

---

## Engine Invariants (verified by property tests)

| # | Invariant | Where checked |
|---|-----------|---------------|
| 1 | Book is never crossed: `best_bid < best_ask` | `check_invariants()` in proptests |
| 2 | No open order has `open_qty() == 0` | `check_invariants()` in proptests |
| 3 | `filled_qty <= original_qty` for every order | `check_invariants()` in proptests |
| 4 | Orders on bid side have `Side::Buy`; asks have `Side::Sell` | `check_invariants()` in proptests |
| 5 | Each level's `total_qty` equals sum of its orders' open quantities | `check_invariants()` in proptests |
| 6 | Same command sequence produces identical event stream (determinism) | `deterministic_replay` proptest |

---

## Test Matrix

| Crate | Test | What it covers |
|-------|------|---------------|
| engine-types | `side_opposite` | `Side::opposite()` correctness |
| engine-types | `order_state_open_qty` | `open_qty()` calculation |
| engine-types | `order_state_open_qty_saturates` | No panic on over-fill |
| engine-types | `command_serialization_roundtrip` | JSON serde round-trip |
| engine-core | `bid_side_ordering` | Best bid = highest price |
| engine-core | `ask_side_ordering` | Best ask = lowest price |
| engine-core | `fifo_within_price_level` | Insertion order preserved |
| engine-core | `remove_order` | Single order removal |
| engine-core | `remove_last_at_level_cleans_up` | Empty level auto-deleted |
| engine-core | `depth_snapshot` | Top-N depth extraction |
| engine-core | `book_uncrossed_invariant` | Crossed detection |
| engine-core | `limit_buy_rests_on_empty_book` | Non-crossing limit rests |
| engine-core | `limit_sell_rests_on_empty_book` | Non-crossing limit rests |
| engine-core | `limit_buy_matches_resting_sell` | Basic crossing match |
| engine-core | `partial_fill_leaves_maker_on_book` | Partial fill handling |
| engine-core | `price_time_priority` | Better price matched first |
| engine-core | `fifo_priority_within_price` | Earlier order matched first |
| engine-core | `market_buy_fills_at_best_ask` | Market order execution |
| engine-core | `market_order_no_liquidity_rejected` | Empty book rejection |
| engine-core | `market_order_partial_fill_canceled` | Partial market fill |
| engine-core | `reject_zero_qty` | Zero quantity rejected |
| engine-core | `reject_duplicate_order_id` | Duplicate ID rejected |
| engine-core | `reject_limit_with_zero_price` | Zero price limit rejected |
| engine-core | `cancel_resting_order` | Successful cancel |
| engine-core | `cancel_unknown_order` | Unknown order rejection |
| engine-core | `cancel_already_filled_order` | Filled order rejection |
| engine-core | `replace_reduce_qty` | Quantity reduction |
| engine-core | `replace_increase_qty_rejected` | Qty increase blocked |
| engine-core | `replace_change_price` | Price amendment |
| engine-core | `replace_crosses_book_fills` | Replace triggers matching |
| engine-core | `bbo_emitted_after_every_command` | BBO event emission |
| engine-core | `book_never_crossed_after_matching` | Post-match invariant |
| engine-core | `ioc_fills_partial_cancels_rest` | IOC partial fill + cancel |
| engine-core | `ioc_no_match_cancels_all` | IOC with no match |
| engine-core | `fok_fills_entirely_or_rejects` | FOK full fill |
| engine-core | `fok_insufficient_liquidity_rejected` | FOK rejection |
| engine-core | `aggressive_buy_sweeps_multiple_price_levels` | Multi-level sweep |
| engine-core | `filled_plus_open_plus_canceled_equals_original` | Accounting consistency |
| engine-core | `random_limit_orders_maintain_invariants` | **Property test**: random orders |
| engine-core | `random_orders_and_cancels` | **Property test**: random cancels |
| engine-core | `deterministic_replay` | **Property test**: replay determinism |
| engine-feed | `bbo_change_detected` | BBO dedup |
| engine-feed | `depth_change_detected` | Depth dedup |
| engine-replay | `wal_write_and_read_back` | WAL round-trip |
| engine-replay | `snapshot_and_restore` | Snapshot round-trip |
| engine-replay | `deterministic_replay_matches_original` | WAL replay == original |
| engine-replay | `recovery_from_snapshot_plus_wal` | Full recovery path |
| engine-gateway | `gateway_basic_flow` | Submit + seq_no increment |
| engine-gateway | `gateway_recovery` | Gateway recovery path |
