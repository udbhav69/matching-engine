//! Simulation: submits a mixed order flow and prints fills, BBO, and depth.
//!
//! Usage: `cargo run -p engine-sim`

use engine_core::matcher::MatchingEngine;
use engine_feed::MarketDataTracker;
use engine_types::*;
use rand::Rng;

fn main() {
    println!("=== Matching Engine Simulation ===\n");

    let mut engine = MatchingEngine::new(1);
    let mut md = MarketDataTracker::new(1);
    let mut rng = rand::thread_rng();
    let mut seq: SeqNo = 0;

    // Phase 1: Build the book with resting orders.
    println!("--- Phase 1: Building initial book ---");
    let initial_orders = vec![
        // Bids
        (Side::Buy, 99, 50),
        (Side::Buy, 98, 100),
        (Side::Buy, 97, 200),
        // Asks
        (Side::Sell, 101, 50),
        (Side::Sell, 102, 100),
        (Side::Sell, 103, 200),
    ];

    for (side, price, qty) in initial_orders {
        seq += 1;
        let cmd = Command {
            seq_no: seq,
            timestamp: seq * 1_000_000,
            payload: CommandPayload::New(NewOrder {
                order_id: seq,
                symbol: 1,
                side,
                order_type: OrderType::Limit,
                tif: TimeInForce::GTC,
                condition: OrderCondition::None,
                price,
                qty,
                stop_price: 0,
            }),
        };
        let result = engine.process(&cmd);
        print_events(seq, &result.events, &mut md);
    }

    print_book_summary(&engine);

    // Phase 2: Mixed aggressive flow.
    println!("\n--- Phase 2: Mixed aggressive flow (20 orders) ---");
    for _ in 0..20 {
        seq += 1;
        let side = if rng.gen_bool(0.5) { Side::Buy } else { Side::Sell };
        let price = rng.gen_range(96..=104);
        let qty = rng.gen_range(5..=30);

        // Randomly choose order type.
        let (order_type, tif, condition) = match rng.gen_range(0..5) {
            0 => (OrderType::Market, TimeInForce::GTC, OrderCondition::None),
            1 => (OrderType::Limit, TimeInForce::IOC, OrderCondition::None),
            2 => (OrderType::Limit, TimeInForce::FOK, OrderCondition::None),
            _ => (OrderType::Limit, TimeInForce::GTC, OrderCondition::None),
        };

        let effective_price = if order_type == OrderType::Market { MARKET_PRICE } else { price };

        let cmd = Command {
            seq_no: seq,
            timestamp: seq * 1_000_000,
            payload: CommandPayload::New(NewOrder {
                order_id: seq,
                symbol: 1,
                side,
                order_type,
                tif,
                condition,
                price: effective_price,
                qty,
                stop_price: 0,
            }),
        };

        println!(
            "\n>> Submit: seq={} {} {} {:?} {:?} price={} qty={}",
            seq,
            side,
            if order_type == OrderType::Market { "MKT" } else { "LMT" },
            tif,
            condition,
            effective_price,
            qty,
        );

        let result = engine.process(&cmd);
        print_events(seq, &result.events, &mut md);
    }

    // Phase 3: Cancel some resting orders.
    println!("\n--- Phase 3: Cancel remaining orders ---");
    let open_ids: Vec<OrderId> = engine
        .book
        .orders
        .values()
        .filter(|o| o.is_open)
        .map(|o| o.order_id)
        .collect();

    for &oid in open_ids.iter().take(3) {
        seq += 1;
        let cmd = Command {
            seq_no: seq,
            timestamp: seq * 1_000_000,
            payload: CommandPayload::Cancel(CancelOrder {
                order_id: oid,
                symbol: 1,
            }),
        };
        println!("\n>> Cancel: order_id={}", oid);
        let result = engine.process(&cmd);
        print_events(seq, &result.events, &mut md);
    }

    print_book_summary(&engine);

    // Final statistics.
    println!("\n=== Final Statistics ===");
    let total_orders = engine.book.orders.len();
    let open_orders = engine.book.orders.values().filter(|o| o.is_open).count();
    let filled_orders = engine
        .book
        .orders
        .values()
        .filter(|o| !o.is_open && o.filled_qty > 0)
        .count();
    println!("Total orders processed: {}", total_orders);
    println!("Currently open: {}", open_orders);
    println!("Filled (partial or full): {}", filled_orders);
    println!("Last seq_no: {}", engine.book.last_seq_no);
    println!("Book uncrossed: {}", engine.book.is_uncrossed());
}

fn print_events(_seq_no: SeqNo, events: &[EngineEvent], md: &mut MarketDataTracker) {
    for event in events {
        match event {
            EngineEvent::OrderAccepted { order_id, .. } => {
                println!("  [ACCEPTED] order_id={}", order_id);
            }
            EngineEvent::OrderRejected { order_id, reason, .. } => {
                println!("  [REJECTED] order_id={} reason={}", order_id, reason);
            }
            EngineEvent::Trade(fill) => {
                println!(
                    "  [TRADE] maker={} taker={} price={} qty={} maker_done={} taker_done={}",
                    fill.maker_order_id,
                    fill.taker_order_id,
                    fill.price,
                    fill.qty,
                    fill.maker_filled_completely,
                    fill.taker_filled_completely,
                );
            }
            EngineEvent::OrderCanceled { order_id, remaining_qty, .. } => {
                println!("  [CANCELED] order_id={} remaining={}", order_id, remaining_qty);
            }
            EngineEvent::CancelRejected { order_id, reason, .. } => {
                println!("  [CANCEL_REJECTED] order_id={} reason={}", order_id, reason);
            }
            EngineEvent::OrderReplaced { order_id, new_price, new_qty, .. } => {
                println!("  [REPLACED] order_id={} new_price={} new_qty={}", order_id, new_price, new_qty);
            }
            EngineEvent::ReplaceRejected { order_id, reason, .. } => {
                println!("  [REPLACE_REJECTED] order_id={} reason={}", order_id, reason);
            }
            EngineEvent::BboChanged(bbo) => {
                if let Some(_new_event) = md.update_bbo(*bbo) {
                    println!(
                        "  [BBO] bid={}@{} ask={}@{}",
                        bbo.bid_qty,
                        bbo.bid_price.map_or("--".to_string(), |p| p.to_string()),
                        bbo.ask_qty,
                        bbo.ask_price.map_or("--".to_string(), |p| p.to_string()),
                    );
                }
            }
            EngineEvent::DepthChanged { side, levels } => {
                println!("  [DEPTH:{}]", side);
                for lvl in levels {
                    println!("    price={} qty={} orders={}", lvl.price, lvl.qty, lvl.order_count);
                }
            }
        }
    }
}

fn print_book_summary(engine: &MatchingEngine) {
    println!("\n--- Book Summary ---");
    let bbo = engine.book.bbo();
    println!(
        "BBO: bid={}@{} | ask={}@{}",
        bbo.bid_qty,
        bbo.bid_price.map_or("--".to_string(), |p| p.to_string()),
        bbo.ask_qty,
        bbo.ask_price.map_or("--".to_string(), |p| p.to_string()),
    );
    println!("Bid depth ({} levels):", engine.book.bids.level_count());
    for lvl in engine.book.bids.depth(5) {
        println!("  {} x {} ({} orders)", lvl.price, lvl.qty, lvl.order_count);
    }
    println!("Ask depth ({} levels):", engine.book.asks.level_count());
    for lvl in engine.book.asks.depth(5) {
        println!("  {} x {} ({} orders)", lvl.price, lvl.qty, lvl.order_count);
    }
    println!("Uncrossed: {}", engine.book.is_uncrossed());
}
