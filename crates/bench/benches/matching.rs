//! Matching engine benchmarks.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use engine_core::matcher::MatchingEngine;
use engine_types::*;
use rand::Rng;

fn make_limit_order(id: OrderId, side: Side, price: Price, qty: Qty) -> Command {
    Command {
        seq_no: id,
        timestamp: id * 1000,
        payload: CommandPayload::New(NewOrder {
            order_id: id,
            symbol: 1,
            side,
            order_type: OrderType::Limit,
            tif: TimeInForce::GTC,
            condition: OrderCondition::None,
            price,
            qty,
            stop_price: 0,
        }),
    }
}

/// Benchmark: insert limit orders (no crossing).
fn bench_insert_no_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_no_match");
    for &n in &[1000, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut engine = MatchingEngine::new(1);
                for i in 1..=n {
                    let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                    let price = if side == Side::Buy { 100 } else { 200 };
                    let cmd = make_limit_order(i, side, price, 10);
                    black_box(engine.process(&cmd));
                }
            });
        });
    }
    group.finish();
}

/// Benchmark: insert crossing orders (every order matches).
fn bench_insert_all_match(c: &mut Criterion) {
    c.bench_function("insert_all_match_10k", |b| {
        b.iter(|| {
            let mut engine = MatchingEngine::new(1);
            for i in 1..=10_000u64 {
                let (side, price) = if i % 2 == 1 {
                    (Side::Sell, 100)
                } else {
                    (Side::Buy, 100)
                };
                let cmd = make_limit_order(i, side, price, 10);
                black_box(engine.process(&cmd));
            }
        });
    });
}

/// Benchmark: random order flow (mix of buys and sells at varying prices).
fn bench_random_flow(c: &mut Criterion) {
    c.bench_function("random_flow_10k", |b| {
        b.iter(|| {
            let mut engine = MatchingEngine::new(1);
            let mut rng = rand::thread_rng();
            for i in 1..=10_000u64 {
                let side = if rng.gen_bool(0.5) { Side::Buy } else { Side::Sell };
                let price = rng.gen_range(95..=105);
                let cmd = make_limit_order(i, side, price, rng.gen_range(1..=20));
                black_box(engine.process(&cmd));
            }
        });
    });
}

/// Benchmark: cancel flow (insert then cancel).
fn bench_cancel(c: &mut Criterion) {
    c.bench_function("cancel_5k", |b| {
        b.iter(|| {
            let mut engine = MatchingEngine::new(1);
            // Insert 5000 orders.
            for i in 1..=5000u64 {
                let cmd = make_limit_order(i, Side::Buy, 100, 10);
                engine.process(&cmd);
            }
            // Cancel all.
            for i in 1..=5000u64 {
                let cmd = Command {
                    seq_no: 5000 + i,
                    timestamp: (5000 + i) * 1000,
                    payload: CommandPayload::Cancel(CancelOrder {
                        order_id: i,
                        symbol: 1,
                    }),
                };
                black_box(engine.process(&cmd));
            }
        });
    });
}

criterion_group!(
    benches,
    bench_insert_no_match,
    bench_insert_all_match,
    bench_random_flow,
    bench_cancel,
);
criterion_main!(benches);
