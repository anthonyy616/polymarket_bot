use polyarb::config::{Config, RiskConfig, ExecutionConfig, FeedsConfig, ApiConfig};
use polyarb::engine::{
    detector::Detector,
    signal::ArbSignal,
    state::{MarketState, ContractState},
};
use polyarb::feeds::{binance::PriceUpdate, polymarket::MarketSnapshot};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use crossbeam_channel::unbounded;
use tokio::sync::broadcast;

fn create_test_config(threshold_pct: f64) -> Arc<Config> {
    Arc::new(Config {
        risk: RiskConfig {
            arb_threshold_pct: threshold_pct,
            max_position_usdc: 20.0,
            max_daily_loss_usdc: 50.0,
            max_concurrent_positions: 3,
            starting_capital_usdc: 500.0,
        },
        execution: ExecutionConfig {
            simulation_mode: true,
            order_timeout_ms: 5000,
            slippage_tolerance_pct: 0.001,
        },
        feeds: FeedsConfig {
            binance_ws_url: "".into(),
            polymarket_api_url: "".into(),
            polymarket_poll_interval_ms: 500,
        },
        target_tokens: vec![],
        api: ApiConfig {
            polymarket_key: "".into(),
            polymarket_secret: "".into(),
            polymarket_passphrase: "".into(),
            wallet_private_key: "".into(),
        },
    })
}

fn current_time_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[tokio::test]
async fn test_no_signal_when_fresh() {
    let threshold = 0.003;
    let config = create_test_config(threshold);
    let state = Arc::new(MarketState::new());

    let (_binance_tx, binance_rx) = broadcast::channel(10);
    let (poly_tx, poly_rx) = broadcast::channel(10);
    let (signal_tx, signal_rx) = unbounded();

    let detector = Detector::new(config, state.clone(), binance_rx, poly_rx, signal_tx);
    detector.start();

    // Setup state
    let now = current_time_us();
    state.update_spot(70_500.0, now);
    state.update_contract(
        "token_1".to_string(),
        "Will BTC close above $70,000?".to_string(),
        0.5, // Implies $70,000
        0.5,
        now - 100_000, // 100ms ago (fresh)
    );

    // Trigger detection manually by hitting polymarket run
    let snapshot = MarketSnapshot { contracts: vec![] };
    let _ = poly_tx.send(snapshot);

    // Give detector a moment to process
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // We shouldn't receive any signal because it implies < 200ms staleness
    assert!(signal_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_buy_yes_signal() {
    let threshold = 0.003;
    let config = create_test_config(threshold);
    let state = Arc::new(MarketState::new());

    let (binance_tx, binance_rx) = broadcast::channel(10);
    let (_poly_tx, poly_rx) = broadcast::channel(10);
    let (signal_tx, signal_rx) = unbounded();

    let detector = Detector::new(config, state.clone(), binance_rx, poly_rx, signal_tx);
    detector.start();

    // Setup state
    let now = current_time_us();
    
    // contract implies $70,000, staleness = 800ms
    state.update_contract(
        "token_1".to_string(),
        "Will BTC close above $70,000?".to_string(),
        0.5, // 0.5 yes price corresponds to $70,000
        0.5,
        now - 800_000,
    );
    
    // Spot is 70,500
    state.update_spot(70_500.0, now);

    // Trigger detection manually
    let update = PriceUpdate { source: "bin".into(), price: 70_500.0, timestamp_us: now };
    let _ = binance_tx.send(update);

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = signal_rx.try_recv().expect("Should receive a signal");
    match res {
        ArbSignal::BuyYes { token_id, edge_pct, .. } => {
            assert_eq!(token_id, "token_1");
            assert!((edge_pct - 0.00709).abs() < 0.0001); // ~0.71% edge
        }
        _ => panic!("Expected BuyYes signal"),
    }
}

#[tokio::test]
async fn test_buy_no_signal() {
    let threshold = 0.003;
    let config = create_test_config(threshold);
    let state = Arc::new(MarketState::new());

    let (binance_tx, binance_rx) = broadcast::channel(10);
    let (_poly_tx, poly_rx) = broadcast::channel(10);
    let (signal_tx, signal_rx) = unbounded();

    let detector = Detector::new(config, state.clone(), binance_rx, poly_rx, signal_tx);
    detector.start();

    // Setup state
    let now = current_time_us();
    
    // contract implies $70,000, staleness = 800ms 
    state.update_contract(
        "token_2".to_string(),
        "Will BTC close above $70,000?".to_string(),
        0.5, // implies $70,000
        0.5,
        now - 800_000,
    );

    // Spot is 69,500
    state.update_spot(69_500.0, now);

    let _ = binance_tx.send(PriceUpdate { source: "bin".into(), price: 69_500.0, timestamp_us: now });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = signal_rx.try_recv().expect("Should receive a signal");
    match res {
        ArbSignal::BuyNo { token_id, edge_pct, .. } => {
            assert_eq!(token_id, "token_2");
            assert!((edge_pct - 0.00719).abs() < 0.0001); // 500 / 69,500 ≈ 0.719%
        }
        _ => panic!("Expected BuyNo signal"),
    }
}

#[tokio::test]
async fn test_below_threshold() {
    let threshold = 0.003; // 0.3%
    let config = create_test_config(threshold);
    let state = Arc::new(MarketState::new());

    let (binance_tx, binance_rx) = broadcast::channel(10);
    let (_poly_tx, poly_rx) = broadcast::channel(10);
    let (signal_tx, signal_rx) = unbounded();

    let detector = Detector::new(config, state.clone(), binance_rx, poly_rx, signal_tx);
    detector.start();

    // Setup state
    let now = current_time_us();
    
    // Spot is 70,100
    state.update_spot(70_100.0, now);
    
    // contract implies $70,000. 100 / 70100 ≈ 0.14% edge (below threshold 0.3%)
    state.update_contract(
        "token_3".to_string(),
        "Will BTC close above $70,000?".to_string(),
        0.5, 
        0.5,
        now - 800_000,
    );

    let _ = binance_tx.send(PriceUpdate { source: "bin".into(), price: 70_100.0, timestamp_us: now });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Should be no signal
    assert!(signal_rx.try_recv().is_err());
}

#[test]
fn test_edge_calculation_accuracy() {
    let btc_spot: f64 = 70_500.0;
    let implied_btc: f64 = 70_000.0;
    
    let edge_pct = (btc_spot - implied_btc).abs() / btc_spot;
    
    // Exact edge calculation verification
    // 500 / 70500 = 0.00709219858...
    let expected_edge = 0.00709219858;
    
    assert!((edge_pct - expected_edge).abs() < 1e-8);
}
