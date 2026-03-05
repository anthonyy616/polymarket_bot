use polyarb::config::Config;
use polyarb::engine::detector::Detector;
use polyarb::engine::state::MarketState;
use polyarb::execution::Executor;
use polyarb::feeds::binance::BinanceFeed;
use polyarb::feeds::polymarket::{MarketSnapshot, PolymarketFeed};
use polyarb::feeds::binance::PriceUpdate;
use polyarb::logger::PnlTracker;
use polyarb::risk::RiskManager;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting polyarb...");

    // 1. Load config
    let config = Arc::new(Config::load()?);
    info!("Config loaded. Simulation mode: {}", config.execution.simulation_mode);

    // 2. Shared state
    let market_state = Arc::new(MarketState::new());

    // 3. Broadcast channels for feeds
    let (binance_tx, binance_rx_detector) = broadcast::channel::<PriceUpdate>(256);
    let (poly_tx, poly_rx_detector) = broadcast::channel::<MarketSnapshot>(64);

    // 4. Async mpsc channel for signals (detector -> executor loop)
    let (signal_tx, mut signal_rx) = mpsc::unbounded_channel();

    // 5. Risk manager
    let risk_manager = Arc::new(RiskManager::new(config.clone()));

    // 6. PnL tracker
    let pnl_tracker = Arc::new(PnlTracker::new("logs"));

    // 7. Executor (with pnl_tracker wired in)
    let executor = Arc::new(Executor::new(config.clone(), risk_manager.clone(), pnl_tracker.clone()));

    // --- Spawn tasks ---

    // Task 1: Binance feed
    let binance_feed = BinanceFeed::new(config.feeds.binance_ws_url.clone(), binance_tx);
    tokio::spawn(async move {
        binance_feed.run().await;
    });

    // Task 2: Polymarket feed
    let polymarket_feed = PolymarketFeed::new(
        config.feeds.polymarket_api_url.clone(),
        config.feeds.polymarket_poll_interval_ms,
        poly_tx,
    );
    tokio::spawn(async move {
        polymarket_feed.run().await;
    });

    // Task 3: Detector subscribing to both feeds
    let detector = Detector::new(
        config.clone(),
        market_state.clone(),
        binance_rx_detector,
        poly_rx_detector,
        signal_tx,
    );
    detector.start();

    // Task 4: Executor loop — reads signals asynchronously, gates through risk, executes
    let executor_handle = {
        let executor = executor.clone();
        let risk_manager = risk_manager.clone();
        tokio::spawn(async move {
            while let Some(signal) = signal_rx.recv().await {
                let decision = risk_manager.evaluate(&signal);
                executor.execute(&signal, &decision).await;
            }
            info!("Signal channel closed. Shutting down executor loop.");
        })
    };

    // Task 5: PnL summary printer every 60 seconds
    let pnl_handle = {
        let pnl_tracker = pnl_tracker.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                pnl_tracker.print_summary();
            }
        })
    };

    // Graceful shutdown on Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Ctrl+C received. Shutting down...");
    executor_handle.abort();
    pnl_handle.abort();
    pnl_tracker.print_summary();
    info!("Final PnL summary printed. Goodbye.");

    Ok(())
}
