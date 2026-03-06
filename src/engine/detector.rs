use crate::config::Config;
use crate::engine::signal::ArbSignal;
use crate::engine::state::MarketState;
use crate::feeds::binance::PriceUpdate;
use crate::feeds::polymarket::MarketSnapshot;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tracing::{debug, info};

pub struct Detector {
    config: Arc<Config>,
    market_state: Arc<MarketState>,
    binance_rx: broadcast::Receiver<PriceUpdate>,
    polymarket_rx: broadcast::Receiver<MarketSnapshot>,
    signal_tx: mpsc::UnboundedSender<ArbSignal>,
}

impl Detector {
    pub fn new(
        config: Arc<Config>,
        market_state: Arc<MarketState>,
        binance_rx: broadcast::Receiver<PriceUpdate>,
        polymarket_rx: broadcast::Receiver<MarketSnapshot>,
        signal_tx: mpsc::UnboundedSender<ArbSignal>,
    ) -> Self {
        Self {
            config,
            market_state,
            binance_rx,
            polymarket_rx,
            signal_tx,
        }
    }

    pub fn start(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("Arbitrage detector started.");

            loop {
                tokio::select! {
                    Ok(price_update) = self.binance_rx.recv() => {
                        self.market_state.update_spot(price_update.price, price_update.timestamp_us);
                        self.run_detection("binance_update");
                    }
                    Ok(snapshot) = self.polymarket_rx.recv() => {
                        for contract in snapshot.contracts {
                            self.market_state.update_contract(
                                contract.token_id,
                                contract.question,
                                contract.yes_price,
                                contract.no_price,
                                contract.fetched_at_us,
                            );
                        }
                        self.run_detection("polymarket_update");
                    }
                }
            }
        })
    }

    fn run_detection(&self, trigger_source: &str) {
        let btc_spot = *self.market_state.btc_spot_price.read().unwrap();
        let btc_window_px = *self.market_state.btc_window_start_price.read().unwrap();
        
        if btc_spot <= 0.0 || btc_window_px <= 0.0 {
            return;
        }

        let move_pct = (btc_spot - btc_window_px) / btc_window_px;
        let threshold = 0.003; // 0.3% as per plan

        let current_time_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        for contract_entry in self.market_state.contracts.iter() {
            let contract = contract_entry.value();
            let yes_prob = contract.yes_price; // probability is stored in implied_btc_price or yes_price

            let staleness_us = current_time_us.saturating_sub(contract.last_polymarket_update_us);
            if staleness_us <= 200_000 {
                continue;
            }

            let mut signal = ArbSignal::NoSignal;
            let recommended_size = self.config.risk.max_position_usdc;

            if move_pct > threshold && yes_prob <= 0.5 {
                signal = ArbSignal::BuyYes {
                    token_id: contract.token_id.clone(),
                    edge_pct: move_pct,
                    recommended_size_usdc: recommended_size,
                    reason: format!("Binance UP {:.2}%, Polymarket Yes Prob {:.2}", move_pct * 100.0, yes_prob),
                };
            } else if move_pct < -threshold && yes_prob >= 0.5 {
                signal = ArbSignal::BuyNo {
                    token_id: contract.token_id.clone(),
                    edge_pct: move_pct.abs(),
                    recommended_size_usdc: recommended_size,
                    reason: format!("Binance DOWN {:.2}%, Polymarket Yes Prob {:.2}", move_pct * 100.0, yes_prob),
                };
            }

            if signal != ArbSignal::NoSignal {
                info!(
                    event = "arb_detected",
                    trigger = trigger_source,
                    token_id = %contract.token_id,
                    move_pct = move_pct,
                    yes_prob = yes_prob,
                    "Signal emitted: {:?}", signal
                );
                let _ = self.signal_tx.send(signal);
            }
        }
    }
}
