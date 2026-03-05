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
        if btc_spot <= 0.0 {
            debug!("Skipping detection: BTC spot price is not yet available.");
            return;
        }

        let current_time_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        for contract_entry in self.market_state.contracts.iter() {
            let contract = contract_entry.value();

            let staleness_us = current_time_us.saturating_sub(contract.last_polymarket_update_us);

            // Skip contracts updated less than 200ms ago (they're fresh, no edge)
            if staleness_us <= 200_000 {
                continue;
            }

            if let Some(implied_btc_price) = contract.implied_btc_price {
                let edge_pct = (btc_spot - implied_btc_price).abs() / btc_spot;

                if edge_pct > self.config.risk.arb_threshold_pct {
                    let mut signal = ArbSignal::NoSignal;
                    
                    let recommended_size = self.config.risk.max_position_usdc;
                    let reason = format!(
                        "Edge {:.3}% > Threshold {:.3}%. Spot: {:.2}, Implied: {:.2}",
                        edge_pct * 100.0,
                        self.config.risk.arb_threshold_pct * 100.0,
                        btc_spot,
                        implied_btc_price
                    );

                    if btc_spot > implied_btc_price {
                        signal = ArbSignal::BuyYes {
                            token_id: contract.token_id.clone(),
                            edge_pct,
                            recommended_size_usdc: recommended_size,
                            reason: reason.clone(),
                        };
                    } else if btc_spot < implied_btc_price {
                        signal = ArbSignal::BuyNo {
                            token_id: contract.token_id.clone(),
                            edge_pct,
                            recommended_size_usdc: recommended_size,
                            reason: reason.clone(),
                        };
                    }

                    if signal != ArbSignal::NoSignal {
                        info!(
                            event = "arb_detected",
                            trigger = trigger_source,
                            token_id = %contract.token_id,
                            staleness_us = staleness_us,
                            edge_pct = edge_pct,
                            spot = btc_spot,
                            implied = implied_btc_price,
                            "Signal emitted: {:?}", signal
                        );

                        // Fire and forget, executor should be picking it up
                        let _ = self.signal_tx.send(signal);
                    }
                } else {
                    // Log the detection event with full context even when no signal fires
                    info!(
                        event = "detection_tick",
                        trigger = trigger_source,
                        token_id = %contract.token_id,
                        staleness_us = staleness_us,
                        edge_pct = edge_pct,
                        spot = btc_spot,
                        implied = implied_btc_price,
                        threshold = self.config.risk.arb_threshold_pct,
                        "No edge found"
                    );
                }
            }
        }
    }
}
