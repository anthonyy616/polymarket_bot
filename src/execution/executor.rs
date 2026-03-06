use crate::config::Config;
use crate::engine::signal::ArbSignal;
use crate::logger::{PnlTracker, TradeRecord};
use crate::risk::{manager::RiskManager, RiskDecision};
use chrono::Local;
use std::sync::Arc;
use tracing::info;

use crate::engine::state::MarketState;
use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SimulatedPosition {
    pub token_id: String,
    pub entry_price: f64,
    pub size_usdc: f64,
    pub direction: String, // "BuyYes" or "BuyNo"
    pub entry_window_start: u64,
    pub edge_at_entry: f64,
    pub staleness_ms: f64,
    pub signal_to_order_ms: f64,
}

pub struct Executor {
    config: Arc<Config>,
    risk_manager: Arc<RiskManager>,
    pnl_tracker: Arc<PnlTracker>,
    market_state: Arc<MarketState>,
    open_positions: DashMap<String, SimulatedPosition>,
}

impl Executor {
    pub fn new(
        config: Arc<Config>, 
        risk_manager: Arc<RiskManager>, 
        pnl_tracker: Arc<PnlTracker>,
        market_state: Arc<MarketState>,
    ) -> Self {
        Self {
            config,
            risk_manager,
            pnl_tracker,
            market_state,
            open_positions: DashMap::new(),
        }
    }

    pub fn start_monitor(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let executor = self.clone();
        tokio::spawn(async move {
            info!("Position monitor started.");
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                executor.check_and_close_positions();
            }
        })
    }

    fn check_and_close_positions(&self) {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap().as_secs();
        let current_window = (now_secs / 900) * 900;

        let mut to_close = Vec::new();

        for entry in self.open_positions.iter() {
            let pos = entry.value();
            
            // 1. Window rollover
            if current_window != pos.entry_window_start {
                to_close.push((pos.token_id.clone(), "Window Expired"));
                continue;
            }

            // 2. Price move > 2% in favor
            if let Some(contract) = self.market_state.contracts.get(&pos.token_id) {
                let current_price = if pos.direction == "BuyYes" {
                    contract.yes_price
                } else {
                    contract.no_price
                };

                let profit_pct = (current_price - pos.entry_price) / pos.entry_price;
                if profit_pct >= 0.02 {
                    to_close.push((pos.token_id.clone(), "Take Profit >2%"));
                }
            }
        }

        for (token_id, reason) in to_close {
            self.close_position(&token_id, reason);
        }
    }

    fn close_position(&self, token_id: &str, reason: &str) {
        if let Some((_, pos)) = self.open_positions.remove(token_id) {
            let current_price = if let Some(contract) = self.market_state.contracts.get(token_id) {
                if pos.direction == "BuyYes" {
                    contract.yes_price
                } else {
                    contract.no_price
                }
            } else {
                // If contract missing, assume neutral exit (unlikely with window logic)
                pos.entry_price
            };

            let pnl_usdc = (current_price - pos.entry_price) * pos.size_usdc / pos.entry_price;

            info!(
                "[SIM] CLOSE {} | reason: {} | exit_px: {:.4} | pnl: ${:.4}",
                token_id, reason, current_price, pnl_usdc
            );

            self.risk_manager.record_close(token_id, pnl_usdc);

            self.pnl_tracker.record_trade(TradeRecord {
                timestamp: Local::now().format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
                token_id: pos.token_id,
                direction: format!("{}_CLOSE", pos.direction),
                size_usdc: pos.size_usdc,
                entry_price: pos.entry_price,
                exit_price: current_price,
                pnl_usdc,
                edge_pct_at_entry: pos.edge_at_entry,
                staleness_ms: pos.staleness_ms,
                signal_to_order_ms: pos.signal_to_order_ms,
            });
        }
    }

    pub async fn execute(&self, signal: &ArbSignal, risk_decision: &RiskDecision) {
        let size_usdc = match risk_decision {
            RiskDecision::Approved { size_usdc } => *size_usdc,
            RiskDecision::Rejected { reason } => {
                info!("Trade rejected by Risk Manager: {}", reason);
                return;
            }
        };

        if self.config.execution.simulation_mode {
            self.simulate_execution(signal, size_usdc).await;
        } else {
            // Live mode requires explicit active flags
            let live_active = std::env::var("LIVE_MODE").unwrap_or_default() == "true" 
                && std::env::var("CONFIRM_LIVE").unwrap_or_default() == "yes";
                
            if live_active {
                tracing::error!(
                    "LIVE MODE IS NOT YET IMPLEMENTED. \
                     Unset LIVE_MODE and CONFIRM_LIVE to use simulation."
                );
                return;
            } else {
                info!("Live mode requested but LIVE_MODE=true and CONFIRM_LIVE=yes not set. Defaulting to simulation.");
                self.simulate_execution(signal, size_usdc).await;
            }
        }
    }

    async fn simulate_execution(&self, signal: &ArbSignal, size_usdc: f64) {
        let current_time_us = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;

        let (token_id, entry_price, edge_pct, direction, staleness_ms, signal_to_order_ms) = match signal {
            ArbSignal::BuyYes { 
                token_id, price, edge_pct, reason, created_at_us, staleness_ms, .. 
            } => {
                let delay_ms = (current_time_us - *created_at_us) as f64 / 1000.0;
                info!(
                    "[SIM] BUY YES {} @ {:.4} | edge: {:.3}% | size: ${:.2} | delay: {:.1}ms | {}",
                    token_id, price, edge_pct * 100.0, size_usdc, delay_ms, reason
                );
                (token_id.clone(), *price, *edge_pct, "BuyYes", *staleness_ms as f64, delay_ms)
            }
            ArbSignal::BuyNo { 
                token_id, price, edge_pct, reason, created_at_us, staleness_ms, .. 
            } => {
                let delay_ms = (current_time_us - *created_at_us) as f64 / 1000.0;
                info!(
                    "[SIM] BUY NO {} @ {:.4} | edge: {:.3}% | size: ${:.2} | delay: {:.1}ms | {}",
                    token_id, price, edge_pct * 100.0, size_usdc, delay_ms, reason
                );
                (token_id.clone(), *price, *edge_pct, "BuyNo", *staleness_ms as f64, delay_ms)
            }
            ArbSignal::NoSignal => return,
        };

        self.risk_manager.record_fill(token_id.clone(), size_usdc);

        let window_start = (current_time_us / 1_000_000 / 900) * 900;
        
        self.open_positions.insert(
            token_id.clone(),
            SimulatedPosition {
                token_id: token_id.clone(),
                entry_price,
                size_usdc,
                direction: direction.to_string(),
                entry_window_start: window_start,
                edge_at_entry: edge_pct,
                staleness_ms,
                signal_to_order_ms,
            },
        );

        self.pnl_tracker.record_trade(TradeRecord {
            timestamp: Local::now().format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
            token_id,
            direction: direction.to_string(),
            size_usdc,
            entry_price,
            exit_price: 0.0,
            pnl_usdc: 0.0,
            edge_pct_at_entry: edge_pct,
            staleness_ms,
            signal_to_order_ms,
        });
    }
}
