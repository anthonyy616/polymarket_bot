use crate::config::Config;
use crate::engine::signal::ArbSignal;
use crate::logger::{PnlTracker, TradeRecord};
use crate::risk::{manager::RiskManager, RiskDecision};
use chrono::Local;
use std::sync::Arc;
use tracing::info;

pub struct Executor {
    config: Arc<Config>,
    risk_manager: Arc<RiskManager>,
    pnl_tracker: Arc<PnlTracker>,
}

impl Executor {
    pub fn new(config: Arc<Config>, risk_manager: Arc<RiskManager>, pnl_tracker: Arc<PnlTracker>) -> Self {
        Self {
            config,
            risk_manager,
            pnl_tracker,
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
        let (token_id, edge_pct, direction) = match signal {
            ArbSignal::BuyYes { token_id, edge_pct, reason, .. } => {
                info!(
                    "[SIM] BUY YES {} | edge: {:.3}% | size: ${:.2} | {}",
                    token_id,
                    edge_pct * 100.0,
                    size_usdc,
                    reason
                );
                (token_id.clone(), *edge_pct, "BuyYes")
            }
            ArbSignal::BuyNo { token_id, edge_pct, reason, .. } => {
                info!(
                    "[SIM] BUY NO {} | edge: {:.3}% | size: ${:.2} | {}",
                    token_id,
                    edge_pct * 100.0,
                    size_usdc,
                    reason
                );
                (token_id.clone(), *edge_pct, "BuyNo")
            }
            ArbSignal::NoSignal => return,
        };

        self.risk_manager.record_fill(token_id.clone(), size_usdc);

        self.pnl_tracker.record_trade(TradeRecord {
            timestamp: Local::now().format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
            token_id,
            direction: direction.to_string(),
            size_usdc,
            entry_price: 0.0,  // Simulated — no real fill price
            exit_price: 0.0,
            pnl_usdc: 0.0,     // Unknown until position closes
            edge_pct_at_entry: edge_pct,
            staleness_ms: 0.0, // TODO: pass from signal metadata
            signal_to_order_ms: 0.0,
        });
    }
}
