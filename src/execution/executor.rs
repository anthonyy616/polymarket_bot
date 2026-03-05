use crate::config::Config;
use crate::engine::signal::ArbSignal;
use crate::risk::{manager::RiskManager, RiskDecision};
use std::sync::Arc;
use tracing::info;

pub struct Executor {
    config: Arc<Config>,
    risk_manager: Arc<RiskManager>,
}

impl Executor {
    pub fn new(config: Arc<Config>, risk_manager: Arc<RiskManager>) -> Self {
        Self {
            config,
            risk_manager,
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
                todo!("LIVE MODE: Sign and submit order to Polymarket CLOB. Request POST to /order");
            } else {
                info!("Live mode requested but LIVE_MODE=true and CONFIRM_LIVE=yes not set. Defaulting to simulation.");
                self.simulate_execution(signal, size_usdc).await;
            }
        }
    }

    async fn simulate_execution(&self, signal: &ArbSignal, size_usdc: f64) {
        match signal {
            ArbSignal::BuyYes { token_id, edge_pct, reason, .. } => {
                info!(
                    "[SIM] BUY YES {} | edge: {:.3}% | size: ${:.2} | {}",
                    token_id,
                    edge_pct * 100.0,
                    size_usdc,
                    reason
                );
                self.risk_manager.record_fill(token_id.clone(), size_usdc);
            }
            ArbSignal::BuyNo { token_id, edge_pct, reason, .. } => {
                info!(
                    "[SIM] BUY NO {} | edge: {:.3}% | size: ${:.2} | {}",
                    token_id,
                    edge_pct * 100.0,
                    size_usdc,
                    reason
                );
                self.risk_manager.record_fill(token_id.clone(), size_usdc);
            }
            ArbSignal::NoSignal => {}
        }
    }
}
