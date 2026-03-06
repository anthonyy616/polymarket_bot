use crate::config::Config;
use crate::engine::signal::ArbSignal;
use dashmap::DashMap;
use std::sync::{Arc, RwLock};
use tracing::info;

#[derive(Debug, Clone)]
pub struct Position {
    pub token_id: String,
    pub entry_size_usdc: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RiskDecision {
    Approved { size_usdc: f64 },
    Rejected { reason: String },
}

pub struct RiskManager {
    config: Arc<Config>,
    daily_pnl: RwLock<f64>,
    open_positions: DashMap<String, Position>,
    total_deployed_usdc: RwLock<f64>,
    available_capital: RwLock<f64>,
}

impl RiskManager {
    pub fn new(config: Arc<Config>) -> Self {
        let starting_capital = config.risk.starting_capital_usdc;
        Self {
            config,
            daily_pnl: RwLock::new(0.0),
            open_positions: DashMap::new(),
            total_deployed_usdc: RwLock::new(0.0),
            available_capital: RwLock::new(starting_capital),
        }
    }

    pub fn evaluate(&self, signal: &ArbSignal) -> RiskDecision {
        let (token_id, edge_pct) = match signal {
            ArbSignal::BuyYes { token_id, edge_pct, .. } => (token_id, edge_pct),
            ArbSignal::BuyNo { token_id, edge_pct, .. } => (token_id, edge_pct),
            ArbSignal::NoSignal => return RiskDecision::Rejected { reason: "NoSignal".into() },
        };

        // 1. Check daily loss limit
        let current_pnl = *self.daily_pnl.read().unwrap();
        if current_pnl <= -self.config.risk.max_daily_loss_usdc {
            return RiskDecision::Rejected { 
                reason: format!("Daily loss limit hit: {}", current_pnl) 
            };
        }

        // 2. Check max concurrent positions
        if self.open_positions.len() >= self.config.risk.max_concurrent_positions {
            return RiskDecision::Rejected { 
                reason: format!("Max concurrent positions ({}) reached", self.config.risk.max_concurrent_positions) 
            };
        }

        // 3. Check if position already open on this token
        if self.open_positions.contains_key(token_id) {
            return RiskDecision::Rejected { 
                reason: format!("Position already open for token {}", token_id) 
            };
        }

        // 4. Check signal edge threshold
        if *edge_pct < self.config.risk.arb_threshold_pct {
            return RiskDecision::Rejected { 
                reason: format!("Edge {:.3}% below threshold {:.3}%", edge_pct * 100.0, self.config.risk.arb_threshold_pct * 100.0) 
            };
        }

        // Calculate size: min(config.max_position_usdc, available_capital * edge_pct * 10)
        let cap = *self.available_capital.read().unwrap();
        let scaled_size = cap * edge_pct * 10.0;
        let size_usdc = self.config.risk.max_position_usdc.min(scaled_size).max(0.0);

        if size_usdc <= 0.0 {
            return RiskDecision::Rejected {
                reason: "Calculated size is <= 0".into()
            };
        }

        RiskDecision::Approved { size_usdc }
    }

    pub fn record_fill(&self, token_id: String, size_usdc: f64) {
        self.open_positions.insert(
            token_id.clone(),
            Position {
                token_id,
                entry_size_usdc: size_usdc,
            },
        );

        let mut total = self.total_deployed_usdc.write().unwrap();
        *total += size_usdc;
        
        // Deduct from available capital
        let mut cap = self.available_capital.write().unwrap();
        *cap -= size_usdc;
        
        info!("Recorded fill for token. Total deployed: {}, Available Cap: {}", *total, *cap);
    }

    pub fn record_close(&self, token_id: &str, pnl_usdc: f64) {
        if let Some((_, pos)) = self.open_positions.remove(token_id) {
            let mut total = self.total_deployed_usdc.write().unwrap();
            *total -= pos.entry_size_usdc;

            let mut daily = self.daily_pnl.write().unwrap();
            *daily += pnl_usdc;

            let mut cap = self.available_capital.write().unwrap();
            *cap += pos.entry_size_usdc + pnl_usdc;

            info!(
                "Closed position for token {}. PnL: {}, Daily PnL: {}, Total deployed: {}", 
                token_id, pnl_usdc, *daily, *total
            );
        }
    }
}
