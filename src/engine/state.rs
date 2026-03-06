use dashmap::DashMap;
use std::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ContractState {
    pub token_id: String,
    pub question: String,
    pub yes_price: f64,
    pub no_price: f64,
    pub last_polymarket_update_us: u64,
    pub implied_btc_price: Option<f64>,
}

#[derive(Debug)]
pub struct MarketState {
    pub btc_spot_price: RwLock<f64>,
    pub btc_spot_updated_us: RwLock<u64>,
    pub btc_window_start_price: RwLock<f64>,
    pub btc_window_start_timestamp: RwLock<u64>,
    pub contracts: DashMap<String, ContractState>,
}

impl Default for MarketState {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketState {
    pub fn new() -> Self {
        Self {
            btc_spot_price: RwLock::new(0.0),
            btc_spot_updated_us: RwLock::new(0),
            btc_window_start_price: RwLock::new(0.0),
            btc_window_start_timestamp: RwLock::new(0),
            contracts: DashMap::new(),
        }
    }

    pub fn update_spot(&self, price: f64, timestamp_us: u64) {
        if let Ok(mut lock) = self.btc_spot_price.write() {
            *lock = price;
        }
        if let Ok(mut lock) = self.btc_spot_updated_us.write() {
            *lock = timestamp_us;
        }

        // Track 15m window start price
        let timestamp_secs = timestamp_us / 1_000_000;
        let window_start = (timestamp_secs / 900) * 900;
        
        if let Ok(mut lock_ts) = self.btc_window_start_timestamp.write() {
            if *lock_ts != window_start {
                *lock_ts = window_start;
                // Clear stale contracts from previous window
                self.contracts.clear();
                
                if let Ok(mut lock_px) = self.btc_window_start_price.write() {
                    *lock_px = price;
                    tracing::info!("New 15m window started at {}: Spot={}", window_start, price);
                }
            }
        }
    }

    pub fn update_contract(
        &self,
        token_id: String,
        question: String,
        yes_price: f64,
        no_price: f64,
        timestamp_us: u64,
    ) {
        // For Up/Down markets, yes_price is the probability
        let implied_btc_price = compute_implied_btc_price(&question, yes_price);

        self.contracts.insert(
            token_id.clone(),
            ContractState {
                token_id,
                question,
                yes_price,
                no_price,
                last_polymarket_update_us: timestamp_us,
                implied_btc_price,
            },
        );
    }
}

pub fn compute_implied_btc_price(_question: &str, yes_price: f64) -> Option<f64> {
    // For 15M markets, we return the probability (0..1) as the "implied price"
    // to be used by the detector for directional scoring.
    Some(yes_price)
}
