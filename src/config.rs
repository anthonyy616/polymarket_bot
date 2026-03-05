use serde::Deserialize;
use config::{Config as ConfigBuilder, Environment, File};
use anyhow::{Context, Result};

#[derive(Debug, Deserialize, Clone)]
pub struct RiskConfig {
    pub arb_threshold_pct: f64,
    pub max_position_usdc: f64,
    pub max_daily_loss_usdc: f64,
    pub max_concurrent_positions: usize,
    pub starting_capital_usdc: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExecutionConfig {
    pub simulation_mode: bool,
    pub order_timeout_ms: u64,
    pub slippage_tolerance_pct: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FeedsConfig {
    pub binance_ws_url: String,
    pub polymarket_api_url: String,
    pub polymarket_poll_interval_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiConfig {
    pub polymarket_key: String,
    pub polymarket_secret: String,
    pub polymarket_passphrase: String,
    pub wallet_private_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub risk: RiskConfig,
    pub execution: ExecutionConfig,
    pub feeds: FeedsConfig,
    
    #[serde(default)]
    pub target_tokens: Vec<String>,

    pub api: ApiConfig,
}

impl Config {
    pub fn load() -> Result<Self> {
        // Load .env file if present
        dotenv::dotenv().ok();

        // Build the configuration
        // 1. Start with the default.toml file
        // 2. Map specific API variables from standard environment variables
        // 3. Allow overarching overrides using POLYARB_ prefix 
        //    (e.g. POLYARB_RISK__MAX_POSITION_USDC)
        let builder = ConfigBuilder::builder()
            .add_source(File::with_name("config/default.toml"))
            .set_override_option("api.polymarket_key", std::env::var("POLYMARKET_API_KEY").ok())?
            .set_override_option("api.polymarket_secret", std::env::var("POLYMARKET_SECRET").ok())?
            .set_override_option("api.polymarket_passphrase", std::env::var("POLYMARKET_PASSPHRASE").ok())?
            .set_override_option("api.wallet_private_key", std::env::var("POLYMARKET_WALLET_KEY").ok())?
            .add_source(Environment::with_prefix("POLYARB").separator("__"));

        let config: Config = builder.build()?
            .try_deserialize()
            .context("Failed to parse configuration")?;

        Ok(config)
    }
}
