use reqwest::Client;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct ContractPrice {
    pub token_id: String,
    pub question: String,
    pub yes_price: f64,
    pub no_price: f64,
    pub fetched_at_us: u64,
}

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub contracts: Vec<ContractPrice>,
}

pub struct PolymarketFeed {
    url: String,
    poll_interval_ms: u64,
    sender: broadcast::Sender<MarketSnapshot>,
    client: Client,
}

impl PolymarketFeed {
    pub fn new(url: String, poll_interval_ms: u64, sender: broadcast::Sender<MarketSnapshot>) -> Self {
        Self {
            url,
            poll_interval_ms,
            sender,
            client: Client::new(),
        }
    }

    pub async fn run(&self) {
        info!("Starting Polymarket HTTP poller at {} every {}ms", self.url, self.poll_interval_ms);
        let endpoint = format!("{}/markets", self.url);

        loop {
            let start = SystemTime::now();
            let timestamp_us = start.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;

            // Fetch BTC markets. Appending query param for broader search
            match self.client
                .get(&endpoint)
                .query(&[
                    ("closed", "false"),
                    ("active", "true"),
                    ("tag_slug", "crypto"),
                    ("limit", "50"),
                ])
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        if let Ok(json) = response.json::<Value>().await {
                            tracing::debug!("Raw Polymarket response: {:?}", json);
                            let mut contracts = Vec::new();

                            // The Polymarket CLOB structure can contain arbitrary tokens
                            // Extract data via Value mapping to gracefully handle mismatches.
                            if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                                for market in data {
                                    let is_closed = market.get("closed").and_then(|v| v.as_bool()).unwrap_or(true);
                                    if is_closed {
                                        continue;
                                    }

                                    let question = market
                                        .get("question")
                                        .and_then(|q| q.as_str())
                                        .unwrap_or("Unknown")
                                        .to_string();

                                    // Secondary check to ensure it's BTC related.
                                    let q = question.to_uppercase();
                                    if !q.contains("BTC") && !q.contains("BITCOIN") {
                                        continue;
                                    }

                                    let mut token_id = String::new();
                                    let mut yes_price = 0.0;
                                    let mut no_price = 0.0;

                                    if let Some(tokens_array) = market.get("tokens").and_then(|t| t.as_array()) {
                                        for token in tokens_array {
                                            let outcome = token.get("outcome").and_then(|o| o.as_str()).unwrap_or("");
                                            let price = token.get("price").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                            // Fallback for bid/ask usage if general price empty
                                            let best_bid = token.get("best_bid").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                            let best_ask = token.get("best_ask").and_then(|p| p.as_f64()).unwrap_or(0.0);
                                            
                                            // Prefer current standard definition of price if it is valid, fallback to ask/bid bounds
                                            let final_price = if price > 0.0 { price } else { best_ask.max(best_bid) };

                                            if outcome.eq_ignore_ascii_case("yes") {
                                                yes_price = final_price;
                                                token_id = token.get("token_id").and_then(|t| t.as_str()).unwrap_or("").to_string();
                                            } else if outcome.eq_ignore_ascii_case("no") {
                                                no_price = final_price;
                                            }
                                        }
                                    }
                                    
                                    // Use best prices derived from market if tokens aren't labeled YES/NO directly.
                                    // For robustness we populate the expected fields from the object regardless of structure.
                                    if !token_id.is_empty() {
                                        contracts.push(ContractPrice {
                                            token_id,
                                            question,
                                            yes_price,
                                            no_price,
                                            fetched_at_us: timestamp_us,
                                        });
                                    }
                                }
                            }

                            if !contracts.is_empty() {
                                let snapshot = MarketSnapshot { contracts };
                                let _ = self.sender.send(snapshot);
                            }
                        } else {
                            warn!("Failed to parse Polymarket JSON response");
                        }
                    } else {
                        warn!("Polymarket API returned status: {}", response.status());
                    }
                }
                Err(e) => {
                    error!("Error fetching from Polymarket: {}", e);
                }
            }

            let elapsed = start.elapsed().unwrap_or(Duration::from_millis(0));
            let delay = Duration::from_millis(self.poll_interval_ms).saturating_sub(elapsed);
            if delay > Duration::from_millis(0) {
                sleep(delay).await;
            }
        }
    }
}
