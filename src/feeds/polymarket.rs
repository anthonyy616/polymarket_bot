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

            match self.client
                .get(&endpoint)
                .query(&[
                    ("active", "true"),
                    ("closed", "false"),
                    ("tag_slug", "crypto"),
                    ("limit", "50"),
                ])
                .send().await
            {
                Ok(response) if response.status().is_success() => {
                    match response.json::<Vec<serde_json::Value>>().await {
                        Ok(markets) => {
                            let mut contracts = Vec::new();

                            for market in &markets {
                                let question = market
                                    .get("question")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                //temporary line for debug
                            tracing::info!("Market found: {}", question);

                                let q = question.to_uppercase();
                                if !q.contains("BTC") && !q.contains("BITCOIN") {
                                    continue;
                                }

                                let accepting = market
                                    .get("acceptingOrders")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);
                                if !accepting {
                                    continue;
                                }

                                // clobTokenIds — try as array first, then as JSON string
                                let token_ids: Vec<String> = market
                                    .get("clobTokenIds")
                                    .and_then(|v| {
                                        if let Some(arr) = v.as_array() {
                                            Some(arr.iter()
                                                .filter_map(|t| t.as_str().map(String::from))
                                                .collect())
                                        } else if let Some(s) = v.as_str() {
                                            serde_json::from_str(s).ok()
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or_default();

                                // outcomePrices — try as array first, then as JSON string
                                let prices: Vec<f64> = market
                                    .get("outcomePrices")
                                    .and_then(|v| {
                                        if let Some(arr) = v.as_array() {
                                            Some(arr.iter()
                                                .filter_map(|p| {
                                                    p.as_f64().or_else(|| {
                                                        p.as_str()?.parse().ok()
                                                    })
                                                })
                                                .collect())
                                        } else if let Some(s) = v.as_str() {
                                            serde_json::from_str(s).ok()
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or_default();

                                // Per docs: index 0 = Yes token, index 1 = No token
                                let yes_token_id = token_ids.get(0).cloned().unwrap_or_default();
                                let yes_price = prices.get(0).copied().unwrap_or(0.0);
                                let no_price = prices.get(1).copied().unwrap_or(0.0);

                                if yes_token_id.is_empty() || yes_price <= 0.0 {
                                    tracing::debug!(
                                        "Skipping BTC market with no valid price: {}", question
                                    );
                                    continue;
                                }

                                tracing::info!(
                                    "Found BTC market: {} | yes: {} | no: {}",
                                    question, yes_price, no_price
                                );

                                contracts.push(ContractPrice {
                                    token_id: yes_token_id,
                                    question,
                                    yes_price,
                                    no_price,
                                    fetched_at_us: timestamp_us,
                                });
                            }

                            if !contracts.is_empty() {
                                let _ = self.sender.send(MarketSnapshot { contracts });
                            } else {
                                tracing::warn!("No active BTC markets found in this poll");
                            }
                        }
                        Err(e) => tracing::warn!("Failed to parse gamma API response: {}", e),
                    }
                }
                Ok(r) => tracing::warn!("Gamma API returned status: {}", r.status()),
                Err(e) => tracing::error!("Gamma API request failed: {}", e),
            }

            let elapsed = start.elapsed().unwrap_or(Duration::from_millis(0));
            let delay = Duration::from_millis(self.poll_interval_ms).saturating_sub(elapsed);
            if delay > Duration::from_millis(0) {
                sleep(delay).await;
            }
        }
    }
}
