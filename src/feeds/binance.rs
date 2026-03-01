use anyhow::Result;
use futures_util::StreamExt;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub source: String,
    pub price: f64,
    pub timestamp_us: u64,
}

#[derive(Debug, Deserialize)]
struct BinanceTradeEvent {
    #[serde(rename = "p", deserialize_with = "deserialize_f64_from_string")]
    price: f64,
    #[serde(rename = "E")]
    event_time_ms: u64,
}

fn deserialize_f64_from_string<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse::<f64>().map_err(serde::de::Error::custom)
}

pub struct BinanceFeed {
    url: String,
    sender: broadcast::Sender<PriceUpdate>,
}

impl BinanceFeed {
    pub fn new(url: String, sender: broadcast::Sender<PriceUpdate>) -> Self {
        Self { url, sender }
    }

    pub async fn run(&self) {
        loop {
            info!("Connecting to Binance WebSocket: {}", self.url);
            match connect_async(&self.url).await {
                Ok((ws_stream, _)) => {
                    info!("Connected to Binance WebSocket");
                    let (_, mut read) = ws_stream.split();

                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                match serde_json::from_str::<BinanceTradeEvent>(&text) {
                                    Ok(trade) => {
                                        let update = PriceUpdate {
                                            source: "binance".to_string(),
                                            price: trade.price,
                                            timestamp_us: trade.event_time_ms * 1000,
                                        };
                                        // Ignore send errors if there are no receivers yet
                                        let _ = self.sender.send(update);
                                    }
                                    Err(e) => {
                                        warn!("Failed to parse Binance trade event: {} - Raw: {}", e, text);
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => {
                                warn!("Binance WebSocket closed normally.");
                                break;
                            }
                            Err(e) => {
                                error!("Binance WebSocket error: {}", e);
                                break;
                            }
                            _ => {} // Ignore ping/pong/binary
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to Binance: {}", e);
                }
            }

            warn!("Reconnecting to Binance in 5 seconds...");
            sleep(Duration::from_secs(5)).await;
        }
    }
}
