# Project: polyarb — Polymarket BTC Latency Arbitrage Bot in Rust

## Overview

This is a high-performance latency arbitrage bot in Rust that exploits the price lag between real-time BTC spot price feeds (Binance) and Polymarket's prediction market contract prices.

### Strategy

Polymarket's prediction markets (e.g., "Will BTC close above $70,000?") reprice slower than spot moves. The bot detects gaps (0.3–0.8% edge) when BTC pumps/dumps and buys mispriced contracts before the crowd catches up. This is a latency arbitrage strategy focused on speed and efficiency.

## Core Components

- **Binance WebSocket Feed**: Real-time BTC price stream.
- **Polymarket HTTP Poller**: Polls CLOB API for contract prices every 500ms.
- **Detection Engine**: Calculates `implied_btc` from contract prices and compares it to spot price to find edge.
- **Risk Manager**: Gates signals based on position sizing, daily loss caps, and deduplication.
- **Executor**: Simulation mode (default) or live order submission via Polygon (0x protocol).

## Technical Stack

- **Runtime**: `tokio` (async)
- **Networking**: `tokio-tungstenite` (WebSocket), `reqwest` (HTTP)
- **Serialization**: `serde`, `serde_json`
- **Crypto/Blockchain**: `ethers` (Ethereum-compatible wallet signing)
- **Concurrency**: `crossbeam-channel`, `dashmap`
- **Execution**: 0x protocol (Polygon)

## Project Structure

- `src/main.rs`: Orchestrator and task spawning.
- `src/feeds/`: External data ingestion (Binance, Polymarket).
- `src/engine/`: Market state and detection logic.
- `src/risk/`: Position management and safety gates.
- `src/execution/`: Order submission and simulation.
- `src/logger/`: PnL tracking and structured logging.

## Operational Modes

- **Simulation**: Default. Logs trades as if they executed, no live capital used.
- **Live**: Requires `LIVE_MODE=true` AND `CONFIRM_LIVE=yes` in `.env`.

## Key Parameters

- **Edge Threshold**: Default 0.3%
- **Max Position**: Default $20 USDC
- **Max Daily Loss**: Default $50 USDC
- **Max Concurrent Positions**: 3
