use chrono::Local;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::RwLock;
use tracing::info;

#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub timestamp: String,
    pub token_id: String,
    pub direction: String,
    pub size_usdc: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub pnl_usdc: f64,
    pub edge_pct_at_entry: f64,
    pub staleness_ms: f64,
    pub signal_to_order_ms: f64,
}

pub struct PnlTracker {
    log_dir: PathBuf,
    trades: RwLock<Vec<TradeRecord>>,
}

impl PnlTracker {
    pub fn new(log_dir: &str) -> Self {
        let path = PathBuf::from(log_dir);
        fs::create_dir_all(&path).expect("Failed to create logs directory");

        Self {
            log_dir: path,
            trades: RwLock::new(Vec::new()),
        }
    }

    fn csv_path(&self) -> PathBuf {
        let date = Local::now().format("%Y-%m-%d").to_string();
        self.log_dir.join(format!("trades_{}.csv", date))
    }

    fn ensure_csv_header(&self, path: &PathBuf) {
        if !path.exists() {
            if let Ok(mut file) = OpenOptions::new().create(true).write(true).open(path) {
                let _ = writeln!(
                    file,
                    "timestamp,token_id,direction,size_usdc,entry_price,exit_price,pnl_usdc,edge_pct_at_entry,staleness_ms,signal_to_order_ms"
                );
            }
        }
    }

    pub fn record_trade(&self, trade: TradeRecord) {
        let csv_path = self.csv_path();
        self.ensure_csv_header(&csv_path);

        if let Ok(mut file) = OpenOptions::new().append(true).open(&csv_path) {
            let _ = writeln!(
                file,
                "{},{},{},{:.2},{:.6},{:.6},{:.4},{:.6},{:.1},{:.1}",
                trade.timestamp,
                trade.token_id,
                trade.direction,
                trade.size_usdc,
                trade.entry_price,
                trade.exit_price,
                trade.pnl_usdc,
                trade.edge_pct_at_entry,
                trade.staleness_ms,
                trade.signal_to_order_ms,
            );
        }

        let mut trades = self.trades.write().unwrap();
        trades.push(trade);
    }

    pub fn print_summary(&self) {
        let trades = self.trades.read().unwrap();
        let total_trades = trades.len();

        if total_trades == 0 {
            info!(
                event = "pnl_summary",
                "--- PnL Summary --- No trades recorded yet."
            );
            return;
        }

        let total_pnl: f64 = trades.iter().map(|t| t.pnl_usdc).sum();
        let wins = trades.iter().filter(|t| t.pnl_usdc > 0.0).count();
        let win_rate = wins as f64 / total_trades as f64 * 100.0;
        let avg_edge: f64 = trades.iter().map(|t| t.edge_pct_at_entry).sum::<f64>() / total_trades as f64;
        let avg_staleness: f64 = trades.iter().map(|t| t.staleness_ms).sum::<f64>() / total_trades as f64;

        info!(
            event = "pnl_summary",
            total_trades = total_trades,
            win_rate = format!("{:.1}%", win_rate),
            total_pnl = format!("${:.2}", total_pnl),
            avg_edge = format!("{:.4}%", avg_edge * 100.0),
            avg_staleness = format!("{:.0}ms", avg_staleness),
            "--- PnL Summary ---"
        );
    }
}
