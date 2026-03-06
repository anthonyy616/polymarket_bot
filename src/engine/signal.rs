#[derive(Debug, Clone, PartialEq)]
pub enum SignalStrength {
    /// 0.3% - 0.5% edge
    Weak,
    /// 0.5% - 0.8% edge
    Moderate,
    /// > 0.8% edge
    Strong,
}

impl SignalStrength {
    /// Determines the signal strength based on the edge percentage.
    pub fn from_edge(edge_pct: f64) -> Self {
        if edge_pct > 0.8 {
            SignalStrength::Strong
        } else if edge_pct >= 0.5 {
            SignalStrength::Moderate
        } else {
            SignalStrength::Weak
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArbSignal {
    BuyYes {
        token_id: String,
        price: f64,
        edge_pct: f64,
        recommended_size_usdc: f64,
        reason: String,
        created_at_us: u64,
        staleness_ms: u64,
    },
    BuyNo {
        token_id: String,
        price: f64,
        edge_pct: f64,
        recommended_size_usdc: f64,
        reason: String,
        created_at_us: u64,
        staleness_ms: u64,
    },
    NoSignal,
}

impl ArbSignal {
    /// Returns the implied signal strength, if there is a signal.
    pub fn strength(&self) -> Option<SignalStrength> {
        match self {
            ArbSignal::BuyYes { edge_pct, .. } | ArbSignal::BuyNo { edge_pct, .. } => {
                Some(SignalStrength::from_edge(*edge_pct))
            }
            ArbSignal::NoSignal => None,
        }
    }
}
