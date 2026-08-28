use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Signal {
    Logs,
    Traces,
    Metrics,
}

impl Signal {
    pub const ALL: [Signal; 3] = [Signal::Logs, Signal::Traces, Signal::Metrics];

    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Traces => "traces",
            Signal::Metrics => "metrics",
        }
    }

    pub fn grpc_method_path(self) -> &'static str {
        match self {
            Signal::Logs => "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            Signal::Traces => "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
            Signal::Metrics => "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
        }
    }

    pub fn from_grpc_method_path(path: &str) -> Option<Signal> {
        Signal::ALL
            .into_iter()
            .find(|signal| signal.grpc_method_path() == path)
    }

    /// Which of the three queues this signal owns.
    ///
    /// A slot in memory and nothing else. The signal is named on the wire and
    /// in the queue's directory names, so there is no number anywhere for this
    /// to have to agree with.
    pub fn index(self) -> usize {
        match self {
            Signal::Logs => 0,
            Signal::Traces => 1,
            Signal::Metrics => 2,
        }
    }
}

impl fmt::Display for Signal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_signal_round_trips_through_its_grpc_method_path() {
        for signal in Signal::ALL {
            assert_eq!(
                Signal::from_grpc_method_path(signal.grpc_method_path()),
                Some(signal)
            );
        }
        assert_eq!(Signal::from_grpc_method_path("/health/Check"), None);
    }

    #[test]
    fn every_signal_has_a_slot_of_its_own() {
        for (slot, signal) in Signal::ALL.into_iter().enumerate() {
            assert_eq!(signal.index(), slot);
        }
    }
}
