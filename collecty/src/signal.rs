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

    pub fn tag(self) -> u8 {
        match self {
            Signal::Logs => 1,
            Signal::Traces => 2,
            Signal::Metrics => 3,
        }
    }

    pub fn from_tag(tag: u8) -> Option<Signal> {
        Signal::ALL.into_iter().find(|signal| signal.tag() == tag)
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
    fn every_signal_round_trips_through_its_tag() {
        for signal in Signal::ALL {
            assert_eq!(Signal::from_tag(signal.tag()), Some(signal));
        }
        assert_eq!(Signal::from_tag(0), None);
        assert_eq!(Signal::from_tag(4), None);
    }
}
