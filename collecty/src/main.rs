use std::sync::Arc;

use collecty::config::Config;
use collecty::observe::Reporter;
use collecty::queue::Queue;
use collecty::receive::{self, Intake};
use collecty::send::{HttpTransport, Sender};
use collecty::signal::Signal;
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("collecty: {error}");
            std::process::exit(2);
        }
    };
    init_tracing(config.log_json);

    if let Err(error) = run(config).await {
        tracing::error!(%error, "collecty stopped");
        std::process::exit(1);
    }
}

fn init_tracing(json: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_env("COLLECTY_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

async fn run(config: Config) -> Result<(), String> {
    let dir = config.queue_dir();
    let queue = Arc::new(
        Queue::open(&dir, config.queue, config.zstd_level)
            .map_err(|error| format!("cannot open the queue at {dir:?}: {error}"))?,
    );

    let listener = receive::bind(&config.socket_path, config.socket_mode)
        .map_err(|error| format!("cannot serve OTLP: {error}"))?;
    let intake = Intake::new(
        queue.clone(),
        config.max_request_bytes,
        config.max_inflight_bytes,
    );

    let (shutdown, watcher) = watch::channel(false);
    let transport = Arc::new(HttpTransport::new(
        config.signy_url.clone(),
        config.send_timeout,
    ));

    let sender = Sender::new(queue.clone(), transport, config.sender);
    let reporter = Reporter::new(queue.clone(), sender.stats());
    let sending = {
        let watcher = watcher.clone();
        tokio::spawn(async move { sender.run(watcher).await })
    };

    let reporting = tokio::spawn(report_loop(
        reporter,
        intake.clone(),
        config.report_interval,
        watcher.clone(),
    ));

    tracing::info!(
        socket = %config.socket_path.display(),
        data_dir = %config.data_dir.display(),
        signy = %config.signy_url,
        queue_max_bytes = config.queue.max_bytes,
        "collecty is accepting OTLP"
    );

    let stopping = shutdown.clone();
    let served = receive::serve(intake, listener, async move {
        wait_for_a_stop_signal().await;
        tracing::info!("collecty is shutting down");
        let _ = stopping.send(true);
    })
    .await;

    let _ = shutdown.send(true);
    let _ = sending.await;
    let _ = reporting.await;

    // The only `fsync` the queue has is the one that closes a segment, so
    // leaving without closing the open one would leave its records to be
    // recovered rather than simply read.
    if let Err(error) = queue.seal() {
        tracing::error!(%error, "the open segment could not be closed on the way out");
    }
    let _ = std::fs::remove_file(&config.socket_path);

    served.map_err(|error| format!("the OTLP listener stopped: {error}"))
}

async fn wait_for_a_stop_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(error) => {
            tracing::error!(%error, "cannot listen for SIGTERM");
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

async fn report_loop(
    reporter: Reporter,
    intake: Arc<Intake>,
    interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => return,
        }
        let observed = reporter.observe();
        reporter.log(&observed);
        let export = reporter.export(&observed);
        if let Err(status) = intake
            .accept(Signal::Metrics, bytes::Bytes::from(export))
            .await
        {
            tracing::warn!(%status, "collecty could not queue its own metrics");
        }
    }
}
