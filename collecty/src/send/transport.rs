use std::time::Duration;

use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

use super::{DeliverFuture, Outcome, Shipment, Transport};

/// Which collecty the segment came from, and which segment it is. Together
/// they are what signy needs to skip what it already stored, so a resend after
/// a crash costs bandwidth and nothing else.
pub const SENDER_HEADER: &str = "x-collecty-sender";
pub const SEGMENT_HEADER: &str = "x-collecty-segment";
const REASON_LIMIT: usize = 512;

pub struct HttpTransport {
    client: Client<HttpConnector, Full<Bytes>>,
    base: String,
    timeout: Duration,
}

impl HttpTransport {
    pub fn new(base: impl Into<String>, timeout: Duration) -> HttpTransport {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        HttpTransport {
            client: Client::builder(TokioExecutor::new()).build(connector),
            base: base.into().trim_end_matches('/').to_string(),
            timeout,
        }
    }

    pub fn route(&self) -> String {
        format!("{}/signy/api/v1/collect", self.base)
    }
}

impl Transport for HttpTransport {
    fn deliver<'a>(&'a self, shipment: Shipment) -> DeliverFuture<'a> {
        Box::pin(async move {
            let uri = self.route();
            let request = match Request::builder()
                .method(Method::POST)
                .uri(&uri)
                .header(CONTENT_TYPE, "application/x-protobuf")
                .header(CONTENT_ENCODING, "zstd")
                .header(SENDER_HEADER, shipment.sender.to_string())
                .header(SEGMENT_HEADER, shipment.segment.to_string())
                .body(Full::new(shipment.body))
            {
                Ok(request) => request,
                Err(error) => {
                    return Outcome::Refused(format!("cannot build a request for {uri}: {error}"));
                }
            };

            let response =
                match tokio::time::timeout(self.timeout, self.client.request(request)).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => return Outcome::Retry(format!("{uri}: {error}")),
                    Err(_) => {
                        return Outcome::Retry(format!(
                            "{uri}: no answer within {}ms",
                            self.timeout.as_millis()
                        ));
                    }
                };

            let status = response.status();
            let explanation = response
                .into_body()
                .collect()
                .await
                .map(|collected| {
                    let text = String::from_utf8_lossy(&collected.to_bytes())
                        .trim()
                        .to_string();
                    text.chars().take(REASON_LIMIT).collect::<String>()
                })
                .unwrap_or_default();

            if status.is_success() {
                return Outcome::Accepted(stored_number(&explanation));
            }

            let reason = format!("{uri}: {status} {explanation}");

            if refuses_the_payload(status) {
                Outcome::Refused(reason)
            } else {
                Outcome::Retry(reason)
            }
        })
    }
}

/// The segment signy says it now holds whole, out of `{"stored":n}`.
///
/// Zero for an answer that does not say, which leaves the sender committing
/// only the segment it just sent.
fn stored_number(body: &str) -> u64 {
    let Some(at) = body.find("\"stored\"") else {
        return 0;
    };
    let rest = &body[at + "\"stored\"".len()..];
    let Some(colon) = rest.find(':') else {
        return 0;
    };
    rest[colon + 1..]
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

fn refuses_the_payload(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::PAYLOAD_TOO_LARGE
            | StatusCode::UNSUPPORTED_MEDIA_TYPE
            | StatusCode::UNPROCESSABLE_ENTITY
    )
}
