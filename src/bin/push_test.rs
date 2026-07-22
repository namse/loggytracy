use prost::Message;
use std::io::{Read, Write};
use std::net::TcpStream;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<StreamAdapter>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamAdapter {
    #[prost(string, tag = "1")]
    pub labels: String,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<EntryAdapter>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<::prost_types::Timestamp>,
    #[prost(string, tag = "2")]
    pub line: String,
}

fn main() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();

    let req = PushRequest {
        streams: vec![
            StreamAdapter {
                labels: r#"{app="test-app", host="local"}"#.to_string(),
                entries: vec![
                    EntryAdapter {
                        timestamp: Some(::prost_types::Timestamp {
                            seconds: now.as_secs() as i64 - 60,
                            nanos: 0,
                        }),
                        line: "hello world from loggytracy test".to_string(),
                    },
                    EntryAdapter {
                        timestamp: Some(::prost_types::Timestamp {
                            seconds: now.as_secs() as i64 - 30,
                            nanos: 0,
                        }),
                        line: "second log line with error keyword".to_string(),
                    },
                    EntryAdapter {
                        timestamp: Some(::prost_types::Timestamp {
                            seconds: now.as_secs() as i64,
                            nanos: 0,
                        }),
                        line: "third line all good".to_string(),
                    },
                ],
            },
            StreamAdapter {
                labels: r#"{app="other-app", env="prod"}"#.to_string(),
                entries: vec![EntryAdapter {
                    timestamp: Some(::prost_types::Timestamp {
                        seconds: now.as_secs() as i64 - 10,
                        nanos: 0,
                    }),
                    line: "log from other app".to_string(),
                }],
            },
        ],
    };

    let mut buf = Vec::with_capacity(req.encoded_len());
    req.encode(&mut buf).unwrap();

    let mut compressor = snap::raw::Encoder::new();
    let compressed = compressor.compress_vec(&buf).unwrap();

    let http_req = format!(
        "POST /loki/api/v1/push HTTP/1.1\r\nHost: localhost:3100\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        compressed.len()
    );

    let mut stream = TcpStream::connect("127.0.0.1:3100").unwrap();
    stream.write_all(http_req.as_bytes()).unwrap();
    stream.write_all(&compressed).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    println!("Push response: {}", response.lines().next().unwrap_or(""));

    if response.contains("204") {
        println!("Push OK (204 No Content)");
    } else {
        println!("Push FAILED");
        println!("{}", response);
        std::process::exit(1);
    }
}
