use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use serde::{Deserialize, Serialize};

use crate::{FindOptions, IncomingEvent, Traqq};

/// commands the server accepts, one per line as JSON
#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    Record {
        event: serde_json::Value,
    },
    Query {
        min: i64,
        max: i64,
    },
    QueryDays {
        days: i32,
    },
    Find {
        min: i64,
        max: i64,
        metric_type: String,
        key: String,
        #[serde(default)]
        add_key: Option<String>,
        #[serde(default)]
        merge: bool,
    },
}

/// response sent back per command
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    fn ok(data: serde_json::Value) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn ok_empty() -> Self {
        Self {
            success: true,
            data: None,
            error: None,
        }
    }

    fn err(msg: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg),
        }
    }
}

/// run the traqq TCP server on the given address.
/// blocks the calling thread.
pub fn run(traqq: Arc<Traqq>, addr: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("traqq server listening on {}", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let traqq = Arc::clone(&traqq);
                thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, &traqq) {
                        eprintln!("connection error: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }

    Ok(())
}

fn handle_connection(stream: TcpStream, traqq: &Traqq) -> std::io::Result<()> {
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<Command>(&line) {
            Ok(cmd) => dispatch(cmd, traqq),
            Err(e) => Response::err(format!("invalid command: {}", e)),
        };

        let json = serde_json::to_string(&response)
            .unwrap_or_else(|e| format!("{{\"success\":false,\"error\":\"{}\"}}", e));
        writer.write_all(json.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }

    Ok(())
}

fn dispatch(cmd: Command, traqq: &Traqq) -> Response {
    match cmd {
        Command::Record { event } => match IncomingEvent::from_json(event) {
            Ok(incoming) => match traqq.record(incoming) {
                Ok(_) => Response::ok_empty(),
                Err(e) => Response::err(e),
            },
            Err(e) => Response::err(e),
        },
        Command::Query { min, max } => match traqq.query(min, max) {
            Ok(result) => match serde_json::to_value(&result.days) {
                Ok(v) => Response::ok(v),
                Err(e) => Response::err(e.to_string()),
            },
            Err(e) => Response::err(e),
        },
        Command::QueryDays { days } => match traqq.query_days(days) {
            Ok(result) => match serde_json::to_value(&result.days) {
                Ok(v) => Response::ok(v),
                Err(e) => Response::err(e.to_string()),
            },
            Err(e) => Response::err(e),
        },
        Command::Find {
            min,
            max,
            metric_type,
            key,
            add_key,
            merge,
        } => match traqq.query(min, max) {
            Ok(result) => {
                let found = result.find(FindOptions {
                    metric_type,
                    key,
                    add_key,
                    merge,
                });
                match serde_json::to_value(&found) {
                    Ok(v) => Response::ok(v),
                    Err(e) => Response::err(e.to_string()),
                }
            }
            Err(e) => Response::err(e),
        },
    }
}
