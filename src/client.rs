use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::TcpStream;

use crate::server::Response;

/// TCP client for the traqq server.
/// sends newline-delimited JSON commands and reads JSON responses.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl Client {
    /// connect to a traqq server at the given address
    pub fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);
        Ok(Self { reader, writer })
    }

    /// record an event (pass raw JSON object)
    pub fn record(&mut self, event: serde_json::Value) -> Result<Response, String> {
        let cmd = serde_json::json!({
            "cmd": "record",
            "event": event,
        });
        self.send(cmd)
    }

    /// query a time range
    pub fn query(&mut self, min: i64, max: i64) -> Result<Response, String> {
        let cmd = serde_json::json!({
            "cmd": "query",
            "min": min,
            "max": max,
        });
        self.send(cmd)
    }

    /// query the last N days
    pub fn query_days(&mut self, days: i32) -> Result<Response, String> {
        let cmd = serde_json::json!({
            "cmd": "query_days",
            "days": days,
        });
        self.send(cmd)
    }

    /// query and find specific metrics
    pub fn find(
        &mut self,
        min: i64,
        max: i64,
        metric_type: &str,
        key: &str,
        add_key: Option<&str>,
        merge: bool,
    ) -> Result<Response, String> {
        let mut cmd = serde_json::json!({
            "cmd": "find",
            "min": min,
            "max": max,
            "metric_type": metric_type,
            "key": key,
            "merge": merge,
        });
        if let Some(ak) = add_key {
            cmd["add_key"] = serde_json::json!(ak);
        }
        self.send(cmd)
    }

    fn send(&mut self, cmd: serde_json::Value) -> Result<Response, String> {
        let line = serde_json::to_string(&cmd).map_err(|e| e.to_string())?;

        self.writer
            .write_all(line.as_bytes())
            .map_err(|e| e.to_string())?;
        self.writer.write_all(b"\n").map_err(|e| e.to_string())?;
        self.writer.flush().map_err(|e| e.to_string())?;

        let mut response_line = String::new();
        self.reader
            .read_line(&mut response_line)
            .map_err(|e| e.to_string())?;

        serde_json::from_str(&response_line).map_err(|e| format!("invalid response: {}", e))
    }
}
