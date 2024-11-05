use tokio::net::{TcpListener, TcpStream};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Serialize, Deserialize)]
enum MetricsCommand {
    Record(HashMap<String, String>),
    Query {
        min: i64,
        max: i64,
        options: QueryOptions,
    },
    QueryDays(i32),
}

#[derive(Serialize, Deserialize)]
struct MetricsResponse {
    success: bool,
    data: Option<serde_json::Value>,
    error: Option<String>,
}

struct MetricsService {
    metrics: Arc<RwLock<Metrics>>,
}

impl MetricsService {
    fn new(config: MetricsConfig) -> Result<Self> {
        let metrics = Metrics::new("./metrics.db", config)?;
        Ok(Self {
            metrics: Arc::new(RwLock::new(metrics)),
        })
    }

    async fn run(self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("Metrics service listening on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New connection from {}", addr);

            let metrics = self.metrics.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, metrics).await {
                    eprintln!("Error handling connection: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    socket: TcpStream,
    metrics: Arc<RwLock<Metrics>>
) -> Result<()> {
    let (reader, writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            break; // Connection closed
        }

        let command: MetricsCommand = serde_json::from_str(&line)?;
        let response = match command {
            MetricsCommand::Record(event) => {
                match metrics.write().await.record(event).await {
                    Ok(_) => MetricsResponse {
                        success: true,
                        data: None,
                        error: None,
                    },
                    Err(e) => MetricsResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    },
                }
            },
            MetricsCommand::Query { min, max, options } => {
                match metrics.read().await.query(min, max, options).await {
                    Ok(result) => MetricsResponse {
                        success: true,
                        data: Some(serde_json::to_value(result)?),
                        error: None,
                    },
                    Err(e) => MetricsResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    },
                }
            },
            MetricsCommand::QueryDays(days) => {
                match metrics.read().await.query_days(days).await {
                    Ok(result) => MetricsResponse {
                        success: true,
                        data: Some(serde_json::to_value(result)?),
                        error: None,
                    },
                    Err(e) => MetricsResponse {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    },
                }
            },
        };

        writer.write_all(serde_json::to_string(&response)?.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    Ok(())
}

// Example usage:
#[tokio::main]
async fn main() -> Result<()> {
    let config = MetricsConfig {
        key: "examples".to_string(),
        map: MetricsMap {
            bmp: vec!["ip".to_string()],
            add: vec![
                "event".to_string(),
                "event~ip".to_string(),
                "event~offer~creative~s1~s2~s3".to_string(),
            ],
            addv: vec![AddValue {
                key: "offer~event".to_string(),
                add_key: "amount".to_string(),
            }],
            top: vec!["geo".to_string(), "offer".to_string()],
        },
    };

    let service = MetricsService::new(config)?;
    service.run("127.0.0.1:8080").await
}

