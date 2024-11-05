use tokio::net::TcpStream;

struct MetricsClient {
    stream: BufWriter<BufReader<TcpStream>>,
}

impl MetricsClient {
    async fn new(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream: BufWriter::new(BufReader::new(stream)),
        })
    }

    async fn record(&mut self, event: HashMap<String, String>) -> Result<MetricsResponse> {
        let command = MetricsCommand::Record(event);
        self.send_command(command).await
    }

    async fn query(&mut self, min: i64, max: i64, options: QueryOptions) -> Result<MetricsResponse> {
        let command = MetricsCommand::Query { min, max, options };
        self.send_command(command).await
    }

    async fn query_days(&mut self, days: i32) -> Result<MetricsResponse> {
        let command = MetricsCommand::QueryDays(days);
        self.send_command(command).await
    }

    async fn send_command(&mut self, command: MetricsCommand) -> Result<MetricsResponse> {
        let cmd_str = serde_json::to_string(&command)? + "\n";
        self.stream.write_all(cmd_str.as_bytes()).await?;
        self.stream.flush().await?;

        let mut response = String::new();
        self.stream.read_line(&mut response).await?;
        Ok(serde_json::from_str(&response)?)
    }
}

// Example client usage:
#[tokio::main]
async fn main() -> Result<()> {
    let mut client = MetricsClient::new("127.0.0.1:8080").await?;

    // Record an event
    let mut event = HashMap::new();
    event.insert("ip".to_string(), "127.0.0.1".to_string());
    event.insert("event".to_string(), "purchase".to_string());
    event.insert("amount".to_string(), "99.99".to_string());

    let response = client.record(event).await?;
    println!("Record response: {:?}", response);

    // Query last 10 days
    let response = client.query_days(10).await?;
    println!("Query response: {:?}", response);

    Ok(())
}
