use tokio::net::TcpListener;

struct MetricsService {
    metrics: Arc<RwLock<Metrics>>,
}

impl MetricsService {
    async fn run(addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        let metrics = Arc::new(RwLock::new(Metrics::new("./db")?));

        while let Ok((socket, _)) = listener.accept().await {
            let metrics = metrics.clone();
            tokio::spawn(async move {
                handle_connection(socket, metrics).await
            });
        }
        Ok(())
    }
}

