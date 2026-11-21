#[tokio::main]
async fn main() -> anyhow::Result<()> {
    control_backend::bootstrap::run().await
}
