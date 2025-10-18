use fagent::run_cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_cli().await
}
