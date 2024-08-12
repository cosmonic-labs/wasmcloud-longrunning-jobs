//! NATS implementation for wasmcloud:messaging.

mod config;
mod longrunning;
mod operation;

use longrunning::LongRunningOperationProvider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    LongRunningOperationProvider::run().await?;
    eprintln!("Long running operation provider exiting");
    Ok(())
}
