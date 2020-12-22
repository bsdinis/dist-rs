use std::io;
use std::net::ToSocketAddrs;

use argh::FromArgs;
use eyre::Result;

use kvs_protocol::key_value_store_server::KeyValueStoreServer;
use tonic::transport::Server;

mod service;
use service::MapStore;

use tracing::{event, Level};

/// Server for counter state machine
#[derive(FromArgs)]
struct Options {
    /// bind addr
    #[argh(positional)]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let options: Options = argh::from_env();

    let addr = options
        .addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))?;

    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stderr());
    let subscriber = tracing_subscriber::fmt().with_writer(non_blocking).finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Unable to set global default subscriber");

    let server = Server::builder()
        .add_service(KeyValueStoreServer::new(MapStore::new()))
        .serve_with_shutdown(addr, ctrl_c());

    event!(Level::INFO, "Sever listening on {:?}", addr);
    server.await?;

    println!("Bye!");
    Ok(())
}

async fn ctrl_c() {
    use std::future;

    if let Err(_) = tokio::signal::ctrl_c().await {
        eprintln!("Failed to listen for Ctrl+C/SIGINT. Server will still exit after receiving them, just not gracefully.");
        future::pending().await // never completes
    }
}
