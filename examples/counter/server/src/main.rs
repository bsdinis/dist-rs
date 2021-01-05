use std::collections::HashMap;
use std::io;
use std::net::ToSocketAddrs;

use argh::FromArgs;
use eyre::Result;

use counter_client_protocol::counter_server::CounterServer;
use counter_server_protocol::raft_server::RaftServer;
use tonic::transport::{Server, Uri};

mod service;
use service::CounterService;

mod machine;
use machine::Counter;

mod conn;
use conn::GrpcConn;

use raft::replica::Replica;

use tracing::{event, Level};

/// Server for counter state machine
#[derive(FromArgs)]
struct Options {
    /// bind addr
    #[argh(positional)]
    addr: String,

    /// initial value
    #[argh(positional)]
    value: i64,

    /// peers
    #[argh(positional)]
    peers_filename: String,

    /// number of tolerated faults
    #[argh(positional)]
    f: usize,

    /// initial value
    #[argh(positional)]
    id: u64,
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

    let peers: HashMap<u64, Uri> = json::parse(&std::fs::read_to_string(options.peers_filename)?)?
        .entries()
        .map(|(k, v)| {
            (
                k.parse::<u64>().unwrap(),
                v.as_str().unwrap().parse::<Uri>().unwrap(),
            )
        })
        .collect();

    let conns = {
        let mut conns = Vec::with_capacity(peers.len());
        for (id, uri) in peers {
            if id != options.id {
                conns.push(GrpcConn::new(id, uri)?);
            }
        }
        conns
    };
    let replica = std::sync::Arc::new(
        Replica::new(conns, options.f, options.id, Counter(options.value)).await,
    );

    let server = Server::builder()
        .add_service(RaftServer::new(CounterService::new(replica.clone())))
        .add_service(CounterServer::new(CounterService::new(replica)))
        .serve_with_shutdown(addr, ctrl_c());

    event!(Level::INFO, "Server listening on {:?}", addr);
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
