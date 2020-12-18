use eyre::Result;
use protos::client_api_client::ClientApiClient;
use structopt::StructOpt;
use tonic::transport::{Channel, Uri};

#[structopt(name = "clnt-state-machine")]
#[derive(StructOpt)]
struct Opt {
    /// location of the server
    #[structopt(name = "server", long)]
    server_addr: Uri,

    /// operation
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(StructOpt)]
enum Command {
    Get,
    Incr { step: u64 },
    Decr { step: u64 },
    AtomicIncr { before: u64, step: u64 },
    AtomicDecr { before: u64, step: u64 },
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let opt = Opt::from_args();

    let remote = ClientApiClient::new(Channel::builder(opt.server_addr).connect_lazy()?);

    match opt.cmd {
        Command::Get => do_get(remote).await,
        Command::Incr { step } => do_incr(remote, step).await,
        Command::Decr { step } => do_decr(remote, step).await,
        Command::AtomicIncr { before, step } => do_atomic_incr(remote, before, step).await,
        Command::AtomicDecr { before, step } => do_atomic_decr(remote, before, step).await,
    }
}

async fn do_get(mut rem: ClientApiClient<Channel>) -> Result<()> {
    println!("executing get()");
    let res = rem.get(protos::GetCounterReq {}).await?.get_ref().cur;
    println!("counter value = {}", res);
    Ok(())
}

async fn do_incr(mut rem: ClientApiClient<Channel>, step: u64) -> Result<()> {
    println!("executing incr({})", step);
    let res = rem
        .incr(protos::IncrCounterReq { step })
        .await?
        .get_ref()
        .cur;
    println!("counter value = {}", res);
    Ok(())
}

async fn do_decr(mut rem: ClientApiClient<Channel>, step: u64) -> Result<()> {
    println!("executing decr({})", step);
    let res = rem
        .decr(protos::DecrCounterReq { step })
        .await?
        .get_ref()
        .cur;
    println!("counter value = {}", res);
    Ok(())
}

async fn do_atomic_incr(mut rem: ClientApiClient<Channel>, before: u64, step: u64) -> Result<()> {
    println!(
        "executing atomic_incr(before = {}, step = {})",
        before, step
    );
    match rem
        .atomic_incr(protos::AtomicIncrCounterReq { before, step })
        .await?
        .get_ref()
    {
        protos::AtomicIncrCounterResp { cur, success: true } => {
            println!("counter value = {} [incremented successfully]", cur)
        }
        protos::AtomicIncrCounterResp {
            cur,
            success: false,
        } => println!("counter value = {} [failed to increment]", cur),
    };
    Ok(())
}

async fn do_atomic_decr(mut rem: ClientApiClient<Channel>, before: u64, step: u64) -> Result<()> {
    println!(
        "executing atomic_decr(before = {}, step = {})",
        before, step
    );
    match rem
        .atomic_decr(protos::AtomicDecrCounterReq { before, step })
        .await?
        .get_ref()
    {
        protos::AtomicDecrCounterResp { cur, success: true } => {
            println!("counter value = {} [decremented successfully]", cur)
        }
        protos::AtomicDecrCounterResp {
            cur,
            success: false,
        } => println!("counter value = {} [failed to decrement]", cur),
    };
    Ok(())
}
