use async_trait::async_trait;
use eyre::Result;
use kvs_protocol::key_value_store_client::KeyValueStoreClient;
use kvs_protocol::*;
use structopt::StructOpt;
use tonic::transport::{Channel, Uri};
use tonic::Status;

#[structopt(name = "kvs-client")]
#[derive(StructOpt)]
struct Opt {
    /// location of the server
    #[structopt(name = "server", short)]
    servers_addr: Vec<Uri>,

    #[structopt(name = "faults", short, long)]
    f: usize,

    #[structopt(name = "id", short, long)]
    client_id: u64,

    /// operation
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(StructOpt)]
enum Command {
    Get { key: u64 },
    Put { key: u64, val: String },
    Rem { key: u64 },
}

struct GrpcConn {
    clnt: KeyValueStoreClient<Channel>,
}

impl GrpcConn {
    fn new(clnt: KeyValueStoreClient<Channel>) -> Self {
        GrpcConn { clnt }
    }
}

#[async_trait]
impl abd::Conn for GrpcConn {
    type Key = u64;
    type Item = Option<Vec<u8>>;
    type Ts = u64;
    type ClientId = u64;
    type Error = Status;

    async fn write(
        &mut self,
        k: &Self::Key,
        v: Self::Item,
        ts: Self::Ts,
        cid: Self::ClientId,
    ) -> Result<(), Self::Error> {
        let val = v.map(|val| Val { val });
        let ts = Some(Ts { ts, cid });
        self.clnt
            .put(kvs_protocol::PutReq { key: *k, val, ts })
            .await?;
        Ok(())
    }
    async fn read(
        &mut self,
        k: &Self::Key,
    ) -> Result<Option<abd::store::Timestamped<Self::Item, Self::Ts, Self::ClientId>>, Self::Error>
    {
        let resp = self.clnt.get(kvs_protocol::GetReq { key: *k }).await?;
        let ts = resp.get_ref().ts.clone();
        Ok(match ts {
            Some(Ts { ts, cid }) => Some(abd::store::Timestamped::new(
                resp.get_ref().val.clone().map(|v| v.val),
                ts,
                cid,
            )),
            None => None,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let opt = Opt::from_args();

    let remotes: Result<Vec<GrpcConn>, tonic::transport::Error> = opt
        .servers_addr
        .into_iter()
        .map(|addr| {
            Channel::builder(addr)
                .connect_lazy()
                .map(|ch| GrpcConn::new(KeyValueStoreClient::new(ch)))
        })
        .collect();

    let clnt = abd::Client::new(remotes?, opt.f);

    match opt.cmd {
        Command::Get { key } => do_get(clnt, key).await,
        Command::Put { key, val } => do_put(clnt, key, val.into_bytes(), opt.client_id).await,
        Command::Rem { key } => do_rem(clnt, key, opt.client_id).await,
    }
}

async fn do_get(clnt: abd::Client<GrpcConn>, key: u64) -> Result<()> {
    println!("executing get({})", key);
    let res = clnt.read(&key).await?;
    if let Some(v) = res {
        println!("{} => {:?}", key, v);
    } else {
        println!("{} => no value", key);
    }

    Ok(())
}

async fn do_put(clnt: abd::Client<GrpcConn>, key: u64, val: Vec<u8>, cid: u64) -> Result<()> {
    println!("executing put({}, {:?}) [id = {}]", key, val, cid);
    clnt.write(&key, Some(val), cid, |ts| ts + 1).await?;
    Ok(())
}

async fn do_rem(clnt: abd::Client<GrpcConn>, key: u64, cid: u64) -> Result<()> {
    println!("executing rem({}) [id = {}]", key, cid);
    clnt.write(&key, None, cid, |ts| ts + 1).await?;
    Ok(())
}
