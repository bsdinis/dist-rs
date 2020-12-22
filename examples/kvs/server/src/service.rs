use kvs_protocol::key_value_store_server::KeyValueStore;
use kvs_protocol::*;
use tonic::{Request, Response, Status};

use abd::store::{LocalStorage, Timestamp, Timestamped};
use dashmap::DashMap;

use tracing::{event, Level};

pub struct MapStore(DashMap<u64, Timestamped<Option<Vec<u8>>, u64, u64>>);

impl MapStore {
    pub fn new() -> Self {
        MapStore(DashMap::new())
    }

    fn inner(&self) -> &DashMap<u64, Timestamped<Option<Vec<u8>>, u64, u64>> {
        &self.0
    }
}

impl LocalStorage for MapStore {
    type Key = u64;
    type Item = Option<Vec<u8>>;
    type Ts = u64;
    type ClientId = u64;
    type Error = ();

    fn write(
        &self,
        k: &Self::Key,
        v: Self::Item,
        ts: Self::Ts,
        cid: Self::ClientId,
    ) -> Result<(), Self::Error> {
        let handle = self.inner().get_mut(k);
        let new_ts = Timestamp(ts, cid);
        if handle.is_none() {
            self.inner().insert(*k, Timestamped { ts: new_ts, val: v });
        } else {
            let mut handle = handle.unwrap();
            if handle.ts < new_ts {
                *handle = Timestamped { ts: new_ts, val: v };
            }
        }

        Ok(())
    }

    fn read(
        &self,
        k: &Self::Key,
    ) -> Result<Option<Timestamped<Self::Item, Self::Ts, Self::ClientId>>, Self::Error> {
        Ok(self.inner().get(k).map(|view| view.value().clone()))
    }
}

#[tonic::async_trait]
impl KeyValueStore for MapStore {
    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetResp>, Status> {
        let v = self
            .read(&request.get_ref().key)
            .map_err(|_| Status::internal("failed to retrieve from local storage"))?;
        if let Some(ts_val) = v {
            if let Some(val) = ts_val.val {
                event!(
                    Level::INFO,
                    "[get {}] {:?} (v{}.{})",
                    request.get_ref().key,
                    val,
                    ts_val.ts.0,
                    ts_val.ts.1
                );
                Ok(Response::new(GetResp {
                    val: Some(Val { val }),
                    ts: Some(Ts {
                        ts: ts_val.ts.0,
                        cid: ts_val.ts.1,
                    }),
                }))
            } else {
                event!(
                    Level::INFO,
                    "[get {}] [no value] (v{}.{})",
                    request.get_ref().key,
                    ts_val.ts.0,
                    ts_val.ts.1
                );
                Ok(Response::new(GetResp {
                    val: None,
                    ts: Some(Ts {
                        ts: ts_val.ts.0,
                        cid: ts_val.ts.1,
                    }),
                }))
            }
        } else {
            event!(
                Level::INFO,
                "[get {}] [key does not exist]",
                request.get_ref().key,
            );
            Ok(Response::new(GetResp {
                val: None,
                ts: None,
            }))
        }
    }
    async fn put(&self, request: Request<PutReq>) -> Result<Response<PutResp>, Status> {
        let val = request.get_ref().val.as_ref().map(|v| &v.val);
        let ts = request
            .get_ref()
            .ts
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("the timestamp cannot be null"))?;
        event!(
            Level::INFO,
            "[put {}] {:?} (v{}.{})",
            request.get_ref().key,
            val,
            ts.ts,
            ts.cid
        );
        self.write(&request.get_ref().key, val.cloned(), ts.ts, ts.cid)
            .map_err(|_| Status::internal("failed to write to local storage"))
            .map(|_| Response::new(PutResp {}))
    }
}
