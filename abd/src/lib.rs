/// ABD protocol implementation
pub mod store;
use async_trait::async_trait;
use futures::future::join_all;
use futures::select;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Mutex;

/// Protocol errors
///
/// This error is generic over the connection error
/// This allows that when a quorum is unavailable *because* there were failures in the connection,
/// that that information is piped to the client
#[derive(Debug)]
pub enum Error<ConnError: std::error::Error> {
    UnavailableQuorum {
        available_nodes: usize,
        quorum_sz: usize,
        conn_errors: Vec<ConnError>,
    },
}

impl<CE> std::error::Error for Error<CE> where CE: std::error::Error {}

impl<CE> std::fmt::Display for Error<CE>
where
    CE: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::UnavailableQuorum {
                available_nodes,
                quorum_sz,
                conn_errors,
            } => {
                let mut backtrace = conn_errors
                    .iter()
                    .fold(String::new(), |a, b| a + &format!("\n{}", b));
                if conn_errors.len() > 0 {
                    backtrace = "\nBacktrace:".to_string() + &backtrace;
                }
                write!(
                    f,
                    "quorum is unavailable: {} online nodes vs. {} required nodes{}",
                    available_nodes, quorum_sz, backtrace
                )
            }
        }
    }
}

/// Model of the connection
///
/// This behaves as a shim remote storage
#[async_trait]
pub trait Conn {
    type Key: Clone;
    type Item: Clone;
    type Ts: Clone + Ord + store::Min + Eq;
    type ClientId: Clone + Ord + store::Min + Eq;
    type Error: std::error::Error;

    /// write an element to the remote storage
    async fn write(
        &mut self,
        k: &Self::Key,
        v: Self::Item,
        ts: Self::Ts,
        cid: Self::ClientId,
    ) -> Result<(), Self::Error>;

    /// read an element from the remote storage
    async fn read(
        &mut self,
        k: &Self::Key,
    ) -> Result<Option<store::Timestamped<Self::Item, Self::Ts, Self::ClientId>>, Self::Error>;

    /// whether a node is reachable or not
    fn is_connected(&self) -> bool {
        true
    }
}

/// The type which implements the protocol
///
/// It simply holds the connections and the fault tolerance parameter
pub struct Client<C: Conn> {
    conns: Vec<Mutex<C>>,
    f: usize,
}

impl<C> Client<C>
where
    C: Conn,
{
    /// Create a new client.
    pub fn new(conns: Vec<C>, f: usize) -> Self {
        Client {
            conns: conns.into_iter().map(|c| Mutex::new(c)).collect(),
            f,
        }
    }

    /// Number of replicas in the replication set
    pub fn n(&self) -> usize {
        2 * self.f + 1
    }

    /// Size of a write quorum
    pub fn write_quorum(&self) -> usize {
        self.f + 1
    }

    /// Size of a read quorum
    pub fn read_quorum(&self) -> usize {
        self.f + 1
    }

    /// Number of unavailable nodes
    fn available_nodes(&self) -> usize {
        self.conns
            .iter()
            .filter(|x| x.lock().unwrap().is_connected())
            .count()
    }

    /// Instanciate a guard over the connections
    ///
    /// When operating over the connections, it is important that the guards to their inner
    /// elements remain binded to a variable. This utility method provides a shorthand for creating
    /// guards to all connections.
    fn guard(&self) -> Vec<std::sync::MutexGuard<'_, C>> {
        let guards: Vec<_> = self.conns.iter().map(|x| x.lock().unwrap()).collect();
        guards
    }

    /// Whether the client is fully available
    pub fn is_available(&self) -> bool {
        self.is_write_available() && self.is_read_available()
    }
    /// Whether the client is available for writes
    pub fn is_write_available(&self) -> bool {
        self.available_nodes() >= self.write_quorum()
    }
    /// Whether the client is available for reads
    pub fn is_read_available(&self) -> bool {
        self.available_nodes() >= self.read_quorum()
    }

    /// Perform write to a distributed register
    pub async fn write<F>(
        &self,
        k: &C::Key,
        v: C::Item,
        cid: C::ClientId,
        update_ts: F,
    ) -> Result<(), Error<C::Error>>
    where
        F: Fn(&C::Ts) -> C::Ts,
    {
        if !self.is_write_available() {
            return Err(Error::UnavailableQuorum {
                available_nodes: self.available_nodes(),
                quorum_sz: self.write_quorum(),
                conn_errors: vec![],
            });
        }

        let mut replies = Vec::with_capacity(self.available_nodes());
        let mut conn_errors = Vec::with_capacity(self.available_nodes());
        {
            let mut guard = self.guard();
            let mut futs: FuturesUnordered<_> = guard
                .iter_mut()
                .filter(|c| c.is_connected())
                .map(|c| c.read(&k))
                .collect();
            loop {
                select! {
                    res = futs.select_next_some() => {
                        match  res {
                            Ok(v) => replies.push(v),
                            Err(e) => conn_errors.push(e),
                        }
                        if replies.len() + conn_errors.len() > self.n() - self.f {
                            break;
                        }
                    }

                    complete => break,
                }
            }
        }
        if replies.len() < self.read_quorum() {
            return Err(Error::UnavailableQuorum {
                available_nodes: replies.len(),
                quorum_sz: self.read_quorum(),
                conn_errors,
            });
        }

        let min_ts = store::Min::min();
        let max_ts = replies
            .iter()
            .map(|x| match x {
                Some(val) => Some(&val.ts.0),
                _ => None,
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .max()
            .or(Some(&min_ts))
            .unwrap();

        let new_ts = update_ts(max_ts);

        let mut recvd_replies = 0;
        let mut ok_replies = 0;
        let mut conn_errors = Vec::with_capacity(self.available_nodes());

        {
            let mut guard = self.guard();
            let mut futs: FuturesUnordered<_> = guard
                .iter_mut()
                .filter(|c| c.is_connected())
                .map(|c| c.write(k, v.clone(), new_ts.clone(), cid.clone()))
                .collect();
            loop {
                select! {
                    res = futs.select_next_some() => {
                        recvd_replies += 1;
                        match res {
                            Ok(_) => ok_replies += 1,
                            Err(e) => conn_errors.push(e),
                        }

                        if recvd_replies >= self.n() - self.f {
                            break;
                        }
                    }

                    complete => break,
                }
            }
        }

        if ok_replies >= self.write_quorum() {
            Ok(())
        } else {
            return Err(Error::UnavailableQuorum {
                available_nodes: recvd_replies,
                quorum_sz: self.write_quorum(),
                conn_errors,
            });
        }
    }

    /// Perform read from a distributed register
    pub async fn read(&self, k: &C::Key) -> Result<Option<C::Item>, Error<C::Error>> {
        if !self.is_read_available() {
            return Err(Error::UnavailableQuorum {
                available_nodes: self.available_nodes(),
                quorum_sz: self.read_quorum(),
                conn_errors: vec![],
            });
        }

        let mut replies = Vec::with_capacity(self.available_nodes());
        let mut conn_errors = Vec::with_capacity(self.available_nodes());
        let mut n_recvd = 0;

        {
            let mut guard = self.guard();
            let mut futs: FuturesUnordered<_> = guard
                .iter_mut()
                .enumerate()
                .filter(|(_, c)| c.is_connected())
                .map(|(i, c)| async move { (i, c.read(&k).await) })
                .collect();

            loop {
                select! {
                    (idx, res) = futs.select_next_some() => {
                        n_recvd+=1;
                        match res {
                            Ok(v) => replies.push((idx, v)),
                            Err(e) => conn_errors.push(e),
                        }
                        if n_recvd >= self.n() - self.f {
                            break;
                        }
                    }

                    complete => break,
                }
            }
        }

        if replies.len() < self.read_quorum() {
            return Err(Error::UnavailableQuorum {
                available_nodes: replies.len(),
                quorum_sz: self.read_quorum(),
                conn_errors,
            });
        }

        if replies.iter().map(|(_, v)| v).all(|v| v.is_none()) {
            return Ok(None);
        }

        let values: Vec<(_, store::Timestamped<_, _, _>)> = replies
            .into_iter()
            .filter(|(_, x)| x.is_some())
            .map(|(i, x)| (i, x.unwrap()))
            .collect();

        let min_ts = store::Timestamp(store::Min::min(), store::Min::min());
        let max_ts = values
            .iter()
            .map(|(_, v)| &v.ts)
            .max()
            .or(Some(&min_ts))
            .unwrap();

        // extract the value from the timestamped value
        let max_val = values
            .iter()
            .filter(|(_, v)| &v.ts == max_ts)
            .next()
            .map(|(_, v)| v.val.clone());

        if max_val.is_none() {
            return Ok(None);
        }

        // write back
        let max_val = max_val.unwrap();
        let mut guard = self.guard();
        let to_rewrite: Vec<_> = values
            .iter()
            .filter(|(_, v)| &v.ts != max_ts)
            .map(|(i, _)| i)
            .collect();
        let mut futs: FuturesUnordered<_> = guard
            .iter_mut()
            .enumerate()
            .filter(|(i, c)| c.is_connected() && to_rewrite.iter().find(|idx| idx == &&i).is_some())
            .map(|(_, c)| c.write(k, max_val.clone(), max_ts.0.clone(), max_ts.1.clone()))
            .collect();
        join_all(futs.iter_mut()).await;

        Ok(Some(max_val))
    }
}

#[cfg(test)]
mod tests {
    use super::store::LocalStorage;
    use super::*;
    use dashmap::DashMap;

    #[derive(Debug)]
    struct ConnError {}
    impl std::error::Error for ConnError {}
    impl std::fmt::Display for ConnError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "dummy error")
        }
    }

    struct MapStore(DashMap<u64, store::Timestamped<u64, usize, usize>>);

    impl MapStore {
        fn new() -> Self {
            MapStore(DashMap::new())
        }

        fn inner(&self) -> &DashMap<u64, store::Timestamped<u64, usize, usize>> {
            &self.0
        }
    }

    impl LocalStorage for MapStore {
        type Key = u64;
        type Item = u64;
        type Ts = usize;
        type ClientId = usize;
        type Error = ConnError;

        fn write(
            &self,
            k: &Self::Key,
            v: Self::Item,
            ts: Self::Ts,
            cid: Self::ClientId,
        ) -> Result<(), Self::Error> {
            let handle = self.inner().get_mut(k);
            let ts = store::Timestamp(ts, cid);
            if handle.is_none() {
                self.inner().insert(*k, store::Timestamped { ts, val: v });
            } else {
                let mut handle = handle.unwrap();
                if handle.ts < ts {
                    *handle = store::Timestamped { ts, val: v };
                }
            }

            Ok(())
        }

        fn read(
            &self,
            k: &Self::Key,
        ) -> Result<Option<store::Timestamped<Self::Item, Self::Ts, Self::ClientId>>, Self::Error>
        {
            Ok(self.inner().get(k).map(|view| view.value().clone()))
        }
    }

    struct DummyConn {
        store: MapStore,
    }

    impl DummyConn {
        fn new() -> Self {
            DummyConn {
                store: MapStore::new(),
            }
        }
    }

    #[async_trait]
    impl Conn for DummyConn {
        type Key = u64;
        type Item = u64;
        type Ts = usize;
        type ClientId = usize;
        type Error = ConnError;

        async fn write(
            &mut self,
            k: &Self::Key,
            v: Self::Item,
            ts: Self::Ts,
            cid: Self::ClientId,
        ) -> Result<(), Self::Error> {
            self.store.write(k, v, ts, cid)
        }
        async fn read(
            &mut self,
            k: &Self::Key,
        ) -> Result<Option<store::Timestamped<Self::Item, Self::Ts, Self::ClientId>>, Self::Error>
        {
            self.store.read(k)
        }
    }

    #[tokio::test]
    async fn dummy_conn_test() {
        let mut conn = DummyConn::new();
        assert_eq!(conn.read(&0).await.unwrap(), None);
        assert_eq!(conn.read(&1).await.unwrap(), None);
        assert_eq!(conn.read(&2).await.unwrap(), None);
        assert_eq!(conn.read(&3).await.unwrap(), None);
        assert_eq!(conn.write(&1, 12, 1, 0).await.unwrap(), ());
        assert_eq!(
            conn.read(&1).await.unwrap(),
            Some(store::Timestamped {
                ts: store::Timestamp(1, 0),
                val: 12
            })
        );
        assert_eq!(conn.write(&1, 42, 2, 2).await.unwrap(), ());
        assert_eq!(
            conn.read(&1).await.unwrap(),
            Some(store::Timestamped {
                ts: store::Timestamp(2, 2),
                val: 42
            })
        );

        assert_eq!(conn.write(&1, 15, 3, 0).await.unwrap(), ());
        assert_eq!(conn.write(&2, 1, 1, 0).await.unwrap(), ());
        assert_eq!(conn.write(&3, 5, 1, 0).await.unwrap(), ());
        assert_eq!(conn.write(&4, 9, 1, 0).await.unwrap(), ());

        assert_eq!(
            conn.read(&1).await.unwrap(),
            Some(store::Timestamped {
                ts: store::Timestamp(3, 0),
                val: 15
            })
        );
        assert_eq!(
            conn.read(&2).await.unwrap(),
            Some(store::Timestamped {
                ts: store::Timestamp(1, 0),
                val: 1
            })
        );
        assert_eq!(
            conn.read(&3).await.unwrap(),
            Some(store::Timestamped {
                ts: store::Timestamp(1, 0),
                val: 5
            })
        );
        assert_eq!(
            conn.read(&4).await.unwrap(),
            Some(store::Timestamped {
                ts: store::Timestamp(1, 0),
                val: 9
            })
        );
        assert_eq!(conn.read(&5).await.unwrap(), None);
    }

    #[tokio::test]
    async fn seq_test() {
        let clnt = Client::new(
            vec![DummyConn::new(), DummyConn::new(), DummyConn::new()],
            1,
        );
        assert_eq!(clnt.read(&0).await.unwrap(), None);
        assert_eq!(clnt.read(&1).await.unwrap(), None);
        assert_eq!(clnt.read(&2).await.unwrap(), None);
        assert_eq!(clnt.read(&3).await.unwrap(), None);
        assert_eq!(clnt.write(&1, 12, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.read(&1).await.unwrap(), Some(12));
        assert_eq!(clnt.write(&1, 42, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.read(&1).await.unwrap(), Some(42));

        assert_eq!(clnt.write(&1, 15, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.write(&2, 1, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.write(&3, 5, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.write(&4, 9, 0, |ts| ts + 1).await.unwrap(), ());

        assert_eq!(clnt.read(&1).await.unwrap(), Some(15));
        assert_eq!(clnt.read(&2).await.unwrap(), Some(1));
        assert_eq!(clnt.read(&3).await.unwrap(), Some(5));
        assert_eq!(clnt.read(&4).await.unwrap(), Some(9));
        assert_eq!(clnt.read(&5).await.unwrap(), None);
    }

    #[tokio::test]
    async fn seq_unavailable_test() {
        let clnt = Client::new(
            vec![DummyConn::new(), DummyConn::new(), DummyConn::new()],
            2,
        );
        assert_eq!(clnt.read(&0).await.unwrap(), None);
        assert_eq!(clnt.read(&1).await.unwrap(), None);
        assert_eq!(clnt.read(&2).await.unwrap(), None);
        assert_eq!(clnt.read(&3).await.unwrap(), None);
        assert_eq!(clnt.write(&1, 12, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.read(&1).await.unwrap(), Some(12));
        assert_eq!(clnt.write(&1, 42, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.read(&1).await.unwrap(), Some(42));

        assert_eq!(clnt.write(&1, 15, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.write(&2, 1, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.write(&3, 5, 0, |ts| ts + 1).await.unwrap(), ());
        assert_eq!(clnt.write(&4, 9, 0, |ts| ts + 1).await.unwrap(), ());

        assert_eq!(clnt.read(&1).await.unwrap(), Some(15));
        assert_eq!(clnt.read(&2).await.unwrap(), Some(1));
        assert_eq!(clnt.read(&3).await.unwrap(), Some(5));
        assert_eq!(clnt.read(&4).await.unwrap(), Some(9));
        assert_eq!(clnt.read(&5).await.unwrap(), None);
    }
}
