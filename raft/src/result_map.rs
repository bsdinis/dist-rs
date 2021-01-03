use core::hash::Hash;
use dashmap::DashMap;
use tokio::sync::oneshot;

/// A map for restults
///
/// Calling `get_fut` on the map will return a future which will be resolved when
/// the value is inserted on that key.
pub struct ResultMap<K: Eq + Hash, V: Clone> {
    futures: DashMap<K, Vec<oneshot::Sender<V>>>,
    map: DashMap<K, V>,
}

/// Future value
///
/// Calling get will transfer ownership
pub enum Val<V> {
    Fut(oneshot::Receiver<V>),
    Ready(V),
}

impl<V> Val<V> {
    pub async fn into(self) -> V {
        match self {
            Val::Fut(rx) => rx.await.unwrap(),
            Val::Ready(v) => v,
        }
    }
}

impl<K, V> ResultMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        ResultMap {
            futures: DashMap::new(),
            map: DashMap::new(),
        }
    }

    /// Insert a new value in the map, only if there is no other value there
    pub fn insert(&self, key: K, value: V) {
        match self.futures.remove(&key) {
            Some((_, txs)) => txs.into_iter().for_each(|tx| {
                tx.send(value.clone()).unwrap_or(());
            }),
            None => {
                self.map.insert(key, value);
            }
        }
    }

    pub fn pop(&self, key: K) -> Val<V> {
        if let Some((_, v)) = self.map.remove(&key) {
            return Val::Ready(v);
        }

        let (tx, rx) = oneshot::channel();

        if let Some(mut txs) = self.futures.get_mut(&key) {
            txs.push(tx);
        } else {
            self.futures.insert(key, vec![tx]);
        }

        Val::Fut(rx)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn future_test() {
        let res_map: std::sync::Arc<ResultMap<u64, u64>> = std::sync::Arc::new(ResultMap::new());
        let res_map_c = res_map.clone();
        let val = res_map.pop(65);
        let val2 = res_map.pop(65);

        assert!(matches!(val, Val::Fut(_)));
        assert!(matches!(val2, Val::Fut(_)));

        tokio::spawn(async move {
            res_map_c.insert(65, 1);
        });

        tokio::spawn(async move {
            assert_eq!(val.into().await, 1);
        });

        tokio::spawn(async move {
            assert_eq!(val2.into().await, 1);
        });

        res_map.insert(1, 42);
        let val3 = res_map.pop(1);
        assert!(matches!(val3, Val::Ready(_)));
        assert_eq!(val3.into().await, 42);
    }
}
