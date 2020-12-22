/// Storage requirements for ABD

/// Produce the minimum value of a type
///
/// This trait makes it possible for the protocol to generate the first timestamp of an object
pub trait Min {
    fn min() -> Self;
}

impl<T: num::Zero> Min for T {
    fn min() -> T {
        num::zero::<T>()
    }
}

/// Timestamp with a client ID to solve ties
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp<Ts: Ord + Min + Eq, ClientId: Ord + Min + Eq>(pub Ts, pub ClientId);

/// Timestamped value
///
/// Requires timestamp which is totally ordered and can produce a minimum value
#[derive(Clone, Debug, PartialEq)]
pub struct Timestamped<Item, Ts: Ord + Min + Eq, ClientId: Ord + Min + Eq> {
    pub ts: Timestamp<Ts, ClientId>,
    pub val: Item,
}

impl<Item, Ts, ClientId> Timestamped<Item, Ts, ClientId>
where
    Ts: Ord + Min + Eq,
    ClientId: Ord + Min + Eq,
{
    pub fn new(val: Item, ts: Ts, cid: ClientId) -> Self {
        Timestamped {
            ts: Timestamp(ts, cid),
            val,
        }
    }
}

/// Trait defining which type of elements are stored and `how` they are stored
///
/// Here would be the place to implement persistency
///
/// The protocol will then be programmed against this local store
///
/// The object should be accessible from multiple threads
///
/// If the protocol should support deletions, that should be modelled by making `Item` be an
/// `Option<T>`. This way, deleting, is just writing None. It does make using the API _slightly_
/// cumbersome because the return type or `read`s becomes `Option<Timestamped<Option<_>, _, _>>`.
pub trait LocalStorage {
    type Key;
    type Item;
    type Ts: Ord + Min;
    type ClientId: Ord + Min;
    type Error;

    /// write a value to the local storage
    fn write(
        &self,
        k: &Self::Key,
        v: Self::Item,
        timestamp: Self::Ts,
        client_id: Self::ClientId,
    ) -> Result<(), Self::Error>;

    /// read a value from the local storage
    fn read(
        &self,
        k: &Self::Key,
    ) -> Result<Option<Timestamped<Self::Item, Self::Ts, Self::ClientId>>, Self::Error>;
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use std::sync::{Arc, RwLock};

    struct SingletonStore(RwLock<Option<Timestamped<u64, usize, usize>>>);

    impl SingletonStore {
        fn new(init: Option<Timestamped<u64, usize, usize>>) -> Self {
            SingletonStore(RwLock::new(init))
        }

        fn inner(&self) -> &RwLock<Option<Timestamped<u64, usize, usize>>> {
            &self.0
        }
    }

    impl LocalStorage for SingletonStore {
        type Key = ();
        type Item = u64;
        type Ts = usize;
        type ClientId = usize;
        type Error = ();

        fn write(
            &self,
            _k: &Self::Key,
            v: Self::Item,
            ts: usize,
            cid: usize,
        ) -> Result<(), Self::Error> {
            let mut guard = self.inner().write().unwrap();
            let ts = Timestamp(ts, cid);
            if guard.is_none() || guard.as_ref().unwrap().ts < ts {
                *guard = Some(Timestamped { ts, val: v });
            }
            Ok(())
        }

        fn read(
            &self,
            _k: &Self::Key,
        ) -> Result<Option<Timestamped<u64, usize, usize>>, Self::Error> {
            Ok(self.inner().read().unwrap().as_ref().cloned())
        }
    }

    #[test]
    fn singleton_store() {
        let stor = SingletonStore::new(None);
        assert_eq!(stor.read(&()), Ok(None));
        assert_eq!(stor.write(&(), 12, 1, 0), Ok(()));
        assert_eq!(
            stor.read(&()),
            Ok(Some(Timestamped {
                ts: Timestamp(1, 0),
                val: 12
            }))
        );
        assert_eq!(stor.write(&(), 42, 2, 4), Ok(()));
        assert_eq!(
            stor.read(&()),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 4),
                val: 42
            }))
        );
        assert_eq!(stor.write(&(), 12, 1, 4), Ok(()));
        assert_eq!(
            stor.read(&()),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 4),
                val: 42
            }))
        );
        assert_eq!(stor.write(&(), 42, 2, 4), Ok(()));
        assert_eq!(
            stor.read(&()),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 4),
                val: 42
            }))
        );
        assert_eq!(stor.write(&(), 42, 2, 2), Ok(()));
        assert_eq!(
            stor.read(&()),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 4),
                val: 42
            }))
        );
        assert_eq!(stor.write(&(), 42, 2, 6), Ok(()));
        assert_eq!(
            stor.read(&()),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 6),
                val: 42
            }))
        );
    }

    #[test]
    fn singleton_store_mt() {
        let stor = Arc::new(SingletonStore::new(None));
        let other_stor = stor.clone();

        assert_eq!(stor.read(&()), Ok(None));

        let thread = std::thread::spawn(move || {
            assert_eq!(other_stor.read(&()), Ok(None));
            assert_eq!(other_stor.write(&(), 1, 1, 1), Ok(()));
            while let Ok(Some(Timestamped {
                ts: Timestamp(1, 1),
                val: 1,
            })) = other_stor.read(&())
            {}
            assert_eq!(other_stor.write(&(), 3, 3, 3), Ok(()));
            while let Ok(Some(Timestamped {
                ts: Timestamp(3, 3),
                val: 3,
            })) = other_stor.read(&())
            {}
            assert_eq!(
                other_stor.read(&()),
                Ok(Some(Timestamped {
                    ts: Timestamp(4, 4),
                    val: 4
                }))
            );
            assert_eq!(other_stor.write(&(), 42, 10, 10), Ok(()));
        });

        while let Ok(None) = stor.read(&()) {}
        assert_eq!(stor.write(&(), 2, 2, 2), Ok(()));
        while let Ok(Some(Timestamped {
            ts: Timestamp(2, 2),
            val: 2,
        })) = stor.read(&())
        {}
        assert_eq!(stor.write(&(), 4, 4, 4), Ok(()));
        thread.join().unwrap();

        assert_eq!(stor.write(&(), 43, 11, 1), Ok(()));
        assert_eq!(
            stor.read(&()),
            Ok(Some(Timestamped {
                ts: Timestamp(11, 1),
                val: 43
            }))
        );
    }

    struct MapStore(DashMap<u64, Timestamped<u64, usize, usize>>);

    impl MapStore {
        fn new() -> Self {
            MapStore(DashMap::new())
        }

        fn inner(&self) -> &DashMap<u64, Timestamped<u64, usize, usize>> {
            &self.0
        }
    }

    impl LocalStorage for MapStore {
        type Key = u64;
        type Item = u64;
        type Ts = usize;
        type ClientId = usize;
        type Error = ();

        fn write(
            &self,
            k: &Self::Key,
            v: Self::Item,
            ts: Self::Ts,
            cid: Self::ClientId,
        ) -> Result<(), Self::Error> {
            let handle = self.inner().get_mut(&k);
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
        ) -> Result<Option<Timestamped<Self::Item, Self::Ts, Self::ClientId>>, Self::Error>
        {
            Ok(self.inner().get(&k).map(|view| view.value().clone()))
        }
    }

    #[test]
    fn map_store() {
        let stor = MapStore::new();
        assert_eq!(stor.read(&0), Ok(None));
        assert_eq!(stor.read(&1), Ok(None));
        assert_eq!(stor.read(&2), Ok(None));
        assert_eq!(stor.read(&3), Ok(None));
        assert_eq!(stor.write(&1, 12, 1, 1), Ok(()));
        assert_eq!(
            stor.read(&1),
            Ok(Some(Timestamped {
                ts: Timestamp(1, 1),
                val: 12
            }))
        );
        assert_eq!(stor.write(&1, 42, 2, 2), Ok(()));
        assert_eq!(
            stor.read(&1),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 2),
                val: 42
            }))
        );
        assert_eq!(stor.write(&1, 12, 1, 3), Ok(()));
        assert_eq!(
            stor.read(&1),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 2),
                val: 42
            }))
        );
        assert_eq!(stor.write(&1, 43, 2, 1), Ok(()));
        assert_eq!(
            stor.read(&1),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 2),
                val: 42
            }))
        );
        assert_eq!(stor.write(&1, 43, 2, 3), Ok(()));
        assert_eq!(
            stor.read(&1),
            Ok(Some(Timestamped {
                ts: Timestamp(2, 3),
                val: 43
            }))
        );

        assert_eq!(stor.write(&1, 15, 3, 0), Ok(()));
        assert_eq!(stor.write(&2, 1, 1, 0), Ok(()));
        assert_eq!(stor.write(&3, 5, 1, 0), Ok(()));
        assert_eq!(stor.write(&4, 9, 1, 0), Ok(()));

        assert_eq!(
            stor.read(&1),
            Ok(Some(Timestamped {
                ts: Timestamp(3, 0),
                val: 15
            }))
        );
        assert_eq!(
            stor.read(&2),
            Ok(Some(Timestamped {
                ts: Timestamp(1, 0),
                val: 1
            }))
        );
        assert_eq!(
            stor.read(&3),
            Ok(Some(Timestamped {
                ts: Timestamp(1, 0),
                val: 5
            }))
        );
        assert_eq!(
            stor.read(&4),
            Ok(Some(Timestamped {
                ts: Timestamp(1, 0),
                val: 9
            }))
        );
        assert_eq!(stor.read(&5), Ok(None));
    }

    #[test]
    fn map_store_mt() {
        let stor = Arc::new(MapStore::new());
        let stor2 = stor.clone();
        let stor3 = stor.clone();

        assert_eq!(stor.read(&0), Ok(None));
        assert_eq!(stor.read(&1), Ok(None));
        assert_eq!(stor.read(&2), Ok(None));

        std::thread::spawn(move || {
            assert_eq!(stor2.read(&0), Ok(None));
            assert_eq!(stor2.write(&0, 1, 1, 1), Ok(()));
            while let Ok(Some(Timestamped {
                ts: Timestamp(1, 1),
                val: 1,
            })) = stor2.read(&0)
            {}
            assert_eq!(stor2.write(&0, 3, 3, 3), Ok(()));
            assert_eq!(stor2.write(&1, 1, 1, 1), Ok(()));
            while let Ok(Some(Timestamped {
                ts: Timestamp(3, 3),
                val: 3,
            })) = stor2.read(&0)
            {}
            assert_eq!(
                stor2.read(&0),
                Ok(Some(Timestamped {
                    ts: Timestamp(4, 4),
                    val: 4
                }))
            );
        });

        let thread = std::thread::spawn(move || {
            assert_eq!(stor3.read(&2), Ok(None));
            assert_eq!(stor3.write(&2, 1, 1, 1), Ok(()));
            assert_eq!(
                stor3.read(&2),
                Ok(Some(Timestamped {
                    ts: Timestamp(1, 1),
                    val: 1
                }))
            );
            assert_eq!(stor3.write(&2, 2, 2, 2), Ok(()));
            assert_eq!(
                stor3.read(&2),
                Ok(Some(Timestamped {
                    ts: Timestamp(2, 2),
                    val: 2
                }))
            );
            assert_eq!(stor3.write(&2, 3, 3, 3), Ok(()));
            assert_eq!(
                stor3.read(&2),
                Ok(Some(Timestamped {
                    ts: Timestamp(3, 3),
                    val: 3
                }))
            );
        });

        while let Ok(None) = stor.read(&0) {}
        assert_eq!(stor.write(&0, 2, 2, 2), Ok(()));
        while let Ok(Some(Timestamped {
            ts: Timestamp(1, 1),
            val: 1,
        })) = stor.read(&1)
        {}
        assert_eq!(
            stor.read(&0),
            Ok(Some(Timestamped {
                ts: Timestamp(3, 3),
                val: 3
            }))
        );
        assert_eq!(stor.write(&0, 4, 4, 4), Ok(()));

        thread.join().unwrap();
        assert_eq!(
            stor.read(&0),
            Ok(Some(Timestamped {
                ts: Timestamp(4, 4),
                val: 4
            }))
        );
        assert_eq!(
            stor.read(&1),
            Ok(Some(Timestamped {
                ts: Timestamp(1, 1),
                val: 1
            }))
        );
        assert_eq!(
            stor.read(&2),
            Ok(Some(Timestamped {
                ts: Timestamp(3, 3),
                val: 3
            }))
        );
    }
}
