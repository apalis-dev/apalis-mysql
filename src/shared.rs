use std::{
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    CompactType, Config, MysqlStorage, MysqlTask,
    ack::{LockTaskLayer, MySqlAck},
    fetcher::MySqlPollFetcher,
    initial_heartbeat, keep_alive,
};
use crate::{from_row::MySqlTaskRow, sink::MySqlSink};

use apalis_core::{
    backend::{
        Backend, BackendExt, TaskStream,
        codec::{Codec, json::JsonCodec},
        shared::MakeShared,
    },
    layers::Stack,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use apalis_sql::{context::SqlContext, from_row::TaskRow};
use futures::{
    FutureExt, Stream, StreamExt, TryStreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{BoxFuture, Shared},
    lock::Mutex,
    stream::{self, BoxStream, select},
};
use sqlx::{MySql, MySqlPool, pool::PoolOptions};
use ulid::Ulid;

/// Shared MySql storage backend that can be used across multiple workers
#[derive(Clone, Debug)]
pub struct SharedMysqlStorage<Decode> {
    pool: MySqlPool,
    registry: Arc<Mutex<HashMap<String, Sender<MysqlTask<CompactType>>>>>,
    drive: Shared<BoxFuture<'static, ()>>,
    _marker: PhantomData<Decode>,
}

impl<Decode> SharedMysqlStorage<Decode> {
    /// Get a reference to the underlying MySql connection pool
    #[must_use]
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
}

impl SharedMysqlStorage<JsonCodec<CompactType>> {
    /// Create a new shared MySql storage backend with the given database URL
    #[must_use]
    pub fn new(url: &str) -> Self {
        Self::new_with_codec(url)
    }
    /// Create a new shared MySql storage backend with the given database URL and codec
    #[must_use]
    pub fn new_with_codec<Codec>(url: &str) -> SharedMysqlStorage<Codec> {
        let pool = PoolOptions::<MySql>::new()
            .connect_lazy(url)
            .expect("Failed to create MySql pool");

        let registry: Arc<Mutex<HashMap<String, Sender<MysqlTask<CompactType>>>>> =
            Arc::new(Mutex::new(HashMap::default()));
        let drive = {
            let pool = pool.clone();
            let registry = registry.clone();
            let fut = async move {
                loop {
                    let interval = apalis_core::timer::Delay::new(Duration::from_millis(100));
                    interval.await;
                    let mut r = registry.lock().await;
                    let job_types: Vec<String> = r.keys().cloned().collect();
                    let lock_at = chrono::Utc::now().timestamp();
                    let job_types = serde_json::to_string(&job_types).unwrap();
                    let mut tx = pool.begin().await.unwrap();
                    sqlx::query_file_as!(
                        MySqlTaskRow,
                        "queries/backend/fetch_next_shared.sql",
                        job_types,
                        lock_at,
                        10_i32
                    )
                    .execute(&pool)
                    .await
                    .unwrap();
                    let rows: Vec<MysqlTask<CompactType>> = sqlx::query_as!(
                        MySqlTaskRow,
                        "SELECT * FROM jobs WHERE status = 'Queued' AND JSON_CONTAINS(?, JSON_QUOTE(job_type)) AND lock_at = ?",
                        job_types,
                        lock_at
                    )
                    .fetch_all(&mut *tx)
                    .await.unwrap()
                    .into_iter()
                    .map(|r| {
                        let row: TaskRow = r.try_into()?;
                        row.try_into_task_compact::<Ulid>()
                            .map_err(|e| sqlx::Error::Protocol(e.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                    tx.commit().await.unwrap();
                    for task in rows {
                        if let Some(sender) =
                            r.get_mut(task.parts.ctx.queue().as_ref().unwrap().as_str())
                        {
                            let _ = sender.try_send(task);
                        }
                    }
                }
            };
            fut.boxed().shared()
        };
        SharedMysqlStorage {
            pool,
            drive,
            registry,
            _marker: PhantomData,
        }
    }
}

/// Errors that can occur when creating a shared MySql storage backend
#[derive(Debug, thiserror::Error)]
pub enum SharedMySqlError {
    /// Namespace already exists in the registry
    #[error("Namespace {0} already exists")]
    NamespaceExists(String),
    /// Could not acquire registry loc
    #[error("Could not acquire registry lock")]
    RegistryLocked,
}

impl<Args, Decode: Codec<Args, Compact = CompactType>> MakeShared<Args>
    for SharedMysqlStorage<Decode>
{
    type Backend = MysqlStorage<Args, Decode, SharedFetcher<CompactType>>;
    type Config = Config;
    type MakeError = SharedMySqlError;
    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        Self::make_shared_with_config(self, Config::new(std::any::type_name::<Args>()))
    }
    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (tx, rx) = mpsc::channel(config.buffer_size());
        let mut r = self
            .registry
            .try_lock()
            .ok_or(SharedMySqlError::RegistryLocked)?;
        if r.insert(config.queue().to_string(), tx).is_some() {
            return Err(SharedMySqlError::NamespaceExists(
                config.queue().to_string(),
            ));
        }
        let sink = MySqlSink::new(&self.pool, &config);
        Ok(MysqlStorage {
            config,
            fetcher: SharedFetcher {
                poller: self.drive.clone(),
                receiver: Arc::new(std::sync::Mutex::new(rx)),
            },
            pool: self.pool.clone(),
            sink,
            job_type: PhantomData,
            codec: PhantomData,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SharedFetcher<Compact> {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Arc<std::sync::Mutex<Receiver<MysqlTask<Compact>>>>,
}

impl<Compact> Stream for SharedFetcher<Compact> {
    type Item = MysqlTask<Compact>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        let _ = this.poller.poll_unpin(cx);

        // Delegate actual items to receiver
        this.receiver.lock().unwrap().poll_next_unpin(cx)
    }
}

impl<Args, Decode> Backend for MysqlStorage<Args, Decode, SharedFetcher<CompactType>>
where
    Args: Send + 'static + Unpin + Sync,
    Decode: Codec<Args, Compact = CompactType> + 'static + Unpin + Send + Sync,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;

    type IdType = Ulid;

    type Error = sqlx::Error;

    type Stream = TaskStream<MysqlTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Context = SqlContext;

    type Layer = Stack<AcknowledgeLayer<MySqlAck>, LockTaskLayer>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let keep_alive_interval = *self.config.keep_alive();
        let pool = self.pool.clone();
        let worker = worker.clone();
        let config = self.config.clone();

        stream::unfold((), move |()| async move {
            apalis_core::timer::sleep(keep_alive_interval).await;
            Some(((), ()))
        })
        .then(move |_| keep_alive(pool.clone(), config.clone(), worker.clone()))
        .boxed()
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.pool.clone());
        let ack = AcknowledgeLayer::new(MySqlAck::new(self.pool.clone()));
        Stack::new(ack, lock)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_shared(worker)
            .map(|a| match a {
                Ok(Some(task)) => Ok(Some(
                    task.try_map(|t| Decode::decode(&t))
                        .map_err(|e| sqlx::Error::Decode(e.into()))?,
                )),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode: Send + 'static> BackendExt
    for MysqlStorage<Args, Decode, SharedFetcher<CompactType>>
where
    Self: Backend<Args = Args, IdType = Ulid, Context = SqlContext, Error = sqlx::Error>,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static + Unpin,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = TaskStream<MysqlTask<Self::Compact>, sqlx::Error>;

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_shared(worker).boxed()
    }
}

impl<Args, Decode: Send + 'static> MysqlStorage<Args, Decode, SharedFetcher<CompactType>> {
    fn poll_shared(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<MysqlTask<CompactType>>, sqlx::Error>> + 'static {
        let pool = self.pool.clone();
        let worker = worker.clone();
        // Initial registration heartbeat
        // This ensures that the worker is registered before fetching any tasks
        // This also ensures that the worker is marked as alive in case it crashes
        // before fetching any tasks
        // Subsequent heartbeats are handled in the heartbeat stream
        let init = initial_heartbeat(
            pool,
            self.config.clone(),
            worker.clone(),
            "SharedMysqlStorage",
        );
        let starter = stream::once(init)
            .map_ok(|_| None) // Noop after initial heartbeat
            .boxed();
        let lazy_fetcher = self.fetcher.map(|s| Ok(Some(s))).boxed();

        let eager_fetcher = StreamExt::boxed(MySqlPollFetcher::<CompactType, Decode>::new(
            &self.pool,
            &self.config,
            &worker,
        ));
        starter.chain(select(lazy_fetcher, eager_fetcher)).boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_core::{
        backend::TaskSink, error::BoxDynError, task::task_id::TaskId,
        worker::builder::WorkerBuilder,
    };

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let mut store = SharedMysqlStorage::new(&std::env::var("DATABASE_URL").unwrap());
        MysqlStorage::setup(store.pool()).await.unwrap();

        let mut map_store = store.make_shared().unwrap();

        let mut int_store = store.make_shared().unwrap();

        map_store
            .push(HashMap::<String, i32>::from([("value".to_string(), 42)]))
            .await
            .unwrap();
        int_store.push(99).await.unwrap();

        async fn send_reminder<T, I>(
            _: T,
            _task_id: TaskId<I>,
            wrk: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            wrk.stop().unwrap();
            Ok(())
        }

        let int_worker = WorkerBuilder::new("rango-tango-2")
            .backend(int_store)
            .build(send_reminder);
        let map_worker = WorkerBuilder::new("rango-tango-1")
            .backend(map_store)
            .build(send_reminder);
        tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
    }
}
