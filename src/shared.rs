//! Shared MySQL storage for multiple workers.
//!
//! This module provides [`MySqlStorageFactory`], a factory for creating
//! multiple [`MySqlStorage`] instances that share a single MySQL connection
//! pool and task-fetching loop.
//!
//! ## Why shared storage?
//!
//! A MySQL backend can be used by multiple workers, but having every worker
//! independently poll the database results in unnecessary database queries.
//!
//! [`MySqlStorageFactory`] addresses this by maintaining a single shared
//! polling task for all storage instances created by the factory. The poller
//! fetches available tasks for all registered queues in a batch and routes
//! each task to the [`SharedFetcher`] belonging to the corresponding storage
//! instance.
//!
//! The resulting architecture is roughly:
//!
//! ```text
//!                         MySQL
//!                            │
//!                      poll interval
//!                            │
//!                            ▼
//!                  ┌───────────────────┐
//!                  │  Shared poller    │
//!                  │                   │
//!                  │ fetches tasks for │
//!                  │ all registered    │
//!                  │ queues in batches │
//!                  └─────────┬─────────┘
//!                            │
//!              ┌─────────────┼─────────────┐
//!              │             │             │
//!              ▼             ▼             ▼
//!          queue A       queue B       queue C
//!              │             │             │
//!              ▼             ▼             ▼
//!          Worker A      Worker B      Worker C
//! ```
//!
//! ## Creating a factory
//!
//! The factory owns the underlying [`MySqlPool`] and can create multiple
//! backends from it:
//!
//! ```ignore
//! # use apalis_mysql::shared::MySqlStorageFactory;
//! # use apalis_core::backend::factory::BackendFactory;
//! let mut factory = MySqlStorageFactory::new("mysql://localhost/apalis");
//!
//! let first = factory.create().unwrap();
//! let second = factory.create().unwrap();
//! ```
//!
//! Each backend has its own queue registration and receiver, while the
//! database connection pool and polling task are shared.
//!
//! ## Queue registration
//!
//! Each backend created by the factory is associated with a queue derived from
//! its [`Config`].
//!
//! A queue may only be registered once with a factory.
//!
//! Attempting to create another backend for an already-registered queue returns
//! [`SharedMySqlError::NamespaceExists`].
//!
//! ## Task dispatch
//!
//! [`SharedFetcher`] implements [`Stream`] and receives tasks from the shared
//! polling loop through an asynchronous channel.
//!
//! The resulting backend is an [`Interleave`] combining the normal
//! [`MySqlStorage`] implementation with [`SharedFetcher`].
//!
//! [`Config`]: crate::Config
//! [`Interleave`]: apalis_core::backend::ext::interleave::Interleave
//! [`MySqlPool`]: sqlx::MySqlPool
//! [`MySqlStorage`]: crate::MySqlStorage
//! [`MySqlStorageFactory`]: crate::shared::MySqlStorageFactory
//! [`SharedFetcher`]: crate::shared::SharedFetcher
//! [`SharedMySqlError`]: crate::shared::SharedMySqlError
//! [`Stream`]: futures::Stream
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use apalis_core::backend::{BackendConfig, ext::interleave::Interleave, factory::BackendFactory};
use futures::{
    FutureExt, SinkExt, Stream, StreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{BoxFuture, Shared},
    lock::Mutex,
};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{MySql, MySqlPool, pool::PoolOptions, types::chrono::Utc};

use crate::{Config, MySqlStorage, MySqlTask, error::Error, from_row::MySqlTaskRow};

/// A [`MySqlStorage`] interleaving with [`SharedFetcher`]
pub type SharedMySqlStorage<Args> = Interleave<MySqlStorage<Args>, SharedFetcher>;

type Registry = Arc<Mutex<HashMap<String, Sender<Result<MySqlTask, Error>>>>>;

/// The default interval at which the shared poller looks for new tasks.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Shared MySQL storage backend that can be used across multiple workers
#[derive(Clone, Debug)]
pub struct MySqlStorageFactory {
    pool: MySqlPool,
    registry: Registry,
    drive: Shared<BoxFuture<'static, ()>>,
    poll_interval: Arc<AtomicU64>,
}

impl MySqlStorageFactory {
    /// Get a reference to the underlying MySQL connection pool
    #[must_use]
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Create a new shared MySQL storage backend with the given database URL
    #[must_use]
    pub fn new(url: &str) -> MySqlStorageFactory {
        Self::new_with_pool_options(url, PoolOptions::<MySql>::new())
            .with_poll_interval(DEFAULT_POLL_INTERVAL)
    }

    /// Set the interval at which the shared poller looks for new tasks.
    ///
    /// MySQL has no equivalent of the SQLite update hook, so the shared
    /// poller is driven by a timer instead of by insertions. The new
    /// interval takes effect on the next poll.
    #[must_use]
    pub fn with_poll_interval(self, interval: Duration) -> MySqlStorageFactory {
        self.poll_interval
            .store(interval.as_millis() as u64, Ordering::Relaxed);
        self
    }

    /// Create a new shared MySQL storage backend with the given database URL and pool options
    #[must_use]
    pub fn new_with_pool_options(url: &str, options: PoolOptions<MySql>) -> MySqlStorageFactory {
        let pool = options
            .connect_lazy(url)
            .expect("Failed to create MySQL pool");
        let registry: Registry = Registry::default();

        let p = pool.clone();
        let instances = registry.clone();
        let poll_interval = Arc::new(AtomicU64::new(DEFAULT_POLL_INTERVAL.as_millis() as u64));
        let poller_interval = poll_interval.clone();
        MySqlStorageFactory {
            pool,
            drive: async move {
                loop {
                    let interval = Duration::from_millis(poller_interval.load(Ordering::Relaxed));
                    let interval = apalis_core::timer::Delay::new(interval);
                    interval.await;
                    let mut r = instances.lock().await;
                    if r.is_empty() {
                        continue;
                    }
                    let job_types: HashSet<String> = r.keys().cloned().collect();
                    let job_types = match serde_json::to_string(&job_types) {
                        Ok(job_types) => job_types,
                        Err(e) => {
                            log::error!("Error encoding job types: {e}");
                            continue;
                        }
                    };
                    let mut tx = match p.begin().await {
                        Ok(tx) => tx,
                        Err(e) => {
                            log::error!("Error fetching tasks: {e}");
                            continue;
                        }
                    };
                    let rows = match sqlx::query_file_as!(
                        MySqlTaskRow,
                        "queries/backend/fetch_next_shared.sql",
                        job_types,
                        Utc::now(),
                        10_i32
                    )
                    .fetch_all(&mut *tx)
                    .await
                    {
                        Ok(rows) => rows,
                        Err(e) => {
                            log::error!("Error fetching tasks: {e}");
                            continue;
                        }
                    };

                    if rows.is_empty() {
                        let _ = tx.rollback().await;
                        continue;
                    }

                    let ids = rows
                        .iter()
                        .filter_map(|row| row.id.clone())
                        .collect::<Vec<_>>();
                    let ids = match serde_json::to_string(&ids) {
                        Ok(ids) => ids,
                        Err(e) => {
                            log::error!("Error encoding task ids: {e}");
                            continue;
                        }
                    };
                    if let Err(e) = sqlx::query_file!("queries/backend/claim_tasks_shared.sql", ids)
                        .execute(&mut *tx)
                        .await
                    {
                        log::error!("Error claiming tasks: {e}");
                        continue;
                    }
                    if let Err(e) = tx.commit().await {
                        log::error!("Error committing tasks: {e}");
                        continue;
                    }

                    for row in rows {
                        let task: MySqlTask = match row.into_task(None) {
                            Ok(task) => task,
                            Err(e) => {
                                log::error!("Error converting task: {e}");
                                continue;
                            }
                        };
                        let queue = task
                            .queue()
                            .map(|queue| queue.to_string())
                            .unwrap_or_default();
                        if let Some(tx) = r.get_mut(&queue)
                            && let Err(e) = tx.send(Ok(task)).await
                        {
                            log::error!("Error pushing task: {e:?}");
                        }
                    }
                }
            }
            .boxed()
            .shared(),
            registry,
            poll_interval,
        }
    }
}

/// Errors that can occur when creating a shared MySQL storage backend
#[derive(Debug, thiserror::Error)]
pub enum SharedMySqlError {
    /// Namespace already exists in the registry
    #[error("Namespace {0} already exists")]
    NamespaceExists(String),
    /// Could not acquire registry loc
    #[error("Could not acquire registry lock")]
    RegistryLocked,
}

impl<Args> BackendFactory<Args> for MySqlStorageFactory
where
    Args: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Backend = SharedMySqlStorage<Args>;
    type Error = SharedMySqlError;

    fn create(&mut self) -> Result<Self::Backend, Self::Error>
    where
        <Self::Backend as BackendConfig>::Config: Default,
    {
        let config = Config::default().queue(std::any::type_name::<Args>());
        self.create_with_config(config)
    }

    fn create_with_config(
        &mut self,
        config: <Self::Backend as BackendConfig>::Config,
    ) -> Result<Self::Backend, Self::Error> {
        let (tx, rx) = mpsc::channel(config.batch_size);
        let mut r = self
            .registry
            .try_lock()
            .ok_or(SharedMySqlError::RegistryLocked)?;
        if r.insert(config.queue.to_string(), tx).is_some() {
            return Err(SharedMySqlError::NamespaceExists(config.queue.to_string()));
        }
        Ok(Interleave::new(
            MySqlStorage::new(&self.pool).with_config(config),
            SharedFetcher {
                poller: self.drive.clone(),
                receiver: Arc::new(Mutex::new(rx)),
            },
        ))
    }
}

/// A fetcher that uses a channel to receive jobs from a shared polling point
#[derive(Clone, Debug)]
pub struct SharedFetcher {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Arc<Mutex<Receiver<Result<MySqlTask, Error>>>>,
}

impl Stream for SharedFetcher {
    type Item = Result<MySqlTask, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        let _ = this.poller.poll_unpin(cx);

        let mut guard = futures::ready!(this.receiver.lock().poll_unpin(cx));
        guard.poll_next_unpin(cx)
    }
}
