#![doc = include_str!("../README.md")]
//!
use std::{fmt, marker::PhantomData};

use apalis_core::{
    backend::{
        Backend, BackendExt, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    features_table,
    layers::Stack,
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
pub use apalis_sql::context::SqlContext;
use futures::{
    FutureExt, Stream, StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
pub use sqlx::{
    Connection, MySql, MySqlConnection, MySqlPool, Pool,
    error::Error as SqlxError,
    mysql::MySqlConnectOptions,
    pool::{PoolConnection, PoolOptions},
};
use ulid::Ulid;

use crate::{
    ack::{LockTaskLayer, MySqlAck},
    fetcher::{MySqlFetcher, MySqlPollFetcher},
    queries::{
        keep_alive::{initial_heartbeat, keep_alive, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
    sink::MySqlSink,
};

mod ack;
mod config;
/// Fetcher module for retrieving tasks from mysql backend
pub mod fetcher;
mod from_row;
/// Queries module for mysql backend
pub mod queries;
mod shared;
/// Sink module for pushing tasks to mysql backend
pub mod sink;

/// Type alias for a task stored in mysql backend
pub type MySqlTask<Args> = Task<Args, SqlContext, Ulid>;
pub use config::Config;
pub use shared::{SharedMySqlError, SharedMySqlStorage};

/// CompactType is the type used for compact serialization in mysql backend
pub type CompactType = Vec<u8>;

/// MySqlStorage is a storage backend for apalis using mysql as the database.
///
/// It supports both standard polling and event-driven (hooked) storage mechanisms.
///
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_mysql::MySqlStorage;
        #   use sqlx::MySqlPool;
        #   let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();
        #   MySqlStorage::setup(&pool).await.unwrap();
        #   MySqlStorage::new(&pool)
        # };
    "#,

    Backend => supported("Supports storage and retrieval of tasks", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => supported("Serialization support for arguments", true),
    Workflow => supported("Flexible enough to support workflows", true),
    WebUI => supported("Expose a web interface for monitoring tasks", true),
    FetchById => supported("Allow fetching a task by its ID", false),
    RegisterWorker => supported("Allow registering a worker with the backend", false),
    MakeShared => supported("Share one connection across multiple workers via [`SharedMySqlStorage`]", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => supported("Resume a task by its ID", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
    ListWorkers => supported("List all workers registered with the backend", false),
    ListTasks => supported("List all tasks in the backend", false),
}]
#[pin_project::pin_project]
pub struct MySqlStorage<T, C, Fetcher> {
    pool: Pool<MySql>,
    job_type: PhantomData<T>,
    config: Config,
    codec: PhantomData<C>,
    #[pin]
    sink: MySqlSink<T, CompactType, C>,
    #[pin]
    fetcher: Fetcher,
}

impl<T, C, F> fmt::Debug for MySqlStorage<T, C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MySqlStorage")
            .field("pool", &self.pool)
            .field("job_type", &"PhantomData<T>")
            .field("config", &self.config)
            .field("codec", &std::any::type_name::<C>())
            .finish()
    }
}

impl<T, C, F: Clone> Clone for MySqlStorage<T, C, F> {
    fn clone(&self) -> Self {
        Self {
            sink: self.sink.clone(),
            pool: self.pool.clone(),
            job_type: PhantomData,
            config: self.config.clone(),
            codec: self.codec,
            fetcher: self.fetcher.clone(),
        }
    }
}

impl MySqlStorage<(), (), ()> {
    /// Get mysql migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("./migrations")
    }

    /// Do migrations for mysql
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &Pool<MySql>) -> Result<(), sqlx::Error> {
        Self::migrations().run(pool).await?;
        Ok(())
    }
}

impl<T> MySqlStorage<T, (), ()> {
    /// Create a new MySqlStorage
    #[must_use]
    pub fn new(
        pool: &Pool<MySql>,
    ) -> MySqlStorage<T, JsonCodec<CompactType>, fetcher::MySqlFetcher> {
        let config = Config::new(std::any::type_name::<T>());
        MySqlStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            sink: MySqlSink::new(pool, &config),
            config,
            codec: PhantomData,
            fetcher: fetcher::MySqlFetcher,
        }
    }

    /// Create a new MySqlStorage for a specific queue
    #[must_use]
    pub fn new_in_queue(
        pool: &Pool<MySql>,
        queue: &str,
    ) -> MySqlStorage<T, JsonCodec<CompactType>, fetcher::MySqlFetcher> {
        let config = Config::new(queue);
        MySqlStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            sink: MySqlSink::new(pool, &config),
            config,
            codec: PhantomData,
            fetcher: fetcher::MySqlFetcher,
        }
    }

    /// Create a new MySqlStorage with config
    #[must_use]
    pub fn new_with_config(
        pool: &Pool<MySql>,
        config: &Config,
    ) -> MySqlStorage<T, JsonCodec<CompactType>, fetcher::MySqlFetcher> {
        MySqlStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: MySqlSink::new(pool, config),
            fetcher: fetcher::MySqlFetcher,
        }
    }
}

impl<T, C, F> MySqlStorage<T, C, F> {
    /// Change the codec used for serialization/deserialization
    pub fn with_codec<D>(self) -> MySqlStorage<T, D, F> {
        MySqlStorage {
            sink: MySqlSink::new(&self.pool, &self.config),
            pool: self.pool,
            job_type: PhantomData,
            config: self.config,
            codec: PhantomData,
            fetcher: self.fetcher,
        }
    }

    /// Get the config used by the storage
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the connection pool used by the storage
    pub fn pool(&self) -> &Pool<MySql> {
        &self.pool
    }
}

impl<Args, Decode> Backend for MySqlStorage<Args, Decode, MySqlFetcher>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;

    type Context = SqlContext;

    type Error = sqlx::Error;

    type Stream = TaskStream<MySqlTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<MySqlAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let keep_alive = keep_alive_stream(pool, config, worker);
        let reenqueue = reenqueue_orphaned_stream(
            self.pool.clone(),
            self.config.clone(),
            *self.config.keep_alive(),
        )
        .map_ok(|_| ());
        futures::stream::select(keep_alive, reenqueue).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.pool.clone());
        let ack = AcknowledgeLayer::new(MySqlAck::new(self.pool.clone()));
        Stack::new(lock, ack)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_default(worker)
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

impl<Args, Decode: Send + 'static> BackendExt for MySqlStorage<Args, Decode, MySqlFetcher>
where
    Self: Backend<Args = Args, IdType = Ulid, Context = SqlContext, Error = sqlx::Error>,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static + Unpin,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = TaskStream<MySqlTask<Self::Compact>, sqlx::Error>;

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_default(worker).boxed()
    }
}

impl<Args, Decode: Send + 'static, F> MySqlStorage<Args, Decode, F> {
    fn poll_default(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<MySqlTask<CompactType>>, sqlx::Error>> + Send + 'static
    {
        let fut = initial_heartbeat(
            self.pool.clone(),
            self.config().clone(),
            worker.clone(),
            "MySqlStorage",
        );
        let register = stream::once(fut.map(|_| Ok(None)));
        register.chain(MySqlPollFetcher::<CompactType, Decode>::new(
            &self.pool,
            &self.config,
            worker,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis::prelude::*;
    use apalis_workflow::*;
    use chrono::Local;
    use serde::{Deserialize, Serialize};
    use sqlx::MySqlPool;

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        const ITEMS: usize = 10;
        let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        MySqlStorage::setup(&pool).await.unwrap();

        let mut backend = MySqlStorage::new(&pool);

        let mut start = 0;

        let mut items = stream::repeat_with(move || {
            start += 1;
            start
        })
        .take(ITEMS);
        backend.push_stream(&mut items).await.unwrap();

        println!("Starting worker at {}", Local::now());

        async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
            if ITEMS == item {
                wrk.stop().unwrap();
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_workflow() {
        let workflow = Workflow::new("odd-numbers-workflow")
            .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>()) })
            .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 3 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 5 != 0 { Some(x) } else { None } })
            .delay_for(Duration::from_millis(1000))
            .and_then(|a: Vec<usize>| async move {
                println!("Sum: {}", a.iter().sum::<usize>());
                Err::<(), BoxDynError>("Intentional Error".into())
            });

        let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        let mut mysql = MySqlStorage::new_with_config(
            &pool,
            &Config::new("workflow-queue").with_poll_interval(
                StrategyBuilder::new()
                    .apply(IntervalStrategy::new(Duration::from_millis(100)))
                    .build(),
            ),
        );

        MySqlStorage::setup(&pool).await.unwrap();

        mysql.push_start(100usize).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(mysql)
            .on_event(|ctx, ev| {
                println!("On Event = {:?}", ev);
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_workflow_complete() {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct PipelineConfig {
            min_confidence: f32,
            enable_sentiment: bool,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct UserInput {
            text: String,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Classified {
            text: String,
            label: String,
            confidence: f32,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Summary {
            text: String,
            sentiment: Option<String>,
        }

        let workflow = Workflow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .and_then(|input: UserInput, mut worker: WorkerContext| async move {
                worker.emit(&Event::custom(format!(
                    "Preprocessing input: {}",
                    input.text
                )));
                let processed = input.text.to_lowercase();
                Ok::<_, BoxDynError>(processed)
            })
            // Step 2: Classify text
            .and_then(|text: String| async move {
                let confidence = 0.85; // pretend model confidence
                let items = text.split_whitespace().collect::<Vec<_>>();
                let results = items
                    .into_iter()
                    .map(|x| Classified {
                        text: x.to_string(),
                        label: if x.contains("rust") {
                            "Tech"
                        } else {
                            "General"
                        }
                        .to_string(),
                        confidence,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, BoxDynError>(results)
            })
            // Step 3: Filter out low-confidence predictions
            .filter_map(
                |c: Classified| async move { if c.confidence >= 0.6 { Some(c) } else { None } },
            )
            .filter_map(move |c: Classified, config: Data<PipelineConfig>| {
                let cfg = config.enable_sentiment;
                async move {
                    if !cfg {
                        return Some(Summary {
                            text: c.text,
                            sentiment: None,
                        });
                    }

                    // pretend we run a sentiment model
                    let sentiment = if c.text.contains("delightful") {
                        "positive"
                    } else {
                        "neutral"
                    };
                    Some(Summary {
                        text: c.text,
                        sentiment: Some(sentiment.to_string()),
                    })
                }
            })
            .and_then(|a: Vec<Summary>, mut worker: WorkerContext| async move {
                worker.emit(&Event::Custom(Box::new(format!(
                    "Generated {} summaries",
                    a.len()
                ))));
                worker.stop()
            });

        let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        let mut mysql = MySqlStorage::new_with_config(&pool, &Config::new("text-pipeline"));

        MySqlStorage::setup(&pool).await.unwrap();

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_string(),
        };
        mysql.push_start(input).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(mysql)
            .data(PipelineConfig {
                min_confidence: 0.8,
                enable_sentiment: true,
            })
            .on_event(|ctx, ev| match ev {
                Event::Custom(msg) => {
                    if let Some(m) = msg.downcast_ref::<String>() {
                        println!("Custom Message: {}", m);
                    }
                }
                Event::Error(_) => {
                    println!("On Error = {:?}", ev);
                    ctx.stop().unwrap();
                }
                _ => {
                    println!("On Event = {:?}", ev);
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
