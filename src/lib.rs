#![doc = include_str!("../README.md")]
//!
//! [`MySqlStorageFactory`]: crate::shared::MySqlStorageFactory
use std::{
    fmt::Debug,
    marker::PhantomData,
    task::{Context, Poll},
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{
        Backend, BackendConfig, TryNewBackend, WireFormatBackend,
        ext::{BackendExt, lifecycle::BeforeStart},
        finalize::Durable,
        persistence::{Persisted, TaskPersistLayer},
    },
    features_table,
    task::{Task, task_id::TaskId},
    worker::context::WorkerContext,
};
use futures::FutureExt;
use serde_json::Value;
use ulid::Ulid;

pub use sqlx::{
    MySqlConnection, MySqlPool, error::Error as SqlxError, mysql::MySqlConnectOptions,
    mysql::MySqlPoolOptions,
};

mod config;
mod error;
mod from_row;
mod persist;
pub mod queries;
pub mod shared;
mod sink;
mod timestamp;

pub use config::Config;
pub use error::Error;

/// An alias for [Task], specialized for MySQL.
pub type MySqlTask<Args = Vec<u8>> = Task<Args>;
/// An alias for [TaskId], specialized for MySQL.
pub type MySqlTaskId = TaskId;

use persist::MySqlPersistence;

/// MySqlStorage is a storage backend for apalis using mysql as the database.
///
/// It supports both standard polling and shared polling storage mechanisms.
///
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_mysql::MySqlStorage;
        #   use sqlx::MySqlPool;
        #   let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();
        #   MySqlStorage::setup(&pool).await.unwrap();
        #   MySqlStorage::<u32>::new(&pool)
        # };
    "#,

    Backend => supported("Supports storage and retrieval of tasks", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => supported("Serialization support for arguments", true),
    Workflow => supported("Flexible enough to support workflows", true),
    WebUI => supported("Expose a web interface for monitoring tasks", true),
    FetchById => supported("Allow fetching a task by its ID", false),
    RegisterWorker => supported("Allow registering a worker in the backend", false),
    BackendFactory => supported("Share one connection across multiple workers via [`MySqlStorageFactory`]", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => supported("Resume a task by its ID", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
    ListWorkers => supported("List all workers registered with the backend", false),
    ListTasks => supported("List all tasks in the backend", false),
}]
///
/// [`MySqlStorageFactory`]: crate::shared::MySqlStorageFactory
#[pin_project::pin_project]
#[derive(Debug)]
pub struct MySqlStorage<Args> {
    #[pin]
    persistence: Persisted<MySqlPersistence>,
    job_type: PhantomData<Args>,
    codec: JsonCodec,
}

impl<Args> Clone for MySqlStorage<Args> {
    fn clone(&self) -> Self {
        Self {
            persistence: self.persistence.clone(),
            job_type: PhantomData,
            codec: self.codec.clone(),
        }
    }
}

impl MySqlStorage<()> {
    /// Perform migrations for storage
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &MySqlPool) -> Result<(), Error> {
        Self::migrations().run(pool).await.map_err(Error::from)?;
        Ok(())
    }

    /// Get mysql migrations without running them
    #[cfg(feature = "migrate")]
    #[must_use]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("./migrations")
    }
}

impl<Args> MySqlStorage<Args> {
    /// Create a new MySqlStorage
    #[must_use]
    pub fn new(pool: &MySqlPool) -> MySqlStorage<Args> {
        let config = Config::default().queue(std::any::type_name::<Args>());
        MySqlStorage {
            job_type: PhantomData,
            codec: JsonCodec::default(),
            persistence: Persisted::new(MySqlPersistence {
                pool: pool.clone(),
                config,
            }),
        }
    }

    /// Create a new MySqlStorage with a custom configuration
    #[must_use]
    pub fn with_config(mut self, config: Config) -> MySqlStorage<Args> {
        self.persistence.config = config;
        self
    }

    /// Get the underlying pool
    #[must_use]
    pub fn pool(&self) -> &MySqlPool {
        &self.persistence.pool
    }

    /// Get the underlying configuration
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.persistence.config
    }
}

impl<Args> Backend for MySqlStorage<Args> {
    type Task = MySqlTask;
    type Error = Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.persistence
            .poll_ready(cx, worker, self.config().heartbeat_interval)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<MySqlTask, Self::Error>>> {
        self.persistence.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.persistence.poll_close(cx, worker)
    }
}

impl<Args> BackendConfig for MySqlStorage<Args> {
    type Args = Args;

    type Id = Ulid;

    type Kind = Durable;

    type Config = Config;

    type Layer = TaskPersistLayer<JsonCodec<Value>, Value>;

    fn config(&self) -> &Self::Config {
        &self.persistence.config
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        self.persistence
            .layer(JsonCodec::default(), self.config().batch_size)
            .persist_results(self.config().persist_results)
            .lock_tasks(self.config().lock_tasks)
    }
}

impl<Args> WireFormatBackend for MySqlStorage<Args> {
    type Codec = JsonCodec<Vec<u8>>;

    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<Args> TryNewBackend for MySqlStorage<Args> {
    type Backend = BeforeStart<Self, Self::Error>;

    fn try_new(config: Self::Config) -> Result<Self::Backend, Self::Error> {
        let url = config
            .database_url
            .as_deref()
            .ok_or_else(|| sqlx::Error::Configuration("No database url provided".into()))?;
        let pool = MySqlPoolOptions::new()
            .connect_lazy(url)
            .map_err(Error::Database)?;

        Ok(MySqlStorage::new(&pool).before_start(|s| {
            let pool = s.pool().clone();
            async move {
                MySqlStorage::setup(&pool).await?;
                Ok(())
            }
            .boxed()
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env, time::Duration};

    use apalis::prelude::*;
    use apalis_workflow::{SteppedFlow, sink::WorkflowSink};
    use futures::{StreamExt, stream};
    use serde::{Deserialize, Serialize};
    use sqlx::MySqlPool;

    use super::*;

    fn filter(status: Option<apalis_core::task::status::Status>) -> apalis_core::backend::Filter {
        apalis_core::backend::Filter {
            status,
            page: 1,
            page_size: Some(50),
        }
    }

    async fn pool() -> MySqlPool {
        let pool = MySqlPool::connect(&env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        MySqlStorage::setup(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn basic_worker() {
        const ITEMS: usize = 10;
        let pool = pool().await;

        let mut backend = MySqlStorage::new(&pool);

        let mut start = 0;
        let mut items = stream::repeat_with(move || {
            start += 1;
            start
        })
        .take(ITEMS);
        backend.push_stream(&mut items).await.unwrap();

        async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
            if ITEMS == item {
                wrk.stop().unwrap();
            }
            Ok(())
        }

        let worker = WorkerBuilder::new(format!("rango-{}", ulid::Ulid::generate()))
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_workflow() {
        let workflow = SteppedFlow::new("odd-numbers-workflow")
            .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>()) })
            .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 3 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 5 != 0 { Some(x) } else { None } })
            .delay_for(Duration::from_millis(1000))
            .and_then(|a: Vec<usize>| async move {
                println!("Sum: {}", a.iter().sum::<usize>());
                Err::<(), BoxDynError>("Intentional Error".into())
            });

        let pool = pool().await;
        let mut mysql = MySqlStorage::new(&pool)
            .with_config(Config::default().queue("workflow-queue").batch_size(10));

        mysql.push_start(100usize).await.unwrap();

        let worker = WorkerBuilder::new(format!("rango-{}", ulid::Ulid::generate()))
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

        let workflow = SteppedFlow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .and_then(|input: UserInput, worker: WorkerContext| async move {
                worker.emit(format!("Preprocessing input: {}", input.text));
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
            .and_then(|a: Vec<Summary>, worker: WorkerContext| async move {
                dbg!(&a);
                worker.emit(format!("Generated {} summaries", a.len()));
                worker.stop()
            });

        let pool = pool().await;
        let mut backend =
            MySqlStorage::new(&pool).with_config(Config::default().queue("test").batch_size(20));

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_string(),
        };
        backend.push(input).await.unwrap();

        let worker = WorkerBuilder::new(format!("rango-{}", ulid::Ulid::generate()))
            .backend(backend)
            .data(PipelineConfig {
                min_confidence: 0.8,
                enable_sentiment: true,
            })
            .build(workflow);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn list_queues_reports_stats() {
        use apalis_core::backend::{ListQueues, TaskSink};

        let pool = pool().await;
        let queue = format!("stats-{}", ulid::Ulid::generate());
        let mut backend =
            MySqlStorage::<u32>::new(&pool).with_config(Config::default().queue(queue.clone()));

        backend.push(42u32).await.unwrap();

        let queues = backend.list_queues().await.unwrap();
        let info = queues
            .iter()
            .find(|info| info.name == queue)
            .expect("queue is listed");

        let stat = |title: &str| {
            info.stats
                .iter()
                .find(|stat| stat.title == title)
                .unwrap_or_else(|| panic!("{title} is reported"))
                .value
                .clone()
        };

        assert_eq!(stat("TOTAL_JOBS"), "1");
        assert_eq!(stat("PENDING_JOBS"), "1");
        assert_eq!(stat("ACTIVE_JOBS"), "1");
        assert_eq!(stat("DONE_JOBS"), "0");
        assert!(info.workers.is_empty());
    }

    #[tokio::test]
    async fn fetch_by_id_returns_the_task() {
        use apalis_core::backend::{FetchById, TaskSink};

        let pool = pool().await;
        let queue = format!("fetch-{}", ulid::Ulid::generate());
        let mut backend =
            MySqlStorage::<u32>::new(&pool).with_config(Config::default().queue(queue.clone()));

        backend.push(42u32).await.unwrap();

        let task = backend
            .list_tasks(&filter(None))
            .await
            .unwrap()
            .pop()
            .expect("a task is listed");
        let id = task.task_id().expect("the task has an id").clone();

        let mut backend =
            MySqlStorage::<u32>::new(&pool).with_config(Config::default().queue(queue.clone()));
        let fetched = backend
            .fetch_by_id(&id)
            .await
            .unwrap()
            .expect("the task exists");
        assert_eq!(fetched.task_id(), Some(&id));
    }

    #[tokio::test]
    async fn push_and_complete_a_task() {
        use apalis_core::backend::TaskSink;

        let pool = pool().await;
        let queue = format!("complete-{}", ulid::Ulid::generate());
        let mut backend =
            MySqlStorage::<u32>::new(&pool).with_config(Config::default().queue(queue.clone()));

        backend.push(42u32).await.unwrap();

        async fn send_reminder(item: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
            assert_eq!(item, 42);
            wrk.stop().unwrap();
            Ok(())
        }

        let worker = WorkerBuilder::new(format!("rango-{}", ulid::Ulid::generate()))
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();

        let backend =
            MySqlStorage::<u32>::new(&pool).with_config(Config::default().queue(queue.clone()));
        let tasks = backend
            .list_tasks(&filter(Some(apalis_core::task::status::Status::Done)))
            .await
            .unwrap();
        let done = tasks
            .iter()
            .find(|task| task.status() == apalis_core::task::status::Status::Done)
            .expect("the task completed");
        assert_eq!(done.args, br#"42"#.to_vec());
    }

    #[tokio::test]
    async fn reenqueue_abandoned_reports_mismatch() {
        use apalis_core::backend::persistence::Persistence;

        let pool = pool().await;
        let queue = format!("reenqueue-{}", ulid::Ulid::generate());
        let config = Config::default().queue(queue.clone());

        let mut storage = MySqlStorage::<u32>::new(&pool).with_config(config.clone());
        storage.push(1u32).await.unwrap();
        storage.push(2u32).await.unwrap();

        let worker = WorkerContext::new(&format!("worker-{}", ulid::Ulid::generate()));
        let mut persistence = MySqlPersistence {
            pool: pool.clone(),
            config,
        };
        persistence.register(&worker).await.unwrap();

        let fetched = persistence.fetch_next(&worker).await.unwrap();
        assert_eq!(fetched.len(), 2);

        let first = fetched[0].task_id().unwrap().to_string();
        let second = fetched[1].task_id().unwrap().to_string();
        sqlx::query("UPDATE jobs SET status = 'Running' WHERE id = ?")
            .bind(&first)
            .execute(&pool)
            .await
            .unwrap();

        let err = persistence
            .reenqueue_abandoned(fetched.clone(), &worker)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::ReenqueueMismatch {
                queued: 2,
                abandoned: 1,
            }
        ));

        // The transaction rolled back, so the other task stays claimed.
        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = ?")
            .bind(&second)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "Queued");

        sqlx::query("UPDATE jobs SET status = 'Queued' WHERE id = ?")
            .bind(&first)
            .execute(&pool)
            .await
            .unwrap();

        let count = persistence
            .reenqueue_abandoned(fetched, &worker)
            .await
            .unwrap();
        assert_eq!(count, 2);

        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(&queue)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(pending, 2);
    }

    #[tokio::test]
    async fn shared_factory_creates_queues() {
        use apalis_core::backend::factory::BackendFactory;

        let mut factory =
            crate::shared::MySqlStorageFactory::new(&env::var("DATABASE_URL").unwrap());
        let queue = format!("shared-{}", ulid::Ulid::generate());

        let mut backend = factory
            .create_with_config(Config::default().queue(queue))
            .unwrap();

        let mut items = stream::repeat_with(HashMap::<String, String>::default).take(1);
        backend.push_stream(&mut items).await.unwrap();
    }
}
