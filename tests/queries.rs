//! Integration tests for the query modules exposed through the backend traits:
//! workers, metrics, vacuum, completion polling and orphan re-enqueueing.
//!
//! They need a MySQL server reachable through `DATABASE_URL`, the same as the
//! unit tests in `src/lib.rs`.

use std::{env, time::Duration};

use apalis::prelude::*;
use apalis_core::{
    backend::{
        Filter, ListQueues, ListTasks, ListWorkers, Metrics, Statistic, TaskSink, Vacuum,
        WaitForCompletion,
    },
    task::status::Status,
};
use apalis_mysql::{Config, Error, MySqlPool, MySqlStorage, queries};
use futures::StreamExt;
use ulid::Ulid;

async fn pool() -> MySqlPool {
    let pool = MySqlPool::connect(&env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    MySqlStorage::setup(&pool).await.unwrap();
    pool
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", Ulid::generate())
}

fn filter(status: Option<Status>) -> Filter {
    Filter {
        status,
        page: 1,
        page_size: Some(50),
    }
}

/// Pushes a single task on a fresh queue, runs it to completion and returns
/// the backend (still bound to that queue), the queue name and the task id.
async fn completed_task(pool: &MySqlPool) -> (MySqlStorage<u32>, String, TaskId) {
    let queue = unique("done");
    let mut backend =
        MySqlStorage::<u32>::new(pool).with_config(Config::default().queue(queue.clone()));
    backend.push(7u32).await.unwrap();

    async fn handler(item: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
        assert_eq!(item, 7);
        wrk.stop().unwrap();
        Ok(())
    }

    let worker = WorkerBuilder::new(unique("worker"))
        .backend(backend.clone())
        .build(handler);
    worker.run().await.unwrap();

    let done = backend
        .list_tasks(&filter(Some(Status::Done)))
        .await
        .unwrap();
    let id = done
        .first()
        .expect("the task completed")
        .task_id()
        .expect("the task has an id")
        .clone();
    (backend, queue, id)
}

async fn jobs_in_queue(pool: &MySqlPool, queue: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE job_type = ?")
        .bind(queue)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn list_workers_reports_registered_worker() {
    let pool = pool().await;
    let queue = unique("workers");
    let config = Config::default().queue(queue.clone());
    let backend = MySqlStorage::<u32>::new(&pool).with_config(config.clone());

    let worker = WorkerContext::new(&unique("worker"));
    let mut conn = pool.acquire().await.unwrap();
    queries::register_worker(&mut *conn, &config, &worker, "MySqlStorage")
        .await
        .unwrap();

    let in_queue = backend.list_workers().await.unwrap();
    let found = in_queue
        .iter()
        .find(|w| w.id == worker.name())
        .expect("the worker is listed for its queue");
    assert_eq!(found.queue, queue);
    assert_eq!(found.backend, "MySqlStorage");

    let all = backend.list_all_workers().await.unwrap();
    assert!(all.iter().any(|w| w.id == worker.name()));

    // A different queue does not see the worker.
    let other = MySqlStorage::<u32>::new(&pool)
        .with_config(Config::default().queue(unique("other-workers")));
    assert!(other.list_workers().await.unwrap().is_empty());
}

#[tokio::test]
async fn register_worker_rejects_a_live_duplicate() {
    let pool = pool().await;
    let config = Config::default().queue(unique("duplicate"));
    let worker = WorkerContext::new(&unique("worker"));
    let mut conn = pool.acquire().await.unwrap();

    queries::register_worker(&mut *conn, &config, &worker, "MySqlStorage")
        .await
        .unwrap();
    let err = queries::register_worker(&mut *conn, &config, &worker, "MySqlStorage")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::WorkerAlreadyExists(name) if name == worker.name()));
}

#[tokio::test]
async fn metrics_report_global_and_per_queue_statistics() {
    let pool = pool().await;
    let queue = unique("metrics");
    let mut backend =
        MySqlStorage::<u32>::new(&pool).with_config(Config::default().queue(queue.clone()));
    backend.push(1u32).await.unwrap();

    let global = backend.global().await.unwrap();
    assert!(global.iter().any(|s| s.title == "PENDING_JOBS"));
    assert!(global.iter().any(|s| s.title == "KILL_RATE"));

    let by_queue = backend.fetch_by_queue().await.unwrap();
    assert!(!by_queue.is_empty());
    let pending = by_queue
        .iter()
        .find(|s| s.title == "PENDING_JOBS")
        .expect("pending jobs are reported for the queue");
    assert_eq!(pending.value, "1");
}

/// Plants a task `run_at_ago` seconds in the past, finished `took` seconds
/// after it started when given.
async fn plant(pool: &MySqlPool, queue: &str, status: &str, run_at_ago: i64, took: Option<i64>) {
    sqlx::query(
        "INSERT INTO jobs (id, job_type, status, run_at, done_at, job) VALUES \
         (?, ?, ?, NOW() - INTERVAL ? SECOND, NOW() - INTERVAL ? SECOND + INTERVAL ? SECOND, '')",
    )
    .bind(Ulid::generate().to_string())
    .bind(queue)
    .bind(status)
    .bind(run_at_ago)
    .bind(run_at_ago)
    .bind(took)
    .execute(pool)
    .await
    .unwrap();
}

fn stat(stats: &[Statistic], title: &str) -> f64 {
    stats
        .iter()
        .find(|s| s.title == title)
        .unwrap_or_else(|| panic!("{title} is reported"))
        .value
        .parse()
        .unwrap_or_else(|_| panic!("{title} is a number"))
}

/// `run_at` and `done_at` are TIMESTAMP columns: the time-window stats must
/// compare them as times, not as unix seconds.
#[tokio::test]
async fn metrics_count_tasks_by_when_they_ran() {
    const MINUTE: i64 = 60;
    const HOUR: i64 = 60 * MINUTE;
    const DAY: i64 = 24 * HOUR;

    let pool = pool().await;
    let queue = unique("windows");
    let backend =
        MySqlStorage::<u32>::new(&pool).with_config(Config::default().queue(queue.clone()));

    plant(&pool, &queue, "Done", 30 * MINUTE, Some(90)).await;
    plant(&pool, &queue, "Done", 2 * HOUR, Some(30)).await;
    plant(&pool, &queue, "Running", 10 * MINUTE, None).await;
    plant(&pool, &queue, "Running", 3 * HOUR, None).await;
    plant(&pool, &queue, "Pending", 5 * MINUTE, None).await;
    plant(&pool, &queue, "Killed", 3 * DAY, None).await;
    plant(&pool, &queue, "Done", 30 * DAY, Some(60)).await;

    let by_queue = backend.fetch_by_queue().await.unwrap();
    assert_eq!(stat(&by_queue, "JOBS_PAST_HOUR"), 3.0);
    assert_eq!(stat(&by_queue, "JOBS_PAST_24_HOURS"), 5.0);
    assert_eq!(stat(&by_queue, "JOBS_PAST_7_DAYS"), 6.0);
    assert_eq!(stat(&by_queue, "STALE_RUNNING_JOBS"), 1.0);
    assert_eq!(stat(&by_queue, "QUEUE_BACKLOG"), 1.0);
    // (1.5 + 0.5 + 1.0) / 3 minutes.
    assert_eq!(stat(&by_queue, "AVG_JOB_DURATION_MINS"), 1.0);
    let longest = stat(&by_queue, "LONGEST_RUNNING_JOB_MINS");
    assert!((180.0..181.0).contains(&longest), "{longest}");

    let queues = backend.list_queues().await.unwrap();
    let info = queues
        .iter()
        .find(|info| info.name == queue)
        .expect("queue is listed");
    let five_minutes_ago = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as f64)
        - 5.0 * MINUTE as f64;
    let most_recent = stat(&info.stats, "MOST_RECENT_JOB");
    assert!(
        (most_recent - five_minutes_ago).abs() < 60.0,
        "MOST_RECENT_JOB is unix seconds: {most_recent}"
    );
    assert_eq!(stat(&info.stats, "JOBS_PAST_7_DAYS"), 6.0);
    // The killed task ran on an earlier day than the rest.
    assert!(info.activity.len() >= 2, "{:?}", info.activity);
    assert_eq!(info.activity.iter().sum::<usize>(), 6);
}

#[tokio::test]
async fn vacuum_removes_only_finished_tasks() {
    let pool = pool().await;
    let (mut backend, queue, _id) = completed_task(&pool).await;
    backend.push(8u32).await.unwrap();
    assert_eq!(jobs_in_queue(&pool, &queue).await, 2);

    // The finished task is too recent to be removed.
    backend
        .vacuum_before(Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(jobs_in_queue(&pool, &queue).await, 2);

    let removed = backend.vacuum().await.unwrap();
    assert!(removed >= 1);
    // The task that never ran is kept.
    assert_eq!(jobs_in_queue(&pool, &queue).await, 1);
}

#[tokio::test]
async fn vacuum_before_removes_old_finished_tasks() {
    let pool = pool().await;
    let (mut backend, queue, id) = completed_task(&pool).await;

    sqlx::query("UPDATE jobs SET done_at = TIMESTAMPADD(SECOND, -7200, NOW()) WHERE id = ?")
        .bind(id.to_string())
        .execute(&pool)
        .await
        .unwrap();

    let removed = backend
        .vacuum_before(Duration::from_secs(3600))
        .await
        .unwrap();
    assert!(removed >= 1);
    assert_eq!(jobs_in_queue(&pool, &queue).await, 0);
}

#[tokio::test]
async fn check_status_and_wait_for_return_completed_results() {
    let pool = pool().await;
    let (mut backend, _queue, id) = completed_task(&pool).await;

    let checked = WaitForCompletion::<()>::check_status(&mut backend, [id.clone()])
        .await
        .unwrap();
    assert_eq!(checked.len(), 1);
    assert_eq!(checked[0].task_id, id);
    assert_eq!(checked[0].status, Status::Done);
    assert!(checked[0].result.is_ok());

    let mut stream = WaitForCompletion::<()>::wait_for(&mut backend, [id.clone()]);
    let waited = stream
        .next()
        .await
        .expect("the stream yields the result")
        .unwrap();
    assert_eq!(waited.task_id, id);
    assert_eq!(waited.status, Status::Done);
    // Every requested task was reported, so the stream ends.
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn check_status_ignores_unfinished_tasks() {
    let pool = pool().await;
    let queue = unique("unfinished");
    let mut backend =
        MySqlStorage::<u32>::new(&pool).with_config(Config::default().queue(queue.clone()));
    backend.push(1u32).await.unwrap();

    let id = backend
        .list_tasks(&filter(None))
        .await
        .unwrap()
        .pop()
        .expect("a task is listed")
        .task_id()
        .expect("the task has an id")
        .clone();

    let checked = WaitForCompletion::<()>::check_status(&mut backend, [id])
        .await
        .unwrap();
    assert!(checked.is_empty());
}

#[tokio::test]
async fn reenqueue_orphaned_leaves_live_workers_alone() {
    let pool = pool().await;
    let queue = unique("alive");
    let config = Config::default().queue(queue.clone());
    let mut backend = MySqlStorage::<u32>::new(&pool).with_config(config.clone());
    backend.push(1u32).await.unwrap();

    let worker = WorkerContext::new(&unique("worker"));
    let mut conn = pool.acquire().await.unwrap();
    queries::register_worker(&mut *conn, &config, &worker, "MySqlStorage")
        .await
        .unwrap();
    sqlx::query("UPDATE jobs SET status = 'Running', lock_by = ? WHERE job_type = ?")
        .bind(worker.name())
        .bind(&queue)
        .execute(&pool)
        .await
        .unwrap();

    let count = queries::reenqueue_orphaned(&mut *conn, &queue, 3600)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn reenqueue_orphaned_returns_a_dead_workers_tasks() {
    let pool = pool().await;
    let queue = unique("orphaned");
    let config = Config::default().queue(queue.clone());
    let mut backend = MySqlStorage::<u32>::new(&pool).with_config(config.clone());
    backend.push(1u32).await.unwrap();

    let worker = WorkerContext::new(&unique("worker"));
    let mut conn = pool.acquire().await.unwrap();
    queries::register_worker(&mut *conn, &config, &worker, "MySqlStorage")
        .await
        .unwrap();
    sqlx::query("UPDATE jobs SET status = 'Running', lock_by = ? WHERE job_type = ?")
        .bind(worker.name())
        .bind(&queue)
        .execute(&pool)
        .await
        .unwrap();
    // The worker's last heartbeat was ten minutes ago.
    sqlx::query("UPDATE workers SET last_seen = NOW() - INTERVAL 600 SECOND WHERE id = ?")
        .bind(worker.name())
        .execute(&pool)
        .await
        .unwrap();

    let count = queries::reenqueue_orphaned(&mut *conn, &queue, 300)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let (status, lock_by): (String, Option<String>) =
        sqlx::query_as("SELECT status, lock_by FROM jobs WHERE job_type = ?")
            .bind(&queue)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "Pending");
    assert_eq!(lock_by, None);
}
