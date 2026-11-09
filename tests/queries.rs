//! Integration tests for the query modules exposed through the backend traits:
//! workers, metrics, vacuum, completion polling and orphan re-enqueueing.
//!
//! They need a MySQL server reachable through `DATABASE_URL`, the same as the
//! unit tests in `src/lib.rs`.

use std::{env, time::Duration};

use apalis::prelude::*;
use apalis_core::{
    backend::{Filter, ListTasks, ListWorkers, Metrics, TaskSink, Vacuum, WaitForCompletion},
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
