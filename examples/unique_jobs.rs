use apalis::prelude::*;
use apalis_mysql::MySqlStorage;
use sqlx::MySqlPool;

#[tokio::main]
async fn main() {
    let dedupe_key = "a5bc4337-7789-4feb-b421-88c7231bac10";

    let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    MySqlStorage::setup(&pool).await.unwrap();
    let mut backend = MySqlStorage::new(&pool);

    let task_1 = TaskBuilder::new(42)
        .with_idempotency_key(dedupe_key)
        .build();

    let task_2 = TaskBuilder::new(43)
        .with_idempotency_key(dedupe_key)
        .build();

    backend.push_task(task_1).await.unwrap();
    backend.push_task(task_2).await.unwrap();

    async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        apalis_core::timer::sleep(std::time::Duration::from_secs(1)).await;
        assert_eq!(task, 42);
        worker.stop()?;
        Ok(())
    }
    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .build(task);
    worker.run().await.unwrap();
}
