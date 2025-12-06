use apalis::prelude::*;
use apalis_mysql::MysqlStorage;
use sqlx::MySqlPool;

#[tokio::main]
async fn main() {
    let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    MysqlStorage::setup(&pool).await.unwrap();
    let mut backend = MysqlStorage::new(&pool);
    backend.push(42).await.unwrap();

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
