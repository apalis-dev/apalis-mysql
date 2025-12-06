use apalis::prelude::*;
use apalis_mysql::MySqlPool;
use apalis_mysql::MySqlStorage;

#[tokio::main]
async fn main() {
    env_logger::init();
    let pool = MySqlPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    MySqlStorage::setup(&pool).await.unwrap();
    let mut backend = MySqlStorage::new(&pool);
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
