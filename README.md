# apalis-mysql

Background task processing in rust using `apalis` and `mysql`

## Features

- **Reliable job queue** using MySql as the backend.
- **Multiple storage types**: standard polling and `trigger` based storages.
- **Custom codecs** for serializing/deserializing job arguments as bytes.
- **Heartbeat and orphaned job re-enqueueing** for robust task processing.
- **Integration with `apalis` workers and middleware.**
- **Observability**: Monitor and manage tasks using [apalis-board](https://github.com/apalis-dev/apalis-board).

## Storage Types

- [`MySqlStorage`]: Standard polling-based storage.
- [`MySqlStorageFactory`]: Shared storage for multiple job types, reuses the connection pool and the polling loop.

The naming is designed to clearly indicate the storage mechanism and its capabilities, but under the hood the result is the `MySqlStorage` struct with different configurations.

### Polling and wakeups

MySQL has no insert notification equivalent to SQLite's update hook or
PostgreSQL's `LISTEN/NOTIFY`, so a worker discovers new tasks by polling. An
idle `MySqlStorage` re-polls on its heartbeat (`Config::heartbeat_interval`,
30 seconds by default). To pick up tasks sooner, wrap the storage in a timer
using the core polling primitives:

```rust,no_run
use apalis::prelude::*;
use apalis_mysql::*;
use std::time::Duration;

# async fn example(pool: &MySqlPool) {
let backend = MySqlStorage::<u32>::new(pool)
    .poll_with_interval(Duration::from_millis(100));
# }
```

`poll_with_interval` comes from the core `BackendExt` trait, re-exported by
`apalis::prelude`. Call `poll_with_backoff` when you need an adaptive
interval. `MySqlStorageFactory` polls on a timer already and exposes
`with_poll_interval` to tune it.

## Setting up

You need a working MySql server setup.
Here is a quick command using docker.

```sh
docker run -d --name test-mysql -e MYSQL_ROOT_PASSWORD=strong_password -p 3306:3306 mysql
```

## Examples

### Basic Worker Example

```rust,no_run
use apalis_mysql::*;
use futures::stream::{self, StreamExt};
use apalis::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let pool = MySqlPool::connect(env!("DATABASE_URL")).await.unwrap();
    MySqlStorage::setup(&pool).await.unwrap();
    let mut backend = MySqlStorage::new(&pool);

    let mut start = 0;
    let mut items = stream::repeat_with(move || {
        start += 1;
        let task = TaskBuilder::new(start)
            .run_after(Duration::from_secs(1))
            .priority(1)
            .build();
        task
    })
    .take(10);
    backend.push_all(&mut items).await.unwrap();

    async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-1")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
```

### Workflow Example

```rust,no_run
use apalis_mysql::*;
use apalis::prelude::*;
use std::time::Duration;
use apalis_workflow::{SteppedFlow, sink::WorkflowSink};

#[tokio::main]
async fn main() {
    let workflow = SteppedFlow::new("odd-numbers-workflow")
        .and_then(|a: usize| async move {
            Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>())
        })
        .filter_map(|x| async move {
            if x % 2 != 0 { Some(x) } else { None }
        })
        .filter_map(|x| async move {
            if x % 3 != 0 { Some(x) } else { None }
        })
        .filter_map(|x| async move {
            if x % 5 != 0 { Some(x) } else { None }
        })
        .delay_for(Duration::from_millis(1000))
        .and_then(|a: Vec<usize>| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
            Ok::<(), BoxDynError>(())
        });

    let pool = MySqlPool::connect(env!("DATABASE_URL")).await.unwrap();
    MySqlStorage::setup(&pool).await.unwrap();
    let mut backend = MySqlStorage::new(&pool)
        .with_config(Config::default().queue("test-workflow"));

    backend.push_start(100usize).await.unwrap();

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|ctx, ev| {
            println!("On Event = {:?}", ev);
            if matches!(ev, Event::Error(_)) {
                ctx.stop().unwrap();
            }
        })
        .build(workflow);

    worker.run().await.unwrap();
}
```

### Shared Example

This shows an example of multiple backends using the same connection.
This can improve performance if you have many types of jobs.

```rs,no_run
use apalis_mysql::*;
use futures::stream;
use apalis::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut store = MySqlStorageFactory::new(env!("DATABASE_URL"));

    MySqlStorage::setup(store.pool()).await.unwrap();

    let mut map_store = store.create().unwrap();

    let mut int_store = store.create().unwrap();

    map_store
        .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
        .await
        .unwrap();
    int_store.push(99).await.unwrap();

    async fn send_reminder<T>(
        _: T,
        task_id: TaskId,
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
```

## Observability

You can track your jobs using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
