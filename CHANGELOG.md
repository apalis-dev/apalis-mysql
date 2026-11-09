# Changelog

## Unreleased

- _breaking_: port the backend to the `apalis-core 1.0.0` backend traits:
  - `MySqlStorage<T, C, F>` is now `MySqlStorage<Args>`, built on `Persisted<MySqlPersistence>`. The codec is fixed to `JsonCodec` and `with_codec` is gone.
  - `SharedMySqlStorage` is replaced by `MySqlStorageFactory`, which implements `BackendFactory` and shares one connection pool and one polling loop between the backends it creates (`create()` / `create_with_config()`).
  - the backend error is now `apalis_mysql::Error` instead of `sqlx::Error`, and `Config` is provided by this crate.
  - the `chrono` and `time` features are gone: timestamps are plain `u64` seconds since the Unix epoch on the public API.
  - `apalis-sql` is no longer a dependency; `apalis-core 1.0.0`, `apalis-codec 1.0.0` and `sqlx 0.9` are used instead.
- _fix_: `reenqueue_abandoned` now returns `Error::ReenqueueMismatch` instead of logging a warning and committing a partial re-enqueue during graceful shutdown, matching the sqlite and postgres backends.
- _fix_: stop double-encoding the task id list in `reenqueue_abandoned`, which made the shutdown re-enqueue match zero rows.
- _fix_: MariaDB compatibility for `wait_for` and `check_status` by replacing `CAST(? AS JSON)` with `JSON_EXTRACT(?, '$')` (#27)

## [1.0.0-rc.8] - 2026-05-08

- _feat_: idempotency for tasks (#56)

## [1.0.0-rc.7] - 2026-04-11

- _bump_: introducing apalis-mysql v1.0.0-rc.7

## [1.0.0-rc.6] - 2026-03-18

- _bump_: introducing apalis-mysql v1.0.0-rc.6

## [1.0.0-rc.4] - 2026-02-21

- _bump_: introducing apalis-mysql v1.0.0-rc.4

## [1.0.0-rc.3] - 2026-02-02

- _bump_: introducing apalis-mysql v1.0.0-rc.3

## [1.0.0-rc.2] - 2026-01-10

- _bump_: introducing apalis-mysql v1.0.0-rc.2
- feat: use DateTime abstraction from apalis-sql
- bump: update apalis deps to rc.2
- _bump_: introducing apalis-mysql v1.0.0-rc.1 (#12)

## [1.0.0-beta.1] - 2025-12-06

- _chore_: introducing apalis-mysql v1 (#6)

## [0.7.0] - 2025-03-17

### 🐛 Bug Fixes

- Reenqueue orphaned before starting streaming (#507)

### 💼 Other

- Generic retry persist check (#498)
- Add associated types to the `Backend` trait (#516)

## [0.6.4] - 2024-12-03

### 🐛 Bug Fixes

- Allow polling only when worker is ready (#472)

## [0.4.9] - 2024-01-03

### 🚀 Features

- Configurable worker set as dead (#220)

## [0.4.7] - 2023-11-15

### 🐛 Bug Fixes

- Allow cargo build --all-features (#204)

## [0.4.5] - 2023-10-05

### 💼 Other

- Api to get migrations

## [0.4.4] - 2023-07-31

### 🐛 Bug Fixes

- Change approach for mysql
- Typo in mysql query
- Typo in mysql query

### 💼 Other

- Sqlx to v0.7

## [0.3.0] - 2022-06-05
