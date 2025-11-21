use apalis_core::backend::{BackendExt, ConfigExt, queue::Queue};
use ulid::Ulid;

use crate::{CompactType, SqlContext, MysqlStorage};

pub use apalis_sql::config::*;

impl<Args: Sync, D, F> ConfigExt for MysqlStorage<Args, D, F>
where
    Self:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
{
    fn get_queue(&self) -> Queue {
        self.config().queue().clone()
    }
}
