use std::time::Duration;

use apalis_core::backend::{Backend, Vacuum};

use crate::{MySqlStorage, error::Error};

impl<Args> Vacuum for MySqlStorage<Args>
where
    Self: Backend<Error = Error>,
    Args: Send,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        let pool = self.persistence.pool.clone();
        let res = sqlx::query_file!("queries/backend/vacuum.sql")
            .execute(&pool)
            .await?;
        Ok(res.rows_affected() as usize)
    }

    async fn vacuum_before(&mut self, duration: Duration) -> Result<usize, Self::Error> {
        let pool = self.persistence.pool.clone();
        let res = sqlx::query_file!(
            "queries/backend/vacuum_before.sql",
            -(duration.as_secs() as i64)
        )
        .execute(&pool)
        .await?;
        Ok(res.rows_affected() as usize)
    }
}
