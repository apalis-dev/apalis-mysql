use apalis_core::{
    backend::{Backend, FetchById},
    task::{Task, task_id::TaskId},
};

use crate::{MySqlStorage, MySqlTask, error::Error, from_row::MySqlTaskRow};

impl<Args> FetchById for MySqlStorage<Args>
where
    Self: Backend<Error = Error, Task = Task<Vec<u8>>>,
    Args: 'static,
{
    fn fetch_by_id(
        &mut self,
        id: &TaskId,
    ) -> impl Future<Output = Result<Option<MySqlTask>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();
        let id = id.to_string();
        async move {
            let task = sqlx::query_file_as!(MySqlTaskRow, "queries/task/find_by_id.sql", id)
                .fetch_optional(&pool)
                .await?
                .map(|r| r.try_into())
                .transpose()?;
            Ok(task)
        }
    }
}
