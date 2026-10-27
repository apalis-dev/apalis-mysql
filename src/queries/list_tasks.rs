use apalis_core::{
    backend::{Backend, Filter, ListAllTasks, ListTasks},
    task::status::Status,
};

use crate::{MySqlStorage, MySqlTask, error::Error, from_row::MySqlTaskRow};

impl<Args> ListTasks for MySqlStorage<Args>
where
    Self: Backend<Error = Error>,
    Args: 'static,
{
    fn list_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<MySqlTask>, Self::Error>> + Send {
        let queue = self.persistence.config.queue.as_ref();
        let pool = self.persistence.pool.clone();
        let limit = filter.limit() as i32;
        let offset = filter.offset() as i32;
        let status = filter
            .status
            .as_ref()
            .unwrap_or(&Status::Pending)
            .to_string();
        async move {
            let tasks = sqlx::query_file_as!(
                MySqlTaskRow,
                "queries/backend/list_jobs.sql",
                status,
                queue,
                limit,
                offset
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}

impl<Args> ListAllTasks for MySqlStorage<Args>
where
    Self: Backend<Error = Error>,
{
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<MySqlTask>, Self::Error>> + Send {
        let status = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or(Status::Pending.to_string());
        let pool = self.persistence.pool.clone();
        let limit = filter.limit() as i32;
        let offset = filter.offset() as i32;
        async move {
            let tasks = sqlx::query_file_as!(
                MySqlTaskRow,
                "queries/backend/list_all_jobs.sql",
                status,
                limit,
                offset
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}
