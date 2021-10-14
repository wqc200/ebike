
#[cfg(test)]
mod tests {
    use crate::core::output::{CoreOutput, FinalCount};
    use crate::mysql::error::MysqlResult;
    use crate::mysql::{message, metadata};
    use crate::test::test_util::create_execution;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use std::sync::Arc;

    #[tokio::test]
    async fn show_databases() -> MysqlResult<()> {
        let mut core_execution = create_execution().await?;

        let result = core_execution.execute_query("show databases").await?;

        let mut results: Vec<RecordBatch> = vec![];
        match result {
            CoreOutput::ResultSet(_, r) => results = r,
            _ => {}
        }

        let expected = vec![
            "+--------------------+",
            "| Database           |",
            "+--------------------+",
            "| mysql              |",
            "| performance_schema |",
            "+--------------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn show_create_table() -> MysqlResult<()> {
        let mut core_execution = create_execution().await?;

        let result = core_execution.execute_query("create schema test").await?;

        let mut count = 0;
        match result {
            CoreOutput::FinalCount(f) => count = f.affect_rows,
            _ => {}
        }

        assert_eq!(1, count);

        Ok(())
    }
}