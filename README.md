# Weather Data Analytics Platform

End-to-end weather analytics system with:
- Data warehouse design (Star + Snowflake)
- Snowflake optimization (clustering, Time Travel, semi-structured data)
- Advanced SQL analytics (CTE, window functions, partition strategy)
- Python ETL/ELT pipeline
- Apache Spark batch + streaming processing
- Security and governance (RBAC, masking, row access, tags)
- Dashboard starter (Streamlit)
- Architecture and presentation materials

## Project Structure

```text
weather_analytics_platform/
  docs/
  sql/
    ddl/
    security/
    analytics/
    performance/
  etl/
    config/
    src/
  spark/
    batch/
    streaming/
  dashboard/
  scripts/
  tests/
  architecture/
  presentation/
  data/sample/
```

## Quick Start

1. Create Snowflake objects:
   - `sql/ddl/01_create_database.sql`
   - `sql/ddl/02_create_tables_star.sql`
   - `sql/ddl/04_semi_structured_ingestion.sql`
2. Apply security:
   - `sql/security/01_rbac.sql`
   - `sql/security/02_data_masking.sql`
   - `sql/security/03_governance.sql`
3. Configure ETL: update `etl/config/settings.yaml`
4. Run Python ETL:
   - `python etl/src/main.py`
5. Run Spark jobs:
   - Batch: `spark-submit spark/batch/weather_batch_job.py`
   - Streaming: `spark-submit spark/streaming/weather_streaming_job.py`
6. Launch dashboard:
   - `streamlit run dashboard/app.py`

## Upgrade Dependencies

1. Run the upgrade helper:
   - `./scripts/upgrade.sh`
2. Re-run your application checks/jobs after upgrade:
   - ETL: `python etl/src/main.py`
   - Dashboard: `streamlit run dashboard/app.py`
