# Architecture Diagram

```mermaid
flowchart LR
  A[Weather APIs / Sensor Feeds] --> B[Python ETL Extract]
  B --> C[RAW Layer in Snowflake VARIANT]
  C --> D[ELT SQL Transformations]
  D --> E[CURATED Dim/Fact Tables]
  E --> F[ANALYTICS Views + Materialized Views]
  A --> G[Spark Structured Streaming]
  G --> H[Stream Event Lakehouse Output]
  H --> E
  E --> I[Dashboard: Streamlit]
  J[Security: RBAC / Masking / Row Access / Tags] --> C
  J --> E
  K[Governance + Monitoring + Query History] --> F
```

## Data Flow
1. Ingest API JSON into `RAW.weather_api_raw` as `VARIANT`.
2. Flatten nested data and merge into curated dimensions/facts.
3. Run batch Spark jobs for historical aggregations.
4. Run Spark streaming for near-real-time event windows.
5. Serve BI via analytics layer and dashboard.
