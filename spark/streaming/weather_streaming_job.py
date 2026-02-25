from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("weather-streaming-job")
        .getOrCreate()
    )

    # Example JSON schema inferred from stream directory for demo purposes.
    stream_df = (
        spark.readStream
        .format("json")
        .option("maxFilesPerTrigger", 1)
        .load("data/sample/stream_input")
    )

    enriched = (
        stream_df
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withWatermark("event_ts", "15 minutes")
    )

    agg = (
        enriched.groupBy(
            F.window("event_ts", "10 minutes"),
            F.col("location_id"),
            F.col("event_type"),
        )
        .agg(F.count("*").alias("event_count"))
    )

    query = (
        agg.writeStream
        .format("parquet")
        .option("path", "data/output/stream_events")
        .option("checkpointLocation", "data/output/chk/weather_stream")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
