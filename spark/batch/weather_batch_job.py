from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("weather-batch-job")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    # Example: read curated hourly dataset from parquet data lake path.
    df = spark.read.parquet("data/sample/hourly_weather.parquet")

    daily = (
        df.withColumn("observed_date", F.to_date("observed_ts"))
        .groupBy("location_id", "observed_date")
        .agg(
            F.avg("temperature_c").alias("avg_temp_c"),
            F.max("wind_speed_kph").alias("max_wind_kph"),
            F.sum("precipitation_mm").alias("total_precip_mm"),
        )
    )

    # Partition output by date for pruning.
    (
        daily.repartition("observed_date")
        .write.mode("overwrite")
        .partitionBy("observed_date")
        .parquet("data/output/daily_weather")
    )

    spark.stop()


if __name__ == "__main__":
    main()
