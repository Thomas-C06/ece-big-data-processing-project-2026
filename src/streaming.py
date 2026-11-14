"""Match live Wikipedia changes with a broad index of IMDb films and professionals."""

import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from pyspark.storagelevel import StorageLevel

from common import OUTPUT, read_clean, spark_session


EVENT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("comment", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("bot", StringType()),
    StructField("wiki", StringType()),
    StructField("server_url", StringType()),
])


def normalized(column):
    without_suffix = F.regexp_replace(column, r"\s+\([^)]*\)$", "")
    return F.lower(F.trim(F.regexp_replace(without_suffix, r"[^\p{L}\p{N}]+", " ")))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=20)
    parser.add_argument("--bootstrap", default="kafka:19092")
    parser.add_argument("--topic", default="wikipedia_changes")
    args = parser.parse_args()

    spark = spark_session("imdb-wikipedia-stream")
    spark.sparkContext.setLogLevel("WARN")
    basics = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")
    names = read_clean(spark, "name.basics")
    principals = read_clean(spark, "title.principals")

    popular_films = (
        basics.filter(
            (F.col("titleType") == "movie") & F.col("startYear").between(1980, 2025)
        )
        .join(ratings.filter(F.col("numVotes") >= 1000), "tconst")
        .select(
            F.col("tconst").alias("entity_id"),
            F.col("primaryTitle").alias("entity_name"),
            F.lit("film").alias("entity_type"),
        )
    )
    popular_people = (
        principals.join(
            popular_films.select(F.col("entity_id").alias("tconst")), "tconst"
        )
        .select("nconst").distinct()
        .join(names.select("nconst", "primaryName"), "nconst")
        .select(
            F.col("nconst").alias("entity_id"),
            F.col("primaryName").alias("entity_name"),
            F.lit("person").alias("entity_type"),
        )
    )
    index = (
        popular_films.unionByName(popular_people)
        .withColumn("normalized_title", normalized(F.col("entity_name")))
        .filter(F.length("normalized_title") >= 3)
        .dropDuplicates(["normalized_title", "entity_type"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    index.count()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_time", F.to_timestamp(F.from_unixtime("timestamp")))
        .withColumn("normalized_title", normalized(F.col("title")))
        .withWatermark("event_time", "10 minutes")
        .dropDuplicatesWithinWatermark(["id"])
    )
    matches = (
        events.join(F.broadcast(index), "normalized_title")
        .select(
            F.col("id").alias("event_id"), "event_time", "wiki", "type",
            F.col("title").alias("wikipedia_title"), "comment", "user",
            "entity_id", "entity_name", "entity_type",
            F.concat("server_url", F.lit("/wiki/"), F.regexp_replace("title", " ", "_")).alias("source_url"),
        )
    )

    def write_batch(batch, batch_id):
        if not batch.isEmpty():
            batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
            batch.withColumn("batch_id", F.lit(batch_id)).write.mode("append").json(
                str(OUTPUT / "streaming" / "matches")
            )
            metrics = batch.agg(
                F.count("*").alias("matching_event_count"),
                F.countDistinct("wikipedia_title").alias("distinct_page_count"),
                F.countDistinct("entity_id").alias("distinct_imdb_entity_count"),
                F.min("event_time").alias("first_event_time"),
                F.max("event_time").alias("last_event_time"),
            ).withColumn("batch_id", F.lit(batch_id))
            metrics.write.mode("append").json(str(OUTPUT / "streaming" / "metrics"))
            batch.unpersist()

    query = (
        matches.writeStream.outputMode("append").foreachBatch(write_batch)
        .option("checkpointLocation", str(OUTPUT / "streaming" / "checkpoints" / "combined"))
        .trigger(processingTime="30 seconds").start()
    )
    query.awaitTermination(args.minutes * 60)
    query.stop()
    index.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

