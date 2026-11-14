"""Enforce the enriched-data schema and clean text with Spark."""

from pathlib import Path
import shutil

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
)

from common import OUTPUT, ROOT, spark_session


PRINCIPAL = StructType([
    StructField("principal_rank", IntegerType()),
    StructField("nconst", StringType()),
    StructField("primaryName", StringType()),
    StructField("category", StringType()),
    StructField("biography", StringType()),
    StructField("biography_source_url", StringType()),
    StructField("biography_note", StringType()),
])

SCHEMA = StructType([
    StructField("tconst", StringType()),
    StructField("genre", StringType()),
    StructField("year", IntegerType()),
    StructField("title", StringType()),
    StructField("averageRating", DoubleType()),
    StructField("numVotes", IntegerType()),
    StructField("genre_rank", IntegerType()),
    StructField("synopsis", StringType()),
    StructField("synopsis_source_url", StringType()),
    StructField("synopsis_note", StringType()),
    StructField("principals", ArrayType(PRINCIPAL)),
    StructField("source", StringType()),
    StructField("retrieval_method", StringType()),
])


def main() -> None:
    spark = spark_session("imdb-enrichment-processing")
    frame = spark.read.schema(SCHEMA).json(str(ROOT / "data" / "enrichment_raw.jsonl"))
    cleaned = (
        frame.withColumn("title", F.trim("title"))
        .withColumn("synopsis", F.trim("synopsis"))
        .filter(
            F.col("tconst").isNotNull()
            & F.col("year").between(1980, 2025)
            & F.col("genre_rank").between(1, 10)
        )
    )
    local_films = Path("/tmp/enriched_films")
    mounted_films = OUTPUT / "enrichment" / "films"
    cleaned.write.mode("overwrite").parquet(str(local_films))
    if mounted_films.exists():
        shutil.rmtree(mounted_films)
    shutil.copytree(local_films, mounted_films)
    coverage = cleaned.agg(
        F.count("*").alias("selected_records"),
        F.sum(F.col("synopsis").isNotNull().cast("int")).alias("records_with_synopsis"),
        F.sum(F.size(F.filter("principals", lambda person: person.biography.isNotNull()))).alias("biographies_found"),
        F.sum(F.size("principals")).alias("principals_requested"),
    ).withColumn(
        "synopsis_coverage_percentage",
        F.round(100 * F.col("records_with_synopsis") / F.col("selected_records"), 2),
    ).withColumn(
        "biography_coverage_percentage",
        F.round(100 * F.col("biographies_found") / F.col("principals_requested"), 2),
    )
    coverage.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(OUTPUT / "enrichment" / "coverage")
    )
    spark.stop()


if __name__ == "__main__":
    main()
