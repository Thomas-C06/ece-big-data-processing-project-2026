"""Select the top ten rated feature films per genre and year with three principals."""

from pyspark.sql import Window, functions as F

from common import OUTPUT, feature_films, read_clean, spark_session


def main() -> None:
    spark = spark_session("imdb-enrichment-selection")
    spark.sparkContext.setLogLevel("WARN")
    basics = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")
    principals = read_clean(spark, "title.principals")
    names = read_clean(spark, "name.basics")

    candidates = (
        feature_films(basics).join(ratings, "tconst")
        .withColumn("genre", F.explode(F.split("genres", ",")))
    )
    ranking = Window.partitionBy("startYear", "genre").orderBy(
        F.desc("averageRating"), F.desc("numVotes"), F.asc("tconst")
    )
    top = candidates.withColumn("genre_rank", F.row_number().over(ranking)).filter(
        F.col("genre_rank") <= 10
    )

    first_three = (
        principals.filter(F.col("ordering") <= 3)
        .withColumnRenamed("ordering", "principal_rank")
        .join(names.select("nconst", "primaryName"), "nconst", "left")
        .groupBy("tconst")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct("principal_rank", "nconst", "primaryName", "category")
                )
            ).alias("principals")
        )
    )
    result = top.join(first_three, "tconst", "left").select(
        "tconst", "genre", F.col("startYear").alias("year"),
        F.col("primaryTitle").alias("title"), "averageRating", "numVotes",
        "genre_rank", "principals",
    )
    result.write.mode("overwrite").json(str(OUTPUT / "enrichment" / "candidates"))
    spark.stop()


if __name__ == "__main__":
    main()

