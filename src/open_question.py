"""Open question: how is runtime related to engagement inside each genre?"""

from pyspark.sql import Window, functions as F

from common import feature_films, read_clean, spark_session, write_csv


def main() -> None:
    spark = spark_session("imdb-open-question")
    basics = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")

    films = (
        feature_films(basics).join(ratings, "tconst")
        .withColumn("genre", F.explode(F.split("genres", ",")))
        .withColumn(
            "runtime_band",
            F.when(F.col("runtimeMinutes") < 75, "40-74")
            .when(F.col("runtimeMinutes") < 105, "75-104")
            .when(F.col("runtimeMinutes") < 135, "105-134")
            .otherwise("135+"),
        )
    )
    result = films.groupBy("genre", "runtime_band").agg(
        F.countDistinct("tconst").alias("film_count"),
        F.round(F.avg("averageRating"), 2).alias("mean_rating"),
        F.round(F.expr("percentile_approx(numVotes, 0.5)"), 0).alias("median_votes"),
        F.round(F.avg(F.log1p("numVotes")), 3).alias("mean_log_votes"),
    )
    rank_window = Window.partitionBy("genre").orderBy(F.desc("mean_log_votes"))
    result = result.withColumn("engagement_rank_inside_genre", F.dense_rank().over(rank_window))
    write_csv(result, "open_question/runtime_engagement_by_genre", ["genre", "engagement_rank_inside_genre"])

    summary = result.groupBy("runtime_band").agg(
        F.count("genre").alias("represented_genres"),
        F.sum(F.when(F.col("engagement_rank_inside_genre") == 1, 1).otherwise(0)).alias("genres_where_band_is_first"),
        F.round(F.avg("mean_rating"), 2).alias("average_of_genre_mean_ratings"),
        F.round(F.avg("median_votes"), 0).alias("average_genre_median_votes"),
    )
    write_csv(summary, "open_question/runtime_engagement_summary", ["runtime_band"])
    spark.stop()


if __name__ == "__main__":
    main()
