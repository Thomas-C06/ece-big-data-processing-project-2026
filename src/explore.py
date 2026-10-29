"""Answer every question from the data-exploration section."""

from pyspark.sql import Window, functions as F
from pyspark.storagelevel import StorageLevel

from common import feature_films, read_clean, spark_session, write_csv


def main() -> None:
    spark = spark_session("imdb-exploration")
    spark.sparkContext.setLogLevel("WARN")
    basics = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")
    names = read_clean(spark, "name.basics")
    principals = read_clean(spark, "title.principals")
    crew = read_clean(spark, "title.crew")
    episodes = read_clean(spark, "title.episode")
    akas = read_clean(spark, "title.akas")

    rated_movies = (
        basics.filter((F.col("titleType") == "movie") & (F.col("runtimeMinutes") >= 40))
        .join(ratings, "tconst")
    )
    movies_by_genre = rated_movies.withColumn("genre", F.explode(F.split("genres", ",")))

    genre_runtime = movies_by_genre.groupBy("genre").agg(
        F.avg("runtimeMinutes").alias("genre_average_runtime"),
        F.stddev_samp("runtimeMinutes").alias("genre_runtime_stddev"),
        F.count("*").alias("genre_film_count"),
    )
    unusual = (
        movies_by_genre.join(genre_runtime, "genre")
        .withColumn(
            "standard_deviations_from_average",
            (F.col("runtimeMinutes") - F.col("genre_average_runtime"))
            / F.col("genre_runtime_stddev"),
        )
        .filter(
            (F.col("genre_film_count") >= 30)
            & (F.abs(F.col("standard_deviations_from_average")) >= 2.0)
        )
        .select(
            "genre", "tconst", "primaryTitle", "startYear", "runtimeMinutes",
            F.round("genre_average_runtime", 2).alias("genre_average_runtime"),
            F.round("standard_deviations_from_average", 2).alias("z_score"),
        )
    )
    write_csv(unusual, "exploration/unusual_feature_lengths", ["genre", F.desc(F.abs("z_score"))])

    period = feature_films(basics).join(ratings, "tconst").withColumn(
        "genre", F.explode(F.split("genres", ","))
    )
    popularity = period.groupBy("startYear", "genre").agg(
        F.sum("numVotes").alias("genre_votes"),
        F.countDistinct("tconst").alias("film_count"),
        F.avg("averageRating").alias("mean_rating"),
    )
    rank_window = Window.partitionBy("startYear").orderBy(
        F.desc("genre_votes"), F.asc("genre")
    )
    ranked = popularity.withColumn("rank", F.row_number().over(rank_window))
    top_three = ranked.filter(F.col("rank") <= 3).select(
        "startYear", "rank", "genre", "genre_votes", "film_count",
        F.round("mean_rating", 2).alias("mean_rating"),
    )
    write_csv(top_three, "exploration/top_three_genres_by_year", ["startYear", "rank"])

    comparison = (
        ranked.withColumn("group", F.when(F.col("rank") <= 3, "top_three").otherwise("other"))
        .groupBy("startYear", "group")
        .agg(
            F.sum("genre_votes").alias("votes"),
            F.sum("film_count").alias("genre_film_assignments"),
            F.count("genre").alias("genre_count"),
        )
        .withColumn("year_votes", F.sum("votes").over(Window.partitionBy("startYear")))
        .withColumn("vote_share_percentage", F.round(100 * F.col("votes") / F.col("year_votes"), 2))
        .drop("year_votes")
    )
    write_csv(comparison, "exploration/top_three_vs_other_genres", ["startYear", "group"])

    directors = (
        crew.select("tconst", F.explode(F.split("directors", ",")).alias("nconst"))
        .join(basics.filter(F.col("titleType") == "movie").select("tconst", "primaryTitle"), "tconst")
        .join(ratings, "tconst")
    )
    director_stats = directors.groupBy("nconst").agg(
        F.sum(F.when(F.col("averageRating") >= 8.0, 1).otherwise(0)).alias("high_rated_count"),
        F.sum(F.when(F.col("averageRating") < 8.0, 1).otherwise(0)).alias("other_movie_count"),
        F.avg(F.when(F.col("averageRating") < 8.0, F.col("averageRating"))).alias("other_average_rating"),
    )
    selected_directors = director_stats.filter(
        (F.col("high_rated_count") == 1)
        & (F.col("other_movie_count") >= 2)
        & (F.col("other_average_rating") <= 6.0)
    )
    director_result = (
        selected_directors.join(names.select("nconst", "primaryName"), "nconst")
        .join(
            directors.filter(F.col("averageRating") >= 8.0).select(
                "nconst", F.col("tconst").alias("successful_tconst"),
                F.col("primaryTitle").alias("successful_title"),
                F.col("averageRating").alias("successful_rating"),
            ),
            "nconst",
        )
        .select(
            "nconst", "primaryName", "successful_tconst", "successful_title",
            "successful_rating", "other_movie_count",
            F.round("other_average_rating", 2).alias("other_average_rating"),
        )
    )
    write_csv(director_result, "exploration/one_hit_directors", ["primaryName"])

    episode_ratings = episodes.join(ratings, "tconst").filter(
        F.col("seasonNumber").isNotNull() & F.col("episodeNumber").isNotNull()
    )
    valid_seasons = episode_ratings.groupBy("parentTconst", "seasonNumber").agg(
        F.countDistinct("tconst").alias("rated_episode_count")
    ).filter(F.col("rated_episode_count") >= 10)
    eligible_series = valid_seasons.groupBy("parentTconst").agg(
        F.countDistinct("seasonNumber").alias("qualifying_seasons"),
        F.max("seasonNumber").alias("last_season"),
    ).filter(F.col("qualifying_seasons") >= 3)
    eligible_episodes = episode_ratings.join(eligible_series, "parentTconst")
    worst_window = Window.partitionBy("parentTconst").orderBy(
        F.asc("averageRating"), F.asc("seasonNumber"), F.asc("episodeNumber")
    )
    best_window = Window.partitionBy("parentTconst").orderBy(
        F.desc("averageRating"), F.asc("seasonNumber"), F.asc("episodeNumber")
    )
    worst = eligible_episodes.withColumn("position", F.row_number().over(worst_window)).filter(
        F.col("position") == 1
    ).select(
        "parentTconst", F.col("seasonNumber").alias("worst_season"),
        F.col("episodeNumber").alias("worst_episode"),
        F.col("averageRating").alias("worst_rating"),
    )
    best = eligible_episodes.withColumn("position", F.row_number().over(best_window)).filter(
        F.col("position") == 1
    ).select(
        "parentTconst", F.col("seasonNumber").alias("best_season"),
        F.col("episodeNumber").alias("best_episode"),
        F.col("averageRating").alias("best_rating"),
    )
    series_result = (
        eligible_series.join(worst, "parentTconst").join(best, "parentTconst")
        .filter((F.col("worst_season") == F.col("last_season")) & (F.col("best_season") <= 2))
        .join(
            basics.select(F.col("tconst").alias("parentTconst"), F.col("primaryTitle").alias("series_title")),
            "parentTconst",
        )
    )
    write_csv(series_result, "exploration/series_best_early_worst_last", ["series_title"])

    actors = principals.filter(F.col("category").isin("actor", "actress"))
    successful = basics.filter(F.col("titleType") == "movie").join(ratings, "tconst").filter(
        (F.col("averageRating") >= 7.0) & (F.col("numVotes") >= 1000)
    )
    title_languages = akas.filter(F.col("language").isNotNull()).select(
        F.col("titleId").alias("tconst"), "language"
    ).distinct()
    language_success = (
        actors.join(successful.select("tconst"), "tconst")
        .join(title_languages, "tconst")
        .groupBy("nconst")
        .agg(
            F.countDistinct("language").alias("language_count"),
            F.sort_array(F.collect_set("language")).alias("languages"),
            F.countDistinct("tconst").alias("successful_movie_count"),
        )
        .filter(F.col("language_count") >= 2)
        .join(names.select("nconst", "primaryName"), "nconst")
    )
    language_output = language_success.withColumn(
        "languages", F.concat_ws(",", "languages")
    ).persist(StorageLevel.DISK_ONLY)
    write_csv(language_output, "exploration/actors_success_multiple_languages", [F.desc("language_count"), "primaryName"])
    write_csv(
        language_output.filter(F.col("language_count") >= 3),
        "exploration/actors_success_three_languages",
        [F.desc("language_count"), "primaryName"],
    )
    language_output.unpersist()

    actor_movies = (
        actors.join(
            basics.filter(F.col("titleType") == "movie").select("tconst", "startYear", "primaryTitle"),
            "tconst",
        )
        .join(ratings, "tconst")
        .filter(F.col("startYear").isNotNull())
    )
    career = actor_movies.groupBy("nconst").agg(
        F.min("startYear").alias("career_start"),
        F.max("startYear").alias("career_end"),
        F.countDistinct("tconst").alias("movie_count"),
    ).withColumn("career_span", F.col("career_end") - F.col("career_start"))
    long_careers = career.filter((F.col("career_span") >= 20) & (F.col("movie_count") >= 15)).withColumn(
        "early_limit", F.col("career_start") + F.floor(F.col("career_span") / 3)
    )
    career_movies = actor_movies.join(long_careers, "nconst")
    career_averages = career_movies.groupBy("nconst").agg(
        F.avg(F.when(F.col("startYear") <= F.col("early_limit"), F.col("averageRating"))).alias("early_average"),
        F.avg(F.when(F.col("startYear") > F.col("early_limit"), F.col("averageRating"))).alias("later_average"),
    )
    peak_window = Window.partitionBy("nconst").orderBy(
        F.desc("averageRating"), F.desc("numVotes"), F.asc("startYear")
    )
    peaks = career_movies.withColumn("position", F.row_number().over(peak_window)).filter(
        F.col("position") == 1
    ).select(
        "nconst", F.col("tconst").alias("peak_tconst"),
        F.col("primaryTitle").alias("peak_title"), F.col("startYear").alias("peak_year"),
        F.col("averageRating").alias("peak_rating"), "early_limit",
    )
    early_peaks = (
        long_careers.join(career_averages, "nconst").join(peaks, ["nconst", "early_limit"])
        .filter((F.col("peak_year") <= F.col("early_limit")) & (F.col("early_average") > F.col("later_average")))
        .join(names.select("nconst", "primaryName"), "nconst")
        .select(
            "nconst", "primaryName", "career_start", "career_end", "career_span",
            "movie_count", "peak_tconst", "peak_title", "peak_year", "peak_rating",
            F.round("early_average", 2).alias("early_average"),
            F.round("later_average", 2).alias("later_average"),
        )
    )
    write_csv(early_peaks, "exploration/actors_with_early_peak", ["primaryName"])

    creative_roles = principals.filter(F.col("category").isin(
        "actor",
        "actress",
        "casting_director",
        "cinematographer",
        "composer",
        "director",
        "editor",
        "producer",
        "production_designer",
        "writer",
    ))
    project_roles = creative_roles.groupBy("nconst", "tconst").agg(
        F.countDistinct("category").alias("role_count"),
        F.sort_array(F.collect_set("category")).alias("roles"),
    )
    multi_talented = creative_roles.groupBy("nconst").agg(
        F.countDistinct("category").alias("career_role_count"),
        F.sort_array(F.collect_set("category")).alias("career_roles"),
    ).filter(F.col("career_role_count") >= 2)
    role_performance = (
        project_roles.join(multi_talented, "nconst").join(ratings, "tconst")
        .withColumn("role_mode", F.when(F.col("role_count") >= 2, "multiple_roles").otherwise("one_role"))
    )
    multi_summary = role_performance.groupBy("role_mode").agg(
        F.countDistinct("nconst").alias("professional_count"),
        F.countDistinct("tconst").alias("title_count"),
        F.round(F.avg("averageRating"), 2).alias("mean_rating"),
        F.round(F.avg("numVotes"), 2).alias("mean_votes"),
    )
    multi_output = multi_talented.join(
        names.select("nconst", "primaryName"), "nconst"
    ).withColumn("career_roles", F.concat_ws(",", "career_roles"))
    write_csv(multi_output, "exploration/multi_talented_professionals", [F.desc("career_role_count"), "primaryName"])
    write_csv(multi_summary, "exploration/multi_role_performance", ["role_mode"])
    spark.stop()


if __name__ == "__main__":
    main()
