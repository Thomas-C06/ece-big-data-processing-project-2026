"""Profile, clean and validate the IMDb tables."""

from functools import reduce
from pathlib import Path
import shutil

from pyspark.sql import DataFrame, functions as F

from common import CLEAN, OUTPUT, SCHEMAS, read_raw, spark_session, typed_frame, write_csv


def profile_missing(raw: DataFrame, table: str) -> DataFrame:
    expressions = [
        F.sum(
            F.when(
                F.col(column).isNull()
                | (F.trim(F.col(column)) == "")
                | (F.col(column) == "\\N"),
                1,
            ).otherwise(0)
        ).alias(column)
        for column in raw.columns
    ]
    counts = raw.agg(F.count(F.lit(1)).alias("__total"), *expressions).first().asDict()
    total = counts.pop("__total")
    rows = [
        (table, column, total, int(counts[column]), 100.0 * counts[column] / total)
        for column in raw.columns
    ]
    return raw.sparkSession.createDataFrame(
        rows, ["table", "column", "row_count", "missing_count", "missing_percentage"]
    )


def anti_join(child: DataFrame, child_key: str, parent: DataFrame, parent_key: str) -> DataFrame:
    return child.join(
        parent.select(F.col(parent_key).alias(child_key)), child_key, "left_anti"
    )


def save_rejected(frame: DataFrame, table: str, reason: str) -> int:
    count = frame.count()
    if count:
        sample_directory = OUTPUT / "preparation" / "rejected_samples"
        sample_directory.mkdir(parents=True, exist_ok=True)
        sample_file = sample_directory / f"{table.replace('.', '_')}__{reason}.jsonl"
        with sample_file.open("w", encoding="utf-8") as handle:
            for line in frame.limit(1000).toJSON().collect():
                handle.write(line + "\n")
    return count


def write_clean(frame: DataFrame, table: str) -> None:
    """Write with Hadoop on Linux, then copy final files to the Windows mount."""
    local_target = Path("/tmp/imdb_clean") / table
    mounted_target = CLEAN / table
    frame.write.mode("overwrite").parquet(str(local_target))
    if mounted_target.exists():
        shutil.rmtree(mounted_target)
    shutil.copytree(local_target, mounted_target)


def main() -> None:
    spark = spark_session("imdb-preparation")
    spark.sparkContext.setLogLevel("WARN")
    profiles = []
    quality_rows = []

    for table in SCHEMAS:
        raw = read_raw(spark, table)
        profiles.append(profile_missing(raw, table))
        typed = typed_frame(raw, table)

        if table == "name.basics":
            impossible_life = typed.filter(F.col("deathYear") < F.col("birthYear"))
            quality_rows.append(("death_before_birth", save_rejected(
                impossible_life, table, "death_before_birth"
            )))
            typed = typed.withColumn(
                "deathYear",
                F.when(F.col("deathYear") < F.col("birthYear"), None).otherwise(F.col("deathYear")),
            )
        elif table == "title.basics":
            invalid_runtime = typed.filter(F.col("runtimeMinutes") <= 0)
            quality_rows.append(("non_positive_runtime", save_rejected(
                invalid_runtime, table, "non_positive_runtime"
            )))
            invalid_years = typed.filter(F.col("endYear") < F.col("startYear"))
            quality_rows.append(("end_before_start", save_rejected(
                invalid_years, table, "end_before_start"
            )))
            typed = (
                typed.withColumn(
                    "runtimeMinutes",
                    F.when(F.col("runtimeMinutes") <= 0, None).otherwise(F.col("runtimeMinutes")),
                )
                .withColumn(
                    "endYear",
                    F.when(F.col("endYear") < F.col("startYear"), None).otherwise(F.col("endYear")),
                )
            )

        write_clean(typed, table)

    nulls = reduce(DataFrame.unionByName, profiles)
    write_csv(nulls, "preparation/null_percentages", [F.desc("missing_percentage")])
    write_csv(nulls.orderBy(F.desc("missing_percentage")).limit(5), "preparation/top_five_null_columns")

    frames = {table: spark.read.parquet(str(CLEAN / table)) for table in SCHEMAS}
    basics = frames["title.basics"]
    names = frames["name.basics"]

    relations = [
        ("title.akas", "titleId", basics, "tconst", "missing_title"),
        ("title.crew", "tconst", basics, "tconst", "missing_title"),
        ("title.episode", "tconst", basics, "tconst", "missing_episode_title"),
        ("title.episode", "parentTconst", basics, "tconst", "missing_parent_series"),
        ("title.principals", "tconst", basics, "tconst", "missing_title"),
        ("title.principals", "nconst", names, "nconst", "missing_person"),
        ("title.ratings", "tconst", basics, "tconst", "missing_title"),
    ]
    for table, child_key, parent, parent_key, reason in relations:
        rejected = anti_join(frames[table], child_key, parent, parent_key)
        count = save_rejected(rejected, table, reason)
        quality_rows.append((f"{table}.{child_key}_{reason}", count))

    crew_people = frames["title.crew"].select(
        F.explode(F.split(F.concat_ws(",", "directors", "writers"), ",")).alias("nconst")
    ).filter(F.col("nconst") != "")
    missing_crew_people = anti_join(crew_people, "nconst", names, "nconst")
    quality_rows.append(("crew_person_missing_parent", missing_crew_people.count()))
    save_rejected(missing_crew_people, "title.crew", "missing_person")

    known_for = names.select("nconst", F.explode(F.split("knownForTitles", ",")).alias("tconst"))
    missing_known_for = anti_join(known_for, "tconst", basics, "tconst")
    quality_rows.append(("known_for_title_missing_parent", missing_known_for.count()))
    save_rejected(missing_known_for, "name.basics", "missing_known_for_title")

    quality = spark.createDataFrame(quality_rows, ["issue", "record_count"])
    write_csv(quality, "preparation/data_quality_issues", [F.desc("record_count")])
    spark.stop()


if __name__ == "__main__":
    main()
