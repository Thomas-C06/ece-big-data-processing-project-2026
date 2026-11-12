"""Shared IMDb schemas and small Spark helpers."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


ROOT = Path("/opt/project")
DATA = ROOT / "data"
RAW = DATA / "raw"
CLEAN = DATA / "clean"
OUTPUT = ROOT / "output"


def _schema(*fields: tuple[str, object]) -> StructType:
    return StructType([StructField(name, data_type, True) for name, data_type in fields])


S = StringType()
I = IntegerType()
D = DoubleType()

SCHEMAS = {
    "title.akas": _schema(
        ("titleId", S), ("ordering", I), ("title", S), ("region", S),
        ("language", S), ("types", S), ("attributes", S),
        ("isOriginalTitle", I),
    ),
    "title.basics": _schema(
        ("tconst", S), ("titleType", S), ("primaryTitle", S),
        ("originalTitle", S), ("isAdult", I), ("startYear", I),
        ("endYear", I), ("runtimeMinutes", I), ("genres", S),
    ),
    "title.crew": _schema(("tconst", S), ("directors", S), ("writers", S)),
    "title.episode": _schema(
        ("tconst", S), ("parentTconst", S), ("seasonNumber", I),
        ("episodeNumber", I),
    ),
    "title.principals": _schema(
        ("tconst", S), ("ordering", I), ("nconst", S), ("category", S),
        ("job", S), ("characters", S),
    ),
    "title.ratings": _schema(
        ("tconst", S), ("averageRating", D), ("numVotes", I),
    ),
    "name.basics": _schema(
        ("nconst", S), ("primaryName", S), ("birthYear", I),
        ("deathYear", I), ("primaryProfession", S), ("knownForTitles", S),
    ),
}

FILES = {name: f"{name}.tsv.gz" for name in SCHEMAS}


def spark_session(name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(name)
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, table: str) -> DataFrame:
    """Read strings first, because missing-value profiling must see literal \\N."""
    return (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("quote", "\u0000")
        .csv(str(RAW / FILES[table]))
    )


def typed_frame(raw: DataFrame, table: str) -> DataFrame:
    columns = []
    for field in SCHEMAS[table].fields:
        value = F.when(
            F.col(field.name).isNull()
            | (F.trim(F.col(field.name)) == "")
            | (F.col(field.name) == "\\N"),
            F.lit(None),
        ).otherwise(F.col(field.name))
        columns.append(value.cast(field.dataType).alias(field.name))
    return raw.select(*columns)


def read_clean(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.parquet(str(CLEAN / table))


def split_values(column: str):
    return F.explode(F.split(F.col(column), ","))


def write_csv(frame: DataFrame, name: str, order_by=None) -> None:
    if order_by:
        frame = frame.orderBy(*order_by)
    (
        frame.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(str(OUTPUT / name))
    )


def feature_films(basics: DataFrame) -> DataFrame:
    return basics.filter(
        (F.col("titleType") == "movie")
        & F.col("startYear").between(1980, 2025)
        & (F.col("runtimeMinutes") >= 40)
    )

