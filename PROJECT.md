# IMDb Big Data Processing Project

## Contributors

- Thomas-C06 — tc.perso14@gmail.com
- Alfred de Vulpian — derfladv@gmail.com

## Environment

The project uses Apache Spark 3.5.7, Apache Kafka 4.1.1 and Python 3.12. The only prerequisite is Docker Desktop with Docker Compose. A minimum of 4 GB of memory and 20 GB of free disk space is recommended. All commands below must be executed from the `Projet` directory.

## Execution guide

### 1. Build the environment

```powershell
docker compose build
```

### 2. Download the IMDb datasets

```powershell
docker compose run --rm --no-deps tools python src/download_data.py
```

The command downloads the seven official compressed files into `data/raw`. An existing complete file is not downloaded again.

### 3. Prepare and validate the data

```powershell
docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --master "local[4]" --driver-memory 3g --conf spark.ui.showConsoleProgress=false src/prepare.py
```

The cleaned Parquet tables are written to `data/clean`. Missing-value statistics, quality metrics and rejected records are written to `output/preparation`.

### 4. Execute the exploration queries

```powershell
docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --master "local[4]" --driver-memory 2g --conf spark.ui.showConsoleProgress=false src/explore.py
```

All exploration tables are written to `output/exploration` as CSV files.

### 5. Execute the enrichment pipeline

```powershell
docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --master "local[4]" --driver-memory 3g --conf spark.ui.showConsoleProgress=false src/select_enrichment.py
docker compose run --rm --no-deps tools python src/wiki_fetch.py
docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --master "local[4]" --driver-memory 3g --conf spark.ui.showConsoleProgress=false src/process_enrichment.py
```

The first command selects the ten best-rated films for every genre and year. The second command retrieves human-written introductions and biographies from English Wikipedia. It uses batches, a 0.2-second pause and a SQLite cache, so an interrupted execution can be resumed. The last command applies the Spark schema and writes the final records to `output/enrichment/films`.

### 6. Execute the open question

```powershell
docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --master "local[4]" --driver-memory 3g --conf spark.ui.showConsoleProgress=false src/open_question.py
```

The two result tables are written to `output/open_question`.

### 7. Run the 20-minute streaming test

Start Kafka and create the topic:

```powershell
docker compose up -d kafka
```

The broker has topic auto-creation enabled. Start the producer before Spark so `wikipedia_changes` exists when the consumer subscribes.

In a first terminal, start the producer:

```powershell
docker compose run --rm tools python src/wiki_producer.py --minutes 20
```

At the same time, in a second terminal, start Spark Structured Streaming:

```powershell
docker compose run --rm spark /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 3g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 src/streaming.py --minutes 20
```

Matches and batch metrics are written to `output/streaming`. Stop Kafka after the test:

```powershell
docker compose down
```

## Git workflow

Each task must be developed on its own `feature/...` branch. A pull request must be opened, reviewed and approved by another group member before it is merged. No direct commit must be made on `main`. The contribution must stay approximately equal between group members.

## AI and LLM declaration

OpenAI Codex was used during development. It was used to interpret the instructions, propose the Docker structure, write the PySpark and Python code, write the two Markdown documents and help validate execution errors. The contributor remains responsible for reviewing the code, the methodological choices and the final results.

