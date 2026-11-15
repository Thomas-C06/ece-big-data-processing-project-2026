# IMDb Big Data Processing Report

The dataset was downloaded on 28 October 2026 from the official IMDb dataset endpoint. Complete machine-readable results are stored in `output`, the tables below show the main results without replacing these files.

## Data preparation

### Methodological explanation

First we read all the columns as strings because otherwise Spark can convert `\\N` before we counted it. For each column, we counted null values, `\\N` markers and empty strings, then divided by the number of rows of the table. We chose this direct aggregation because it gives an exact result. We also considered approximating the sample but it could lead to some rare problems.

Following profiling, explicit types were applied. Dates of life that don’t match reality, negative runtimes and negative end years before start years are not allowed. In the cleaned table, the incorrect values were set to null, as deleting a person or movie entirely from the database would be a loss of data. Deleting the child row was considered, but would lead to loss of data, So child rows missing a title or person parent were counted and included in the quarantine table, they were kept in place, as an inner join would not be able to connect them to a parent that isn’t true.

### Results

The complete percentages are in `output/preparation/null_percentages` and the five largest percentages are in `output/preparation/top_five_null_columns`.

| Rank | Table | Column | Missing percentage |
| ---: | --- | --- | ---: |
| 1 | `title.akas` | `attributes` | 99.47% |
| 2 | `title.basics` | `endYear` | 98.73% |
| 3 | `name.basics` | `deathYear` | 98.32% |
| 4 | `name.basics` | `birthYear` | 95.66% |
| 5 | `title.principals` | `job` | 80.69% |

The quality counts can be found in `output/preparation/data_quality_issues`. Up to 1000 rejected records from the source per issue are saved as a JSONL file in `output/preparation/rejected_samples` along with its reason for rejection. This limitation ensures that the repository remains lightweight but the count is computed on the full table.

| Detected quality issue | Records |
| --- | ---: |
| Principal with a missing person parent | 7,154 |
| Crew person with a missing person parent | 1,565 |
| Alternative title with a missing title parent | 36 |
| Death year before birth year | 27 |
| Known-for title with a missing title parent | 7 |
| Episode with a missing parent series | 5 |
| Non-positive runtime | 4 |

## Data exploration

### Methodological explanation

A feature-length movie is defined to be a `movie` that lasts at least 40 minutes. A difference in duration is considered to be big enough if it's at least two standard deviations away from the mean of the genre. Genres with less than 30 movies with ratings are not used. A median or MAD (median absolute deviation) measurement could have been used, but it is compared to the average.

We needed to measure a genre popularity based on the number of votes received by the films, while accounting for production volume and average ratings. But, the final approach focused on the total number of IMDb votes received by all feature films in each genre and year. Average ratings weren't used as a direct measure of popularity, because niche films can receive relatively few votes and still achieve high ratings. The comparison represents the proportion of total votes of the three most popular genres against all remaining genres.

A movie is considered successful in a language if it has a rating of at least 7.0 and 1,000 votes or more. IMDb does not provide the original language of each movie, so the title.akas.language field is used instead. This field lists the languages of alternative titles on IMDb, which means it may include languages that were not actually used in the film. As a result, the number of languages can be higher than the real number of production languages.

To identify an early career peak, a professional must have at least 15 rated movies over a career of at least 20 years. Their highest-rated movie must be in the first third of their career, and the average rating of this period must be higher than in the following years. Dividing the career into two halves was also considered, but using the first third makes the definition of an early peak more strict.

A multi-talented professional must have at least two different main creative job categories. Finally, titles with several creative categories are compared with those linked to only one category.

### Results

Every complete result is a CSV table:

| Question | Complete output |
| --- | --- |
| Unusual feature-film runtimes | `output/exploration/unusual_feature_lengths` |
| Top three genres, 1980–2025 | `output/exploration/top_three_genres_by_year` |
| Top three compared with other genres | `output/exploration/top_three_vs_other_genres` |
| Directors with one success and weaker other films | `output/exploration/one_hit_directors` |
| Series with the best episode early and the worst in the last season | `output/exploration/series_best_early_worst_last` |
| Successful actors in several language proxies | `output/exploration/actors_success_multiple_languages` |
| Successful actors in at least three language proxies | `output/exploration/actors_success_three_languages` |
| Long careers with an early peak | `output/exploration/actors_with_early_peak` |
| Multi-talented professionals | `output/exploration/multi_talented_professionals` |
| Performance with one or several simultaneous roles | `output/exploration/multi_role_performance` |

The annual top-three ranking includes three genres for each year between 1980 and 2025. Drama ranks first in all six decade snapshots shown below. Over the full period, the three most popular genres account for an average of 44,75% of all genre-related votes. This share ranges from 38,33% to 58,25%.

| Year | First genre | Second genre | Third genre | Top-three vote share |
| ---: | --- | --- | --- | ---: |
| 1980 | Drama | Adventure | Horror | 44.38% |
| 1990 | Drama | Comedy | Crime | 45.58% |
| 2000 | Drama | Comedy | Adventure | 44.97% |
| 2010 | Drama | Action | Adventure | 39.98% |
| 2020 | Drama | Comedy | Action | 44.82% |
| 2025 | Drama | Action | Comedy | 39.29% |

The other queries identify 12 914 unusual feature-film lengths, 1 571 one-hit directors, 485 qualifying series and 7 503 actors with an early career peak.

The language analysis finds 59 847 successful actors across different language labels. Among them, 51 041 have at least three labels. This high number shows the limitation of using alternative-title languages as a proxy for the actual production languages.

The analysis also identifies 1 159 584 professionals with at least two creative roles. Their 711 679 movie credits with multiple roles have an average rating of 6,84, compared with 7,00 for the 1 456 396 credits with only one role. This difference shows an association between the number of roles and ratings, but it does not prove a causal link.

## Data enrichment

### Methodological explanation

For every genre and year, feature films are ranked by rating, then by vote count to resolve ties, and the top ten are selected. The first three IMDb principals are kept according to their official ordering. Wikipedia is the only enrichment source. Film pages and person pages are requested in batches. Only the human-written introductory extracts are used, and the exact page URL is stored with each text. If no page is found, a programmatic note is added instead of generating content.

The first approach was to request each record separately without a cache, but this would lead to duplicate calls and put more load on the API. So the fetching step is kept separate from Spark, because network requests are not transformations. It uses an identifying user agent, a delay between requests, batches and a persistent SQLite cache. Spark handles candidate selection, schema enforcement, cleaning and final storage. Scraping HTML was also an idea, but the MediaWiki API is more stable and gives a better source information.

### Results

The final nested records are stored in output/enrichment/films. Each synopsis includes a synopsis_source_url, and each principal has a biography_source_url. When a page is missing, a note is added instead. Coverage metrics are stored in output/enrichment/coverage.

The pipeline selected 10 198 genre-year film records. English Wikipedia provided 4 636 film introductions, or 45,46%, and 16 423 biographies in 30 333 requested principal positions, or 54,14%. The other records contain the required programmatic absence note, with no generated replacement text.

## Streaming data processing

### Methodological explanation

The producer reads recent changes from the English, French and German Wikipedia and keeps only events from the main namespace. Spark converts titles to lowercase, normalises the punctuation and keeps parenthetical qualifiers. Then it matches them with an IMDb film index covering 1980 - 2025, with at least 1 000 votes, and their principals. This approach captures several relevant edits within a 15 - 30 minute window while keeping a direct link to IMDb films. We also thought about removing all parenthetical suffixes, but this also matched pages about footballers, songs or different film versions with unrelated IMDb entities sharing the same base name.

Event time comes from Wikimedia rather than Kafka arrival time. We first used a longer watermark, but it required too much state to be stored during the live stream and increased memory usage. So we reduced it to ten minutes, which still allows events to arrive a bit late while keeping memory usage under control. Event IDs are deduplicated within this time window. Events arriving after the watermark may be dropped, but this helps limit the amount of stored state and prevents memory usage from growing indefinitely.

### Results

Individual matching events are in `output/streaming/matches`, with the Wikipedia URL and IMDb entity. Metrics per batch are in `output/streaming/metrics`.

The 15-minute Spark test returned 50 matching rows across 21 batches containing data. These represent 48 different events, 37 Wikipedia pages and 38 IMDb entities. There are 10 film matches and 40 person matches. By language, 28 matches come from English Wikipedia, 20 from French and 2 from German. The matching event times range from 14 November 2026 at 10:03:47 UTC to 10:18:44 UTC. This shows that matches were found regularly during the whole test period.

## Open question

### Question and interest

**After controlling for genre, which runtime band receives the strongest audience engagement?** This question is interesting because comparing all films directly could mix up the effects of runtime and genre. For example, animation and drama films often have different runtimes. No extra data is needed for this analysis.

### Methodological explanation

Title basics are joined with ratings, and genres are separated. Films are then grouped into four runtime bands: 40 – 74, 75 – 104, 105 – 134 and 135+ minutes. For each genre and runtime band, the number of films, average rating, median votes and average log(1 + votes) are calculated. The logarithm reduces the impact of a few very popular films, while the median is easier to understand. Runtime bands are then ranked within each genre to see which durations get the most votes. Finally, the results are compared across genres to find which runtime band gets the strongest overall engagement. A regression was also considered, but grouped results are easier to understand and still involve several steps, including a join, genre separation, calculations and ranking.

### Results and takeaways

The detailed table is in `output/open_question/runtime_engagement_by_genre` and the summary is in `output/open_question/runtime_engagement_summary`.

| Runtime band | Represented genres | Genres where ranked first | Average genre mean rating | Average genre median votes |
| --- | ---: | ---: | ---: | ---: |
| 40–74 | 26 | 1 | 6.47 | 31 |
| 75–104 | 26 | 2 | 6.04 | 155 |
| 105–134 | 25 | 10 | 6.30 | 420 |
| 135+ | 26 | 13 | 6.69 | 473 |

The 135+ minute band ranks first in 13 of the 26 genres. It also has the highest average of genre median votes. The 105 – 134 minute band comes first in 10 other genres. These results mean that longer films tend to receive more votes, even when genres are compared separately. However, it doesn't prove that longer runtimes lead to higher engagement.
