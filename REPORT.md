# IMDb Big Data Processing Report

The dataset was downloaded on 28 October 2026 from the official IMDb non-commercial dataset endpoint. Complete machine-readable results are stored under `output`; the tables below present the main results without replacing these files.

## Data preparation

### Methodological explanation

I first read all columns as strings because otherwise Spark can convert `\\N` before I count it. For every column, I count null values, `\\N` markers and empty strings, then I divide by the number of rows of its table. I chose this direct aggregation because it gives an exact result. An approximate sample was considered, but it can hide rare problems and does not really help with compressed single-file inputs.

After profiling, I apply explicit types. Impossible life dates, non-positive runtimes and end years before start years are put in quarantine. The incorrect value is changed to null in the clean table, because removing a complete person or title would lose useful information. Child records without a title or person parent are counted and sampled in the quarantine but retained: their other fields stay available, while a normal inner join cannot attach them to a false parent. Deleting each complete record was considered, but it would lose valid information. Keeping an incorrect scalar value without an audit was also considered, but it would silently contaminate calculations.

### Results

The complete percentages are in `output/preparation/null_percentages` and the five largest percentages are in `output/preparation/top_five_null_columns`.

| Rank | Table | Column | Missing percentage |
| ---: | --- | --- | ---: |
| 1 | `title.akas` | `attributes` | 99.47% |
| 2 | `title.basics` | `endYear` | 98.73% |
| 3 | `name.basics` | `deathYear` | 98.32% |
| 4 | `name.basics` | `birthYear` | 95.66% |
| 5 | `title.principals` | `job` | 80.69% |

The complete quality counts are in `output/preparation/data_quality_issues`. An audit sample of up to 1,000 rejected source records per problem is preserved as JSONL in `output/preparation/rejected_samples` with its reason. The sample limit keeps the submitted repository light, while the count is always calculated on the complete table.

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

I define a feature-length film as a `movie` of at least 40 minutes. A runtime is notably different when it is at least two standard deviations from the genre mean, and genres with fewer than 30 rated films are excluded. A robust median and median absolute deviation was considered, but the question explicitly compares with an average, so mean and standard deviation are more direct.

Popularity is the sum of IMDb votes received by all feature films of one genre and year. This is preferable to the number of films because production volume is not the same as audience interest. Mean rating was considered, but a niche film with few voters can have a very high rating. The comparison reports the vote share of the top three against all other genres.

A successful film for the language analysis has a rating of at least 7.0 and at least 1,000 votes. IMDb does not provide an original-language field, so the available `title.akas.language` field is used as a documented proxy. It represents the languages in which IMDb stores alternative titles and can overestimate the real production languages.

An early career peak requires at least 15 rated movies over at least 20 years. The highest-rated movie must occur in the first career third and the mean rating in this first third must be higher than later. Splitting the career in half was considered, but the first third gives a stricter meaning to “early”. Multi-talented professionals have at least two distinct creative principal categories. `self` and archive appearances are excluded because they are not creative jobs. Their titles with several simultaneous categories are compared to their titles with one category.

### Results

Every complete result is supplied as a CSV table:

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

The annual top-three table contains three rows for every year from 1980 to 2025. Drama is first in each of the six decade snapshots below. The three leading genres receive 44.75% of genre-attributed votes on average over the complete period, with a minimum of 38.33% and a maximum of 58.25%.

| Year | First genre | Second genre | Third genre | Top-three vote share |
| ---: | --- | --- | --- | ---: |
| 1980 | Drama | Adventure | Horror | 44.38% |
| 1990 | Drama | Comedy | Crime | 45.58% |
| 2000 | Drama | Comedy | Adventure | 44.97% |
| 2010 | Drama | Action | Adventure | 39.98% |
| 2020 | Drama | Comedy | Action | 44.82% |
| 2025 | Drama | Action | Comedy | 39.29% |

The other queries return 12,914 unusual feature-film lengths, 1,571 one-hit directors, 485 qualifying series and 7,503 actors with an early peak. The language proxy gives 59,847 successful actors across several language labels, of whom 51,041 have at least three labels. This very large result confirms the limitation of alternative-title languages explained above. There are 1,159,584 professionals with at least two creative roles. Their 711,679 movie credits with simultaneous roles have a mean rating of 6.84, against 7.00 for 1,456,396 credits with one role. It is an association and not a causal effect.

## Data enrichment

### Methodological explanation

For every genre and year, I rank feature films by rating, then by vote count for deterministic ties, and keep ten. I keep the first three IMDb principals by their official ordering. Wikipedia is the only enrichment source. I request the film page and person pages in batches, take only the human-written introductory extracts and store the exact page URL beside every text. When no page is found, the output contains a programmatic note instead of invented content.

The fetching step is separated from Spark because network requests are not transformations. It uses an identifying user agent, a delay, batches and a persistent SQLite cache. Spark performs candidate selection, schema enforcement, cleaning and final storage. A request for each record without cache was considered, but it would duplicate calls and be less respectful of the API. Scraping HTML was also considered, but the MediaWiki API is more stable and provides clearer provenance.

### Results

The final nested records are in `output/enrichment/films`. Each synopsis has `synopsis_source_url`, and each principal has `biography_source_url`. Missing pages have a note. Coverage metrics are in `output/enrichment/coverage`.

The pipeline selected 10,198 genre-year film records. English Wikipedia supplied 4,636 film introductions, or 45.46%, and 16,423 biographies among 30,333 requested principal positions, or 54.14%. The remaining records contain the required programmatic absence note and no generated replacement text.

## Streaming data processing

### Methodological explanation

The producer reads Wikimedia recent changes for the English, French and German Wikipedias and keeps main-namespace events. Spark lowercases titles and normalises their punctuation while preserving parenthetical qualifiers, then it matches them with an index of IMDb films from 1980–2025 having at least 1,000 votes and their principals. This is broad enough to capture several relevant edits during a 15–30 minute window while still having a direct IMDb relation. Removing every parenthetical suffix was considered, but it matched pages about footballers, songs or different film versions with unrelated IMDb entities having the same base name. Matching every word was rejected because common words would create too many false positives.

Event time comes from Wikimedia, not Kafka arrival time. A ten-minute watermark accepts events arriving moderately late. Event IDs are deduplicated inside this state window. Events later than the watermark can be dropped, which bounds the state and prevents unlimited memory growth. A longer watermark would retain more very late events but would also retain more state for a live stream.

### Results

Individual matching events are in `output/streaming/matches`, including the Wikipedia URL and IMDb entity. Per-batch metrics are in `output/streaming/metrics`.

The 15-minute Spark test produced 50 matching rows in 21 non-empty micro-batches. They represent 48 distinct events, 37 distinct Wikipedia pages and 38 distinct IMDb entities. There are 10 film matches and 40 person matches. By wiki, 28 matches come from English, 20 from French and 2 from German Wikipedia. Matching event times extend from 14 November 2026 at 10:03:47 UTC to 10:18:44 UTC, so multiple matches were continuously captured inside the required test window.

## Open question

### Question and interest

**After controlling for genre, which runtime band receives the strongest audience engagement?** This question is interesting because a raw comparison can confuse runtime with genre: for example, animation and drama do not have the same usual duration. No additional dataset is needed.

### Methodological explanation

I join title basics with ratings, explode genres and divide runtimes into `40–74`, `75–104`, `105–134` and `135+` minutes. For every genre and band, I calculate the film count, mean rating, median votes and mean `log(1 + votes)`. The logarithm limits the domination of a few blockbusters, while the median remains easy to interpret. I then rank runtime bands inside each genre. A regression was considered, but grouped results are more transparent for this project and remain nontrivial because they need a join, an exploded many-to-many genre relation, robust aggregation and a partitioned rank.

### Results and takeaways

The detailed table is in `output/open_question/runtime_engagement_by_genre` and the summary is in `output/open_question/runtime_engagement_summary`.

| Runtime band | Represented genres | Genres where ranked first | Average genre mean rating | Average genre median votes |
| --- | ---: | ---: | ---: | ---: |
| 40–74 | 26 | 1 | 6.47 | 31 |
| 75–104 | 26 | 2 | 6.04 | 155 |
| 105–134 | 25 | 10 | 6.30 | 420 |
| 135+ | 26 | 13 | 6.69 | 473 |

The 135+ minute band ranks first for engagement in 13 of 26 represented genres and also has the largest average of genre median votes. The 105–134 minute band is first in another ten genres. Longer films are therefore associated with stronger engagement after the comparison is made inside genres, but this grouped analysis does not establish that runtime causes engagement.
