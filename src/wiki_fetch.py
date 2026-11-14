"""Fetch human-written Wikipedia summaries in respectful, restartable batches."""

import argparse
import json
import shutil
import sqlite3
import time
from pathlib import Path

import requests


API = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "ECE-IMDb-student-project/1.0 (contact: tc.perso14@gmail.com)"


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(path)
    database.execute(
        "CREATE TABLE IF NOT EXISTS pages (kind TEXT, entity_id TEXT, label TEXT, year INTEGER, "
        "summary TEXT, source_url TEXT, note TEXT, PRIMARY KEY(kind, entity_id))"
    )
    database.commit()
    return database


def candidate_files(path: Path):
    yield from sorted(path.glob("part-*.json"))


def read_candidates(path: Path):
    for filename in candidate_files(path):
        with filename.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def queue_entities(database: sqlite3.Connection, candidates: Path) -> None:
    for record in read_candidates(candidates):
        database.execute(
            "INSERT OR IGNORE INTO pages(kind, entity_id, label, year) VALUES('film', ?, ?, ?)",
            (record["tconst"], record["title"], record["year"]),
        )
        for person in record.get("principals") or []:
            if person.get("nconst") and person.get("primaryName"):
                database.execute(
                    "INSERT OR IGNORE INTO pages(kind, entity_id, label, year) VALUES('person', ?, ?, NULL)",
                    (person["nconst"], person["primaryName"]),
                )
    database.commit()


def chunks(rows, size):
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def resolve_batch(session: requests.Session, rows: list[tuple], kind: str) -> dict[str, dict]:
    requested = {}
    titles = []
    for _, entity_id, label, year in rows:
        if kind == "film":
            guesses = [f"{label} ({year} film)", f"{label} (film)", label]
        else:
            guesses = [label]
        requested[entity_id] = guesses
        titles.extend(guesses)

    response = session.get(
        API,
        params={
            "action": "query", "format": "json", "formatversion": 2,
            "prop": "extracts|info", "exintro": 1, "explaintext": 1,
            "inprop": "url", "redirects": 1, "titles": "|".join(titles),
            "maxlag": 5,
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json().get("query", {})
    aliases = {}
    for item in payload.get("normalized", []):
        aliases[item["from"].casefold()] = item["to"]
    for item in payload.get("redirects", []):
        aliases[item["from"].casefold()] = item["to"]
    pages = {
        page.get("title", "").casefold(): page
        for page in payload.get("pages", [])
        if not page.get("missing")
    }

    resolved = {}
    for entity_id, guesses in requested.items():
        selected = None
        for guess in guesses:
            canonical = aliases.get(guess.casefold(), guess)
            canonical = aliases.get(canonical.casefold(), canonical)
            page = pages.get(canonical.casefold()) or pages.get(guess.casefold())
            if page and page.get("extract"):
                selected = page
                break
        if selected:
            resolved[entity_id] = {
                "summary": selected["extract"].strip(),
                "source_url": selected.get("fullurl"),
                "note": None,
            }
        else:
            resolved[entity_id] = {
                "summary": None,
                "source_url": None,
                "note": "No matching English Wikipedia page was available.",
            }
    return resolved


def fetch_pending(database: sqlite3.Connection, delay: float) -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    # TextExtracts returns at most 20 intro extracts per request; films try 3 titles each.
    for kind, batch_size in (("film", 6), ("person", 20)):
        while True:
            rows = database.execute(
                "SELECT kind, entity_id, label, year FROM pages "
                "WHERE kind=? AND note IS NULL AND summary IS NULL LIMIT ?",
                (kind, batch_size),
            ).fetchall()
            if not rows:
                break
            try:
                results = resolve_batch(session, rows, kind)
            except requests.RequestException as error:
                print(f"Wikipedia request failed, retrying: {error}")
                time.sleep(max(5.0, delay))
                continue
            for _, entity_id, _, _ in rows:
                result = results[entity_id]
                database.execute(
                    "UPDATE pages SET summary=?, source_url=?, note=? WHERE kind=? AND entity_id=?",
                    (result["summary"], result["source_url"], result["note"], kind, entity_id),
                )
            database.commit()
            print(f"Processed {len(rows)} {kind} pages")
            time.sleep(delay)


def export(database: sqlite3.Connection, candidates: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".part")
    with temporary.open("w", encoding="utf-8") as handle:
        for record in read_candidates(candidates):
            film = database.execute(
                "SELECT summary, source_url, note FROM pages WHERE kind='film' AND entity_id=?",
                (record["tconst"],),
            ).fetchone()
            enriched_principals = []
            for person in record.get("principals") or []:
                bio = database.execute(
                    "SELECT summary, source_url, note FROM pages WHERE kind='person' AND entity_id=?",
                    (person["nconst"],),
                ).fetchone()
                enriched_principals.append(
                    {
                        **person,
                        "biography": bio[0] if bio else None,
                        "biography_source_url": bio[1] if bio else None,
                        "biography_note": bio[2] if bio else "Principal name was not available.",
                    }
                )
            enriched = {
                **record,
                "synopsis": film[0] if film else None,
                "synopsis_source_url": film[1] if film else None,
                "synopsis_note": film[2] if film else "Film title was not available.",
                "principals": enriched_principals,
                "source": "English Wikipedia",
                "retrieval_method": "MediaWiki Action API",
            }
            handle.write(json.dumps(enriched, ensure_ascii=False) + "\n")
    temporary.replace(output)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", type=Path, default=Path("output/enrichment/candidates"))
    parser.add_argument("--cache", type=Path, default=Path("data/wikipedia_cache.sqlite"))
    parser.add_argument("--output", type=Path, default=Path("data/enrichment_raw.jsonl"))
    parser.add_argument("--delay", type=float, default=0.2)
    args = parser.parse_args()
    working_cache = Path("/tmp/wikipedia_cache.sqlite")
    if args.cache.exists():
        shutil.copy2(args.cache, working_cache)
    database = connect(working_cache)
    queue_entities(database, args.candidates)
    fetch_pending(database, max(args.delay, 0.1))
    export(database, args.candidates, args.output)
    database.close()
    args.cache.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(working_cache, args.cache)


if __name__ == "__main__":
    main()

