"""Download the seven IMDb non-commercial datasets without extracting them."""

import argparse
import urllib.request
from pathlib import Path

BASE_URL = "https://datasets.imdbws.com"
RAW = Path("/opt/project/data/raw")
FILES = [
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
    "name.basics.tsv.gz",
]


def download(destination: Path, force: bool = False) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for filename in FILES:
        target = destination / filename
        if target.exists() and target.stat().st_size > 0 and not force:
            print(f"Keeping {target}")
            continue
        temporary = target.with_suffix(target.suffix + ".part")
        print(f"Downloading {filename}")
        urllib.request.urlretrieve(f"{BASE_URL}/{filename}", temporary)
        temporary.replace(target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    download(RAW, args.force)

