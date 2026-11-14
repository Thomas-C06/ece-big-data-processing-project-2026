"""Forward live Wikipedia recent-change events to Kafka."""

import argparse
import json
import time

import requests
from kafka import KafkaProducer


STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "ECE-IMDb-student-project/1.0 (contact: tc.perso14@gmail.com)"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=20)
    parser.add_argument("--bootstrap", default="kafka:19092")
    parser.add_argument("--topic", default="wikipedia_changes")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        compression_type="gzip",
    )
    deadline = time.monotonic() + args.minutes * 60
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    sent = 0
    while time.monotonic() < deadline:
        try:
            with requests.get(
                STREAM_URL,
                headers=headers,
                stream=True,
                timeout=(20, 90),
            ) as response:
                response.raise_for_status()
                for raw_line in response.iter_lines(decode_unicode=True):
                    if time.monotonic() >= deadline:
                        break
                    if not raw_line or not raw_line.startswith("data:"):
                        continue
                    event = json.loads(raw_line[5:].strip())
                    if event.get("namespace") == 0 and event.get("wiki") in {
                        "enwiki",
                        "frwiki",
                        "dewiki",
                    }:
                        producer.send(args.topic, event)
                        sent += 1
                        if sent % 100 == 0:
                            producer.flush()
                            print(f"Sent {sent} events")
        except requests.RequestException as error:
            if time.monotonic() >= deadline:
                break
            print(f"Wikimedia stream disconnected: {error}. Reconnecting...")
            time.sleep(2)
    producer.flush()
    producer.close()
    print(f"Producer finished after sending {sent} events")


if __name__ == "__main__":
    main()

