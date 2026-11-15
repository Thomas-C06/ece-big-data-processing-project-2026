"""Unit tests for Wikipedia batch resolution, without real API calls."""

import pytest
import requests

from wiki_fetch import resolve_batch


class FakeResponse:
    """Mimics the requests response: only what resolve_batch uses."""

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    """Mimics requests.Session: get() always returns the same response."""

    def __init__(self, payload):
        self.payload = payload

    def get(self, *args, **kwargs):
        return FakeResponse(self.payload)


def test_film_found_with_year_suffix():
    payload = {"query": {"pages": [{
        "title": "Heat (1995 film)",
        "extract": "Heat is a 1995 crime film.",
        "fullurl": "https://en.wikipedia.org/wiki/Heat_(1995_film)",
    }]}}
    result = resolve_batch(FakeSession(payload), [("film", "tt0113277", "Heat", 1995)], "film")
    assert result["tt0113277"]["summary"] == "Heat is a 1995 crime film."
    assert result["tt0113277"]["source_url"].endswith("Heat_(1995_film)")


def test_missing_page_gets_a_note():
    result = resolve_batch(FakeSession({"query": {"pages": []}}), [("person", "nm1", "Nobody", None)], "person")
    assert result["nm1"]["summary"] is None
    assert result["nm1"]["note"]


def test_api_error_is_retried_not_cached():
    with pytest.raises(requests.RequestException):
        resolve_batch(FakeSession({"error": {"code": "maxlag"}}), [("person", "nm1", "X", None)], "person")
