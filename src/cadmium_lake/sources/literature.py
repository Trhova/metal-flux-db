from __future__ import annotations

from urllib.parse import quote_plus

import httpx

from cadmium_lake.models import ReviewQueueRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class LiteratureSearchAdapter(BaseAdapter):
    source_id = "literature_search"

    THEMES = {
        "plant": [
            "cadmium rice grain concentration",
            "cadmium wheat grain uptake",
            "cadmium potato tuber concentration",
            "cadmium leafy vegetable uptake",
            "cadmium cocoa bean concentration",
            "soil plant transfer factor cadmium",
        ],
        "gut": [
            "cadmium bioaccessibility INFOGEST",
            "cadmium PBET UBM",
            "cadmium duplicate diet cadmium",
            "cadmium Caco-2 dietary bioavailability",
        ],
    }

    def fetch(self) -> list:
        return []

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        inventory = []
        for layer, terms in self.THEMES.items():
            for term in terms:
                inventory.extend(self._search_epmc(term, layer))
                inventory.extend(self._search_openalex(term, layer))
                inventory.extend(self._search_datacite(term, layer))
        seen = set()
        for item in inventory:
            key = (item.get("doi"), item.get("pmid"), item.get("title"))
            if key in seen:
                continue
            seen.add(key)
            study_id = stable_id(self.source_id, item.get("title"), item.get("doi"), item.get("pmid"))
            payload.studies_or_batches.append(
                StudyRecord(
                    study_id=study_id,
                    source_id=self.source_id,
                    study_title=item.get("title"),
                    year_start=item.get("year"),
                    year_end=item.get("year"),
                    country=item.get("country"),
                    citation=item.get("source_api"),
                    doi=item.get("doi"),
                    pmid=item.get("pmid"),
                    repository_doi=item.get("repository_doi"),
                    notes=f"search_query={item.get('search_query')} oa_url={item.get('oa_url')}",
                )
            )
            if item.get("review_needed", True):
                payload.review_queue.append(
                    ReviewQueueRecord(
                        review_id=stable_id("review", study_id),
                        source_id=self.source_id,
                        study_id=study_id,
                        local_path=item.get("oa_url"),
                        issue_type="ambiguous_literature_table",
                        issue_summary=f"Manual review required for {item.get('title')}",
                        parsing_feasibility=item.get("parsing_feasibility"),
                        status="open",
                        notes=f"supplements={item.get('supplement_links')}",
                    )
                )
        self._write_staging_json("literature_inventory.json", inventory)
        return payload

    def _search_epmc(self, term: str, layer: str) -> list[dict]:
        url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query={quote_plus(term)}&format=json&pageSize=5"
        data = self._json(url)
        out = []
        for item in data.get("resultList", {}).get("result", []):
            out.append(
                {
                    "layer": layer,
                    "search_query": term,
                    "source_api": "Europe PMC",
                    "title": item.get("title"),
                    "doi": item.get("doi"),
                    "pmid": item.get("pmid"),
                    "repository_doi": None,
                    "oa_url": item.get("fullTextUrlList", {}).get("fullTextUrl", [{}])[0].get("url")
                    if item.get("fullTextUrlList")
                    else None,
                    "supplement_links": [],
                    "year": safe_int(item.get("pubYear")),
                    "country": None,
                    "review_needed": True,
                    "parsing_feasibility": "metadata_only",
                }
            )
        return out

    def _search_openalex(self, term: str, layer: str) -> list[dict]:
        url = f"https://api.openalex.org/works?search={quote_plus(term)}&per-page=5"
        data = self._json(url)
        out = []
        for item in data.get("results", []):
            locations = item.get("locations", [])
            oa_url = next((loc.get("pdf_url") or loc.get("landing_page_url") for loc in locations if loc), None)
            out.append(
                {
                    "layer": layer,
                    "search_query": term,
                    "source_api": "OpenAlex",
                    "title": item.get("display_name"),
                    "doi": item.get("doi"),
                    "pmid": item.get("ids", {}).get("pmid"),
                    "repository_doi": None,
                    "oa_url": oa_url,
                    "supplement_links": [],
                    "year": safe_int(item.get("publication_year")),
                    "country": None,
                    "review_needed": True,
                    "parsing_feasibility": "metadata_only",
                }
            )
        return out

    def _search_datacite(self, term: str, layer: str) -> list[dict]:
        url = f"https://api.datacite.org/dois?query={quote_plus(term)}&page[size]=5"
        data = self._json(url)
        out = []
        for item in data.get("data", []):
            attrs = item.get("attributes", {})
            out.append(
                {
                    "layer": layer,
                    "search_query": term,
                    "source_api": "DataCite",
                    "title": (attrs.get("titles") or [{}])[0].get("title"),
                    "doi": attrs.get("doi"),
                    "pmid": None,
                    "repository_doi": attrs.get("doi"),
                    "oa_url": attrs.get("url"),
                    "supplement_links": [],
                    "year": safe_int(attrs.get("publicationYear")),
                    "country": None,
                    "review_needed": True,
                    "parsing_feasibility": "repository_review",
                }
            )
        return out

    def _json(self, url: str) -> dict:
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            response = client.get(url, headers={"User-Agent": "cadmium-lake/0.1.0"})
            response.raise_for_status()
            return response.json()


def safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
