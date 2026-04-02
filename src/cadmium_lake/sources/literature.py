from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from cadmium_lake.models import RawMeasurementRecord, ReviewQueueRecord, SampleRecord, StudyRecord
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
            "cadmium bioaccessibility food cadmium",
        ],
    }

    CURATED_TARGETS = {
        "PMC12733840": {"layer": "plant", "search_query": "cadmium rice grain concentration"},
        "PMC12846066": {"layer": "gut", "search_query": "cadmium bioaccessibility food cadmium"},
    }

    def fetch(self) -> list:
        return []

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        inventory: list[dict] = []
        for layer, terms in self.THEMES.items():
            for term in terms:
                inventory.extend(self._search_epmc(term, layer))
                inventory.extend(self._search_pubmed(term, layer))
                inventory.extend(self._search_openalex(term, layer))
                inventory.extend(self._search_datacite(term, layer))
        inventory.extend(self._ensure_curated_targets())

        deduped = self._dedupe_inventory(inventory)
        extracted_rows = []
        for item in deduped:
            study_id = stable_id(
                self.source_id,
                item.get("pmcid"),
                item.get("title"),
                item.get("doi"),
                item.get("pmid"),
                item.get("repository_doi"),
            )
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
                    notes=(
                        f"layer={item.get('layer')} search_query={item.get('search_query')} "
                        f"pmcid={item.get('pmcid')} oa_url={item.get('oa_url')}"
                    ),
                )
            )
            item["study_id"] = study_id
            extracted = self._extract_curated_records(item, payload)
            item["extracted_records"] = extracted
            extracted_rows.append(
                {
                    "study_id": study_id,
                    "layer": item.get("layer"),
                    "pmcid": item.get("pmcid"),
                    "title": item.get("title"),
                    "extracted_records": extracted,
                    "supplement_links": item.get("supplement_links", []),
                }
            )
            if item.get("review_needed", True):
                payload.review_queue.append(
                    ReviewQueueRecord(
                        review_id=stable_id("review", study_id, item.get("issue_type", "literature")),
                        source_id=self.source_id,
                        study_id=study_id,
                        local_path=item.get("local_path") or item.get("oa_url"),
                        issue_type=item.get("issue_type", "ambiguous_literature_table"),
                        issue_summary=item.get("issue_summary")
                        or f"Manual review required for {item.get('title')}",
                        parsing_feasibility=item.get("parsing_feasibility"),
                        status="open",
                        notes=(
                            f"layer={item.get('layer')} supplements={item.get('supplement_links')} "
                            f"search_query={item.get('search_query')}"
                        ),
                    )
                )
        self._write_staging_json("literature_inventory.json", deduped)
        self._write_staging_json("curated_extractions.json", extracted_rows)
        return payload

    def _search_epmc(self, term: str, layer: str) -> list[dict]:
        url = (
            "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
            f"?query={quote_plus(term)}&format=json&pageSize=5"
        )
        data = self._json(url)
        out = []
        for item in data.get("resultList", {}).get("result", []):
            pmcid = item.get("pmcid")
            out.append(
                {
                    "layer": layer,
                    "search_query": term,
                    "source_api": "Europe PMC",
                    "title": item.get("title"),
                    "doi": item.get("doi"),
                    "pmid": item.get("pmid"),
                    "pmcid": pmcid,
                    "repository_doi": None,
                    "oa_url": self._pmc_article_url(pmcid) if pmcid else self._first_full_text_url(item),
                    "supplement_links": [],
                    "year": safe_int(item.get("pubYear")),
                    "country": None,
                    "review_needed": True,
                    "parsing_feasibility": "pmc_table_review" if pmcid else "metadata_only",
                }
            )
        return out

    def _search_pubmed(self, term: str, layer: str) -> list[dict]:
        esearch = (
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
            f"?db=pubmed&retmode=json&retmax=5&term={quote_plus(term)}"
        )
        ids = self._json(esearch).get("esearchresult", {}).get("idlist", [])
        if not ids:
            return []
        esummary = (
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
            f"?db=pubmed&retmode=json&id={','.join(ids)}"
        )
        data = self._json(esummary)
        out = []
        for pmid in ids:
            item = data.get("result", {}).get(pmid, {})
            article_ids = item.get("articleids", [])
            doi = next((entry.get("value") for entry in article_ids if entry.get("idtype") == "doi"), None)
            pmcid = next((entry.get("value") for entry in article_ids if entry.get("idtype") == "pmc"), None)
            out.append(
                {
                    "layer": layer,
                    "search_query": term,
                    "source_api": "PubMed",
                    "title": item.get("title"),
                    "doi": doi,
                    "pmid": pmid,
                    "pmcid": pmcid,
                    "repository_doi": None,
                    "oa_url": self._pmc_article_url(pmcid) if pmcid else None,
                    "supplement_links": [],
                    "year": safe_int(item.get("pubdate", "")[:4]),
                    "country": None,
                    "review_needed": True,
                    "parsing_feasibility": "pmc_table_review" if pmcid else "metadata_only",
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
            pmid = item.get("ids", {}).get("pmid")
            out.append(
                {
                    "layer": layer,
                    "search_query": term,
                    "source_api": "OpenAlex",
                    "title": item.get("display_name"),
                    "doi": normalize_doi(item.get("doi")),
                    "pmid": pmid.rsplit("/", 1)[-1] if pmid else None,
                    "pmcid": None,
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
            resource_type = (attrs.get("types") or {}).get("resourceTypeGeneral")
            if resource_type and resource_type.lower() not in {"dataset", "text", "collection"}:
                continue
            out.append(
                {
                    "layer": layer,
                    "search_query": term,
                    "source_api": "DataCite",
                    "title": (attrs.get("titles") or [{}])[0].get("title"),
                    "doi": attrs.get("doi"),
                    "pmid": None,
                    "pmcid": None,
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

    def _ensure_curated_targets(self) -> list[dict]:
        out = []
        active_layers = set(self.THEMES)
        for pmcid, meta in self.CURATED_TARGETS.items():
            if meta["layer"] not in active_layers:
                continue
            url = (
                "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
                f"?query=PMCID:{quote_plus(pmcid)}&format=json&pageSize=1"
            )
            data = self._json(url)
            result = (data.get("resultList", {}).get("result") or [None])[0]
            if not result:
                continue
            out.append(
                {
                    "layer": meta["layer"],
                    "search_query": meta["search_query"],
                    "source_api": "Europe PMC",
                    "title": result.get("title"),
                    "doi": result.get("doi"),
                    "pmid": result.get("pmid"),
                    "pmcid": result.get("pmcid"),
                    "repository_doi": None,
                    "oa_url": self._pmc_article_url(result.get("pmcid")),
                    "supplement_links": [],
                    "year": safe_int(result.get("pubYear")),
                    "country": None,
                    "review_needed": True,
                    "parsing_feasibility": "pmc_table_review",
                }
            )
        return out

    def _extract_curated_records(self, item: dict, payload: ParsedPayload) -> int:
        pmcid = item.get("pmcid")
        if pmcid not in self.CURATED_TARGETS:
            return 0
        article_url = self._pmc_article_url(pmcid)
        html = self._download_text(article_url)
        article_path = self.raw_dir / f"{pmcid}.html"
        article_path.write_text(html, encoding="utf-8")
        payload.source_files.append(
            self._write_raw_file(f"{pmcid}.html", article_url, html.encode("utf-8"))
        )
        soup = BeautifulSoup(html, "lxml")
        supplement_links = self._extract_supplement_links(soup, article_url)
        item["supplement_links"] = supplement_links
        item["local_path"] = str(article_path)
        for link in supplement_links:
            if is_downloadable_supplement(link):
                filename = safe_filename(urlparse(link).path.split("/")[-1] or f"{pmcid}_supplement")
                payload.source_files.append(self._write_raw_file(filename, link, self._download(link)))
        if pmcid == "PMC12733840":
            records = self._extract_rice_grain_table(item, soup, payload)
        elif pmcid == "PMC12846066":
            records = self._extract_bioaccessibility_table(item, soup, payload)
        else:
            records = 0
        item["review_needed"] = bool(supplement_links)
        item["issue_type"] = "supplement_review" if supplement_links else None
        item["issue_summary"] = (
            f"Supplement review required for {item.get('title')}" if supplement_links else None
        )
        return records

    def _extract_rice_grain_table(self, item: dict, soup: BeautifulSoup, payload: ParsedPayload) -> int:
        tables = soup.find_all("table")
        if not tables:
            return 0
        table = tables[0]
        rows = table.find_all("tr")
        count = 0
        for row in rows[2:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 9:
                continue
            site = clean_text(cells[0].get_text(" ", strip=True))
            value_text = clean_text(cells[8].get_text(" ", strip=True))
            value = parse_first_numeric(value_text)
            if not site or value is None:
                continue
            sample_id = stable_id(item["study_id"], "plant", site, "TCd-G")
            payload.samples.append(
                SampleRecord(
                    sample_id=sample_id,
                    source_id=self.source_id,
                    study_id=item["study_id"],
                    matrix_group="plant",
                    matrix_subtype="rice_grain",
                    sample_name=f"Rice grain at site {site}",
                    specimen_or_part="grain",
                    dry_wet_basis=None,
                    location_name=site,
                    country="China",
                    analyte_method=None,
                    comments=f"Parsed from {item.get('pmcid')} Table 1 TCd-G column",
                )
            )
            payload.measurements_raw.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(sample_id, "cadmium", "TCd-G"),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=value,
                    raw_value_text=value_text,
                    raw_unit="mg/kg",
                    nondetect_flag=False,
                    detection_qualifier=None,
                    raw_basis_text=None,
                    page_or_sheet=item.get("pmcid"),
                    table_or_figure="Table 1",
                    row_label=site,
                    column_label="TCd-G (mg/kg)",
                    extraction_method="pmc_html_table_specific",
                    confidence_score=0.97,
                )
            )
            count += 1
        return count

    def _extract_bioaccessibility_table(
        self,
        item: dict,
        soup: BeautifulSoup,
        payload: ParsedPayload,
    ) -> int:
        tables = soup.find_all("table")
        if len(tables) < 2:
            return 0
        table = tables[1]
        rows = table.find_all("tr")
        count = 0
        for row in rows[2:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            species = clean_text(cells[0].get_text(" ", strip=True))
            value_text = clean_text(cells[1].get_text(" ", strip=True))
            if not species or not value_text:
                continue
            sample_id = stable_id(item["study_id"], "gut", species, "bioaccessibility")
            payload.samples.append(
                SampleRecord(
                    sample_id=sample_id,
                    source_id=self.source_id,
                    study_id=item["study_id"],
                    matrix_group="gut",
                    matrix_subtype="in_vitro_bioaccessibility",
                    sample_name=f"{species} cadmium bioaccessibility",
                    specimen_or_part="fruiting_body",
                    country="China",
                    analyte_method="biomimetic digestion",
                    comments=f"Parsed from {item.get('pmcid')} Table 2 biological accessibility column",
                )
            )
            nondetect = value_text.upper() == "N"
            payload.measurements_raw.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(sample_id, "cadmium", "bioaccessible_fraction"),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=None if nondetect else parse_first_numeric(value_text),
                    raw_value_text=value_text,
                    raw_unit="%",
                    nondetect_flag=nondetect,
                    detection_qualifier="N" if nondetect else None,
                    raw_basis_text="bioaccessible_fraction",
                    page_or_sheet=item.get("pmcid"),
                    table_or_figure="Table 2",
                    row_label=species,
                    column_label="Biological Accessibility of Cadmium (%)",
                    extraction_method="pmc_html_table_specific",
                    confidence_score=0.94,
                )
            )
            count += 1
        return count

    def _dedupe_inventory(self, inventory: list[dict]) -> list[dict]:
        seen = set()
        deduped = []
        for item in inventory:
            key = (
                normalize_doi(item.get("doi")),
                item.get("pmcid"),
                item.get("pmid"),
                normalize_title(item.get("title")),
                item.get("layer"),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _pmc_article_url(self, pmcid: str | None) -> str | None:
        if not pmcid:
            return None
        return f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"

    def _first_full_text_url(self, item: dict) -> str | None:
        urls = item.get("fullTextUrlList", {}).get("fullTextUrl", [])
        if urls:
            return urls[0].get("url")
        return None

    def _extract_supplement_links(self, soup: BeautifulSoup, article_url: str) -> list[str]:
        links = []
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            text = clean_text(anchor.get_text(" ", strip=True)).lower()
            full_url = urljoin(article_url, href)
            if "supplement" in text or "/bin/" in href:
                links.append(full_url)
        return sorted(set(links))

    def _download_text(self, url: str) -> str:
        with self._client() as client:
            response = client.get(url)
            response.raise_for_status()
            response.encoding = response.encoding or "utf-8"
            return response.text

    def _json(self, url: str) -> dict:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers={"User-Agent": "cadmium-lake/0.1.0"})
            response.raise_for_status()
            return response.json()


def normalize_doi(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if value.startswith("https://doi.org/"):
        return value.removeprefix("https://doi.org/")
    return value


def normalize_title(value: str | None) -> str | None:
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip().lower()


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    return re.sub(r"\s+", " ", value).strip()


def parse_first_numeric(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", value)
    if not match:
        return None
    return float(match.group(0))


def safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "download.bin"


def is_downloadable_supplement(url: str) -> bool:
    path = Path(urlparse(url).path.lower())
    return path.suffix in {".pdf", ".csv", ".tsv", ".txt", ".xls", ".xlsx"}


def safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
