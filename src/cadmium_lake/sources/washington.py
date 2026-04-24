from __future__ import annotations

import json
import os
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from cadmium_lake.models import RawMeasurementRecord, SampleRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class WashingtonFertilizerAdapter(BaseAdapter):
    source_id = "washington_fertilizer"
    LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890") + [""]
    DETAIL_LIMIT: int | None = None

    def fetch(self) -> list[SourceFileRecord]:
        base_url = self.config["source_url"]
        html = self._download(base_url)
        html_record = self._write_raw_file("landing.html", base_url, html)
        records = [html_record]

        list_url = urljoin(base_url, "/LookupTypes/GetFertilizerList")
        detail_url = urljoin(base_url, "/LookupTypes/GetFertilizerDetail")
        auto_keys: dict[int, dict] = {}
        for letter in self.LETTERS:
            items = self._fetch_fertilizer_list(letter)
            for item in items:
                auto_keys[item["AutoKey"]] = item
        list_path = self.raw_dir / "fertilizer_list.json"
        list_path.write_text(json.dumps(list(auto_keys.values()), indent=2), encoding="utf-8")
        records.append(
            SourceFileRecord(
                file_id=stable_id(self.source_id, "fertilizer_list"),
                source_id=self.source_id,
                original_url=list_url,
                local_path=str(list_path),
                mime_type="application/json",
                sha256=self._write_and_hash_existing(list_path),
                retrieved_at=self._timestamp(),
                parser_version=self.parser_version,
            )
        )
        detail_path = self.raw_dir / "fertilizer_detail.json"
        details = self._load_existing_details(detail_path)
        fetched_auto_keys = {detail.get("_AutoKey") for detail in details if detail.get("_AutoKey") is not None}
        limit = self._detail_limit()
        selected_auto_keys = sorted(auto_keys)[:limit] if limit else sorted(auto_keys)
        detail_url = urljoin(base_url, "/LookupTypes/GetFertilizerDetail")
        with self._client() as client:
            for idx, auto_key in enumerate(selected_auto_keys, start=1):
                if auto_key in fetched_auto_keys:
                    continue
                response = client.post(detail_url, json={"autoKey": auto_key})
                response.raise_for_status()
                detail = response.json()
                detail["_AutoKey"] = auto_key
                details.append(detail)
                if idx % 100 == 0:
                    detail_path.write_text(json.dumps(details, indent=2), encoding="utf-8")
                    print(f"{self.source_id}: fetched {len(details)} product details")
        detail_path.write_text(json.dumps(details, indent=2), encoding="utf-8")
        records.append(
            SourceFileRecord(
                file_id=stable_id(self.source_id, "fertilizer_detail"),
                source_id=self.source_id,
                original_url=detail_url,
                local_path=str(detail_path),
                    mime_type="application/json",
                    sha256=self._write_and_hash_existing(detail_path),
                    retrieved_at=self._timestamp(),
                    parser_version=self.parser_version,
                )
            )
        return records

    def _detail_limit(self) -> int | None:
        raw_limit = os.environ.get("CADMIUM_LAKE_WSDA_DETAIL_LIMIT")
        if raw_limit is None:
            return self.DETAIL_LIMIT
        raw_limit = raw_limit.strip()
        if not raw_limit:
            return None
        return int(raw_limit)

    def _fetch_fertilizer_list(self, letter: str) -> list[dict]:
        base_url = self.config["source_url"]
        list_url = urljoin(base_url, "/LookupTypes/GetFertilizerList")
        with self._client() as client:
            response = client.get(list_url, params={"selectedLetter": letter})
            response.raise_for_status()
            return response.json()

    def _fetch_fertilizer_detail(self, auto_key: int) -> dict:
        base_url = self.config["source_url"]
        detail_url = urljoin(base_url, "/LookupTypes/GetFertilizerDetail")
        with self._client() as client:
            response = client.post(detail_url, json={"autoKey": auto_key})
            response.raise_for_status()
            return response.json()

    def _load_existing_details(self, path) -> list[dict]:
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        details_path = self.raw_dir / "fertilizer_detail.json"
        if not details_path.exists():
            return payload
        study = StudyRecord(
            study_id=stable_id(self.source_id, "wsda", "current"),
            source_id=self.source_id,
            study_title="WSDA fertilizer product database representative slice",
            publication_year=2026,
            notes="Parsed from official WSDA fertilizer JSON list/detail endpoints.",
        )
        payload.studies_or_batches.append(study)
        details = json.loads(details_path.read_text(encoding="utf-8"))
        parsed_rows = []
        for detail in details:
            product = detail.get("Product") or {}
            sample_name = str(product.get("prod_name", "")).strip()
            if not sample_name:
                continue
            raw_text = f"{product.get('cd_met_type', '')}{product.get('pc_cd_metals', '')}".strip()
            value = first_numeric([str(product.get("pc_cd_metals", "")).strip()])
            sample_id = stable_id(self.source_id, product.get("prod_number"), sample_name)
            payload.samples.append(
                SampleRecord(
                    sample_id=sample_id,
                    source_id=self.source_id,
                    study_id=study.study_id,
                    matrix_group="fertilizer",
                    matrix_subtype="fertilizer_product",
                    sample_name=sample_name,
                    specimen_or_part="product",
                    country="US",
                    publication_year=study.publication_year,
                    year_for_plotting=study.publication_year,
                    year_for_plotting_source="publication_year",
                    comments=f"WSDA product #{product.get('prod_number')}",
                )
            )
            payload.measurements_raw.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(sample_id, "cadmium"),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=value,
                    raw_value_text=raw_text,
                    raw_unit="ppm",
                    nondetect_flag=raw_text.startswith("<"),
                    detection_qualifier=product.get("cd_met_type"),
                    page_or_sheet="fertilizer_detail.json",
                    table_or_figure=product.get("prod_number"),
                    row_label=sample_name,
                    column_label="pc_cd_metals",
                    extraction_method="official_json_endpoint",
                    confidence_score=0.95,
                )
            )
            parsed_rows.append(
                {
                    "sample_id": sample_id,
                    "sample_name": sample_name,
                    "prod_number": product.get("prod_number"),
                    "raw_value": value,
                    "raw_value_text": raw_text,
                    "raw_unit": "ppm",
                }
            )
        self._write_staging_json("parsed_rows.json", parsed_rows)
        return deduplicate_payload(payload)

def first_numeric(values) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(str(value).replace(",", ""))
        except ValueError:
            continue
    return None


def infer_unit_from_text(text: str) -> str | None:
    lowered = text.lower().replace("µ", "u")
    for unit in ["mg/kg", "ug/kg", "ug/g", "ng/g", "ppm"]:
        if unit in lowered:
            return unit
    return None


def deduplicate_payload(payload: ParsedPayload) -> ParsedPayload:
    payload.samples = list({sample.sample_id: sample for sample in payload.samples}.values())
    payload.measurements_raw = list(
        {measurement.measurement_id: measurement for measurement in payload.measurements_raw}.values()
    )
    return payload
