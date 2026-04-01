from __future__ import annotations

import zipfile
from urllib.parse import urljoin

import pandas as pd

from bs4 import BeautifulSoup

from cadmium_lake.models import RawMeasurementRecord, SampleRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class FdaTdsAdapter(BaseAdapter):
    source_id = "fda_tds"

    def fetch(self) -> list[SourceFileRecord]:
        landing_url = "https://www.fda.gov/food/fda-total-diet-study-tds/fda-total-diet-study-tds-1991-2017"
        html = self._download(landing_url)
        records = [self._write_raw_file("landing.html", landing_url, html)]
        soup = BeautifulSoup(html, "lxml")
        for anchor in soup.select("a[href]"):
            href = anchor["href"]
            text = anchor.get_text(" ", strip=True).lower()
            if (
                href.lower().endswith((".xlsx", ".xls", ".csv"))
                or "/media/" in href.lower()
                or "analytical results" in text
                or "column key" in text
            ):
                file_url = urljoin(landing_url, href)
                content = self._download(file_url)
                suffix = file_url.split("?")[0].strip("/").replace("/", "_") or "fda_tds_download"
                records.append(self._write_raw_file(suffix, file_url, content))
        return records

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        files = [path for path in self._records_from_raw_dir() if path.name != "landing.html"]
        if not files:
            return payload
        study = StudyRecord(
            study_id=stable_id(self.source_id, "tds", "modern"),
            source_id=self.source_id,
            study_title="FDA TDS representative slice",
            country="US",
            notes="Representative modern TDS/TDSi food cadmium extract",
        )
        payload.studies_or_batches.append(study)
        parsed_rows = []
        for file_path in files:
            rows = load_cadmium_rows(file_path)
            for row in rows:
                lowered = {str(key).lower(): value for key, value in row.items()}
                sample_name = str(lowered.get("food name") or lowered.get("foodname") or stable_id(self.source_id, row))
                value = try_float(lowered.get("conc"))
                mb = lowered.get("mb")
                sample_id = stable_id(self.source_id, sample_name, mb)
                payload.samples.append(
                    SampleRecord(
                        sample_id=sample_id,
                        source_id=self.source_id,
                        study_id=study.study_id,
                        matrix_group="food",
                        matrix_subtype="total_diet_study",
                        sample_name=sample_name,
                        specimen_or_part="food_item",
                        edible_portion_flag=True,
                        dry_wet_basis="as sold",
                        as_sold_prepared_flag="as_sold",
                        country="US",
                        collection_date=str(mb),
                        analyte_method=str(lowered.get("method")) if lowered.get("method") is not None else None,
                        lod=try_float(lowered.get("lod")),
                        loq=try_float(lowered.get("loq")),
                        comments=f"Parsed from {file_path.name}",
                    )
                )
                raw_text = str(lowered.get("conc"))
                qualifier = str(lowered.get("result qualifier and remarks") or "").strip()
                payload.measurements_raw.append(
                    RawMeasurementRecord(
                        measurement_id=stable_id(sample_id, "cadmium"),
                        sample_id=sample_id,
                        analyte_name="cadmium",
                        raw_value=value,
                        raw_value_text=raw_text,
                        raw_unit=str(lowered.get("unit") or "mg/kg"),
                        nondetect_flag=raw_text.startswith("<") or "nd" in qualifier.lower(),
                        detection_qualifier=qualifier or None,
                        raw_basis_text="as sold",
                        page_or_sheet=file_path.name,
                        table_or_figure=str(lowered.get("mb") or "elements"),
                        row_label=sample_name,
                        column_label="Conc",
                        extraction_method="zipped_tds_text",
                        confidence_score=0.95,
                    )
                )
                parsed_rows.append(
                    {
                        "sample_id": sample_id,
                        "sample_name": sample_name,
                        "mb": mb,
                        "raw_value": value,
                        "raw_unit": str(lowered.get("unit") or "mg/kg"),
                    }
                )
        self._write_staging_json("parsed_rows.json", parsed_rows)
        payload.samples = list({sample.sample_id: sample for sample in payload.samples}.values())
        payload.measurements_raw = list({item.measurement_id: item for item in payload.measurements_raw}.values())
        return payload


def load_cadmium_rows(path):
    rows = []
    if "151752" not in path.name and "149817" not in path.name:
        return rows
    with zipfile.ZipFile(path) as zf:
        for member in zf.namelist():
            if not member.lower().startswith("elements") or not member.lower().endswith(".txt"):
                continue
            with zf.open(member) as handle:
                try:
                    frame = pd.read_csv(handle, sep="\t", encoding="utf-8")
                except UnicodeDecodeError:
                    handle.seek(0)
                    frame = pd.read_csv(handle, sep="\t", encoding="latin-1")
            frame.columns = [str(column).strip() for column in frame.columns]
            if "Element" not in frame.columns:
                continue
            cadmium = frame[frame["Element"].astype(str).str.strip().str.lower() == "cadmium"]
            rows.extend(cadmium.to_dict(orient="records"))
    return rows


def try_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None
