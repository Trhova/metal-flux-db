from __future__ import annotations

import math

import pandas as pd

from cadmium_lake.models import RawMeasurementRecord, SampleRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class HealthCanadaTdsTraceElementsAdapter(BaseAdapter):
    source_id = "health_canada_tds_trace_elements"

    DATA_URL = (
        "https://open.canada.ca/data/dataset/83934503-cfae-4773-b258-e336896c2c53/"
        "resource/9c988c19-07ae-4aa4-8496-84658b6b0c45/download/"
        "total-diet-study-trace-elements-results-1993-2018.csv"
    )
    DICTIONARY_URL = (
        "https://open.canada.ca/data/dataset/83934503-cfae-4773-b258-e336896c2c53/"
        "resource/441f799c-82ec-4f33-abc4-f685d94d40fa/download/"
        "data-dictionary-dictionnaire-donnees-en.txt"
    )

    def fetch(self) -> list[SourceFileRecord]:
        return [
            self._write_raw_file(
                "total-diet-study-trace-elements-results-1993-2018.csv",
                self.DATA_URL,
                self._download(self.DATA_URL),
            ),
            self._write_raw_file(
                "data-dictionary-en.txt",
                self.DICTIONARY_URL,
                self._download(self.DICTIONARY_URL),
            ),
        ]

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        data_path = self.raw_dir / "total-diet-study-trace-elements-results-1993-2018.csv"
        if not data_path.exists():
            return payload

        frame = pd.read_csv(data_path, encoding="latin-1", dtype=str)
        frame.columns = [str(column).strip() for column in frame.columns]
        cadmium = frame[
            frame["Analyte Name"].fillna("").str.contains("Cadmium", case=False, regex=False)
        ].copy()
        if cadmium.empty:
            return payload

        studies = {}
        parsed_rows = []
        for _, row in cadmium.iterrows():
            values = {column: clean_text(row.get(column)) for column in cadmium.columns}
            project_code = values.get("Project Code") or "unknown_project"
            if project_code not in studies:
                study = StudyRecord(
                    study_id=stable_id(self.source_id, project_code),
                    source_id=self.source_id,
                    study_title=values.get("Project Name") or "Canadian Total Diet Study trace elements",
                    year_start=infer_year(values.get("Sample Collection Date")),
                    year_end=infer_year(values.get("Sample Collection Date")),
                    country="Canada",
                    citation="Health Canada Canadian Total Diet Study - Trace Elements 1993-2018",
                    repository_doi="83934503-cfae-4773-b258-e336896c2c53",
                    notes=values.get("Project Description") or None,
                )
                studies[project_code] = study
                payload.studies_or_batches.append(study)

            sample_code = values.get("Sample Code") or stable_id(self.source_id, values)
            collection_date = values.get("Sample Collection Date")
            collection_year = infer_year(collection_date)
            sample_id = stable_id(self.source_id, sample_code, values.get("Food Name"), collection_date)
            result_operator = values.get("Result Operator")
            result_value = try_float(values.get("Result Value"))
            lod = try_float(values.get("LOD"))
            nondetect = result_operator in {"<", "<=", "ND"} or (
                result_value == 0 and "zero are <lod" in values.get("Result Comments", "").lower()
            )
            unit = normalize_unit(values.get("Units of measurement"))
            basis = values.get("Basis for Result") or None
            result_key = stable_id(
                sample_code,
                values.get("Analyte Name"),
                values.get("Result Value"),
                values.get("Analysis Completion Date"),
            )

            payload.samples.append(
                SampleRecord(
                    sample_id=sample_id,
                    source_id=self.source_id,
                    study_id=studies[project_code].study_id,
                    matrix_group="food",
                    matrix_subtype=values.get("Food Group") or "food_composite",
                    sample_name=values.get("Food Name") or sample_code,
                    specimen_or_part=values.get("Product Description") or None,
                    edible_portion_flag=True,
                    as_sold_prepared_flag=values.get("Intended Use") or None,
                    location_name=", ".join(
                        part
                        for part in [
                            values.get("Sampling Location City Name"),
                            values.get("Sampling Location Province"),
                        ]
                        if part and part.lower() not in {"unknown", "multiple"}
                    )
                    or None,
                    country=values.get("Sampling Location Country") or "Canada",
                    collection_date=collection_date or None,
                    collection_year=collection_year,
                    year_for_plotting=collection_year,
                    year_for_plotting_source="collection_year" if collection_year else None,
                    analyte_method=values.get("Test Method Name") or values.get("Instrumentation") or None,
                    lod=lod,
                    comments=(
                        f"sample_code={sample_code}; sample_type={values.get('Sample Type')}; "
                        f"sub_sample={values.get('Sub Sample')}; country_of_origin={values.get('Country of Origin')}; "
                        f"lab={values.get('Lab Name')}; project_code={project_code}"
                    ),
                )
            )
            payload.measurements_raw.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(self.source_id, result_key),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=result_value,
                    raw_value_text=values.get("Result Value") or values.get("Result Comments") or "",
                    raw_unit=unit,
                    nondetect_flag=nondetect,
                    detection_qualifier=result_operator or None,
                    raw_basis_text=basis,
                    page_or_sheet=data_path.name,
                    table_or_figure="Canadian Total Diet Study trace-elements CSV",
                    row_label=sample_code,
                    column_label="Result Value",
                    extraction_method="official_open_canada_csv",
                    confidence_score=0.98,
                )
            )
            parsed_rows.append(
                {
                    "sample_id": sample_id,
                    "sample_code": sample_code,
                    "food_name": values.get("Food Name"),
                    "food_group": values.get("Food Group"),
                    "result_value": result_value,
                    "unit": unit,
                    "operator": result_operator,
                }
            )

        self._write_staging_json("cadmium_rows.json", parsed_rows)
        payload.samples = list({sample.sample_id: sample for sample in payload.samples}.values())
        payload.measurements_raw = list({item.measurement_id: item for item in payload.measurements_raw}.values())
        return payload


def clean_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def try_float(value) -> float | None:
    text = clean_text(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text.lstrip("<").strip())
    except ValueError:
        return None


def infer_year(value: str | None) -> int | None:
    text = clean_text(value)
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


def normalize_unit(value: str | None) -> str | None:
    text = clean_text(value).replace("µ", "u")
    if text.lower() in {"ng/g", "nanograms per gram"}:
        return "ng/g"
    if text.lower() in {"ug/g", "micrograms per gram"}:
        return "ug/g"
    if text.lower() in {"mg/kg"}:
        return "mg/kg"
    return text or None
