from __future__ import annotations

from urllib.parse import urljoin

import pandas as pd
import pyreadstat
from bs4 import BeautifulSoup

from cadmium_lake.models import RawMeasurementRecord, SampleRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class NhanesBloodCadmiumAdapter(BaseAdapter):
    source_id = "nhanes_blood_cadmium"

    DATA_FILES = [
        "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/P_PBCD.xpt",
        "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/PBCD_L.xpt",
    ]

    def fetch(self) -> list[SourceFileRecord]:
        records = []
        for file_url in self.DATA_FILES:
            xpt_content = self._download(file_url)
            filename = file_url.rsplit("/", 1)[-1]
            records.append(self._write_raw_file(filename, file_url, xpt_content))
        return records

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        xpts = [path for path in self._records_from_raw_dir() if path.suffix.lower() == ".xpt"]
        if not xpts:
            return payload
        study = StudyRecord(
            study_id=stable_id(self.source_id, "latest"),
            source_id=self.source_id,
            study_title="NHANES blood cadmium representative slice",
            year_start=2017,
            year_end=2021,
            publication_year=2021,
            country="US",
            notes="Latest available public NHANES blood cadmium-compatible lab cycle configured for v1.",
        )
        payload.studies_or_batches.append(study)
        parsed_rows = []
        for file_path in xpts:
            frame, _ = pyreadstat.read_xport(file_path)
            frame.columns = [str(column).lower() for column in frame.columns]
            cadmium_col = next(
                (
                    col
                    for col in frame.columns
                    if ("cad" in col or col.endswith("cd") or "bcd" in col) and frame[col].dtype != object
                ),
                None,
            )
            if cadmium_col is None:
                continue
            for row in frame.head(200).to_dict(orient="records"):
                respondent = row.get("seqn") or stable_id(self.source_id, row)
                sample_id = stable_id(self.source_id, respondent)
                collection_year = infer_nhanes_year(file_path.name, row.get("sddsrvyr"))
                payload.samples.append(
                    SampleRecord(
                        sample_id=sample_id,
                        source_id=self.source_id,
                        study_id=study.study_id,
                        matrix_group="blood",
                        matrix_subtype="whole_blood",
                        sample_name=f"NHANES participant {respondent}",
                        specimen_or_part="whole blood",
                        country="US",
                        collection_date=str(row.get("sddsrvyr") or ""),
                        collection_year=collection_year,
                        publication_year=study.publication_year,
                        year_for_plotting=collection_year or study.publication_year,
                        year_for_plotting_source="collection_year" if collection_year else "publication_year",
                        comments=f"Parsed from {file_path.name}",
                    )
                )
                value = try_float(row.get(cadmium_col))
                payload.measurements_raw.append(
                    RawMeasurementRecord(
                        measurement_id=stable_id(sample_id, "cadmium"),
                        sample_id=sample_id,
                        analyte_name="cadmium",
                        raw_value=value,
                        raw_value_text=str(row.get(cadmium_col)),
                        raw_unit="ug/L",
                        page_or_sheet=file_path.name,
                        table_or_figure="xpt_table",
                        row_label=str(respondent),
                        column_label=cadmium_col,
                        extraction_method="sas_xpt",
                        confidence_score=0.85,
                    )
                )
                parsed_rows.append({"sample_id": sample_id, "raw_value": value, "column": cadmium_col})
        self._write_staging_json("parsed_rows.json", parsed_rows)
        payload.samples = list({sample.sample_id: sample for sample in payload.samples}.values())
        payload.measurements_raw = list({item.measurement_id: item for item in payload.measurements_raw}.values())
        return payload


def try_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def infer_nhanes_year(filename: str, survey_cycle) -> int | None:
    for token in filename.replace(".xpt", "").split("_"):
        if token.isdigit() and len(token) == 4:
            return int(token)
    text = str(survey_cycle or "").strip()
    if text.isdigit() and len(text) == 4:
        return int(text)
    return None
