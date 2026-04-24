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
        {
            "cycle": "1999-2000",
            "year_start": 1999,
            "year_end": 2000,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/1999/DataFiles/LAB06.xpt",
        },
        {
            "cycle": "2001-2002",
            "year_start": 2001,
            "year_end": 2002,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2001/DataFiles/L06_B.xpt",
        },
        {
            "cycle": "2003-2004",
            "year_start": 2003,
            "year_end": 2004,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2003/DataFiles/L06BMT_C.xpt",
        },
        {
            "cycle": "2005-2006",
            "year_start": 2005,
            "year_end": 2006,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2005/DataFiles/PBCD_D.xpt",
        },
        {
            "cycle": "2007-2008",
            "year_start": 2007,
            "year_end": 2008,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2007/DataFiles/PBCD_E.xpt",
        },
        {
            "cycle": "2009-2010",
            "year_start": 2009,
            "year_end": 2010,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2009/DataFiles/PBCD_F.xpt",
        },
        {
            "cycle": "2011-2012",
            "year_start": 2011,
            "year_end": 2012,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2011/DataFiles/PBCD_G.xpt",
        },
        {
            "cycle": "2013-2014",
            "year_start": 2013,
            "year_end": 2014,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2013/DataFiles/PBCD_H.xpt",
        },
        {
            "cycle": "2015-2016",
            "year_start": 2015,
            "year_end": 2016,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2015/DataFiles/PBCD_I.xpt",
        },
        {
            "cycle": "2017-2020",
            "year_start": 2017,
            "year_end": 2020,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/P_PBCD.xpt",
        },
        {
            "cycle": "2021-2023",
            "year_start": 2021,
            "year_end": 2023,
            "url": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/PBCD_L.xpt",
        },
    ]

    def fetch(self) -> list[SourceFileRecord]:
        records = []
        for metadata in self.DATA_FILES:
            file_url = metadata["url"]
            xpt_content = self._download(file_url)
            filename = file_url.rsplit("/", 1)[-1]
            records.append(self._write_raw_file(filename, file_url, xpt_content))
        return records

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        xpts = [path for path in self._records_from_raw_dir() if path.suffix.lower() == ".xpt"]
        if not xpts:
            return payload
        metadata_by_filename = {item["url"].rsplit("/", 1)[-1].lower(): item for item in self.DATA_FILES}
        parsed_rows = []
        for file_path in xpts:
            metadata = metadata_by_filename.get(file_path.name.lower(), {})
            cycle = metadata.get("cycle") or file_path.stem
            study = StudyRecord(
                study_id=stable_id(self.source_id, cycle),
                source_id=self.source_id,
                study_title=f"NHANES blood cadmium {cycle}",
                year_start=metadata.get("year_start"),
                year_end=metadata.get("year_end"),
                publication_year=metadata.get("year_end"),
                country="US",
                notes="Public NHANES whole-blood cadmium laboratory data file.",
            )
            payload.studies_or_batches.append(study)
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
            for row in frame.to_dict(orient="records"):
                respondent = row.get("seqn") or stable_id(self.source_id, row)
                sample_id = stable_id(self.source_id, respondent)
                collection_year = infer_nhanes_year(file_path.name, row.get("sddsrvyr"), metadata)
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
                        comments=f"Parsed from {file_path.name}; NHANES cycle={cycle}",
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
                parsed_rows.append({"sample_id": sample_id, "raw_value": value, "column": cadmium_col, "cycle": cycle})
        self._write_staging_json("parsed_rows.json", parsed_rows)
        payload.studies_or_batches = list({study.study_id: study for study in payload.studies_or_batches}.values())
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


def infer_nhanes_year(filename: str, survey_cycle, metadata: dict | None = None) -> int | None:
    if metadata and metadata.get("year_start"):
        return int(metadata["year_start"])
    for token in filename.replace(".xpt", "").split("_"):
        if token.isdigit() and len(token) == 4:
            return int(token)
    text = str(survey_cycle or "").strip()
    if text.isdigit() and len(text) == 4:
        return int(text)
    return None
