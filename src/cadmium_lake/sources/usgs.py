from __future__ import annotations

import zipfile

from cadmium_lake.models import RawMeasurementRecord, SampleRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class UsgsSoilAdapter(BaseAdapter):
    source_id = "usgs_soil"

    SAMPLE_URL = "https://pubs.usgs.gov/ds/801/downloads/Appendix_3a_Ahorizon_18Sept2013.xls"

    def fetch(self) -> list[SourceFileRecord]:
        url = self.SAMPLE_URL
        content = self._download(url)
        return [self._write_raw_file("Appendix_3a_Ahorizon_18Sept2013.xls", url, content)]

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        xls_path = self.raw_dir / "Appendix_3a_Ahorizon_18Sept2013.xls"
        csv_zip_path = self.raw_dir / "usgs_soildata.zip"
        source_label = xls_path.name
        if xls_path.exists():
            df = load_usgs_excel(xls_path)
        elif csv_zip_path.exists():
            df = load_first_csv_from_zip(csv_zip_path)
            source_label = csv_zip_path.name
        else:
            return payload
        study = StudyRecord(
            study_id=stable_id(self.source_id, "ds801"),
            source_id=self.source_id,
            study_title="USGS soil geochemical representative slice",
            citation="USGS Data Series 801",
            country="US",
            notes="Representative cadmium-bearing soil table from USGS geochemical release.",
        )
        payload.studies_or_batches.append(study)
        parsed_rows = []
        for row in df.to_dict(orient="records"):
            lowered = {str(key).lower(): value for key, value in row.items()}
            cadmium_key = next(
                (key for key in lowered if "cad" in key or key.endswith("_cd") or key == "cd"),
                None,
            )
            if cadmium_key is None:
                continue
            raw_text = str(lowered[cadmium_key]).strip()
            qualifier = "<" if raw_text.startswith("<") else None
            value = try_float(raw_text.lstrip("<").strip())
            sample_name = str(lowered.get("a_labid") or lowered.get("siteid") or stable_id(self.source_id, row))
            sample_id = stable_id(self.source_id, sample_name)
            payload.samples.append(
                SampleRecord(
                    sample_id=sample_id,
                    source_id=self.source_id,
                    study_id=study.study_id,
                    matrix_group="soil",
                    matrix_subtype="topsoil",
                    sample_name=sample_name,
                    location_name=str(lowered.get("site_name") or lowered.get("state") or "usgs_site"),
                    latitude=try_float(lowered.get("latitude")),
                    longitude=try_float(lowered.get("longitude")),
                    country="US",
                    comments="Parsed from USGS soil geochemical file",
                )
            )
            payload.measurements_raw.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(sample_id, "cadmium"),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=value,
                    raw_value_text=raw_text,
                    raw_unit="mg/kg",
                    nondetect_flag=qualifier == "<",
                    detection_qualifier=qualifier,
                    raw_basis_text="dry weight",
                    page_or_sheet=source_label,
                    table_or_figure="appendix_3a",
                    row_label=sample_name,
                    column_label=cadmium_key,
                    extraction_method="spreadsheet",
                    confidence_score=0.8,
                )
            )
            parsed_rows.append({"sample_id": sample_id, "cadmium_key": cadmium_key, "raw_value": value})
        self._write_staging_json("parsed_rows.json", parsed_rows)
        return payload


def load_usgs_excel(path):
    import pandas as pd

    frame = pd.read_excel(path, header=12)
    frame = frame.iloc[1:].reset_index(drop=True)
    frame.columns = [str(column).strip() for column in frame.columns]
    return frame


def load_first_csv_from_zip(path):
    import pandas as pd

    with zipfile.ZipFile(path) as zf:
        for member in zf.namelist():
            if member.lower().endswith(".csv") or member.lower().endswith(".txt"):
                with zf.open(member) as handle:
                    frame = pd.read_csv(handle)
                frame.columns = [str(column).strip() for column in frame.columns]
                return frame
    return pd.DataFrame()


def try_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None
