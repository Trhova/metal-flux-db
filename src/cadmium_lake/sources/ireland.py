from __future__ import annotations

import json
from urllib.parse import urlencode

from cadmium_lake.models import RawMeasurementRecord, SampleRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class GsiDublinSoilAdapter(BaseAdapter):
    source_id = "gsi_dublin_soil"

    LAYER_URL = (
        "https://gsi.geodata.gov.ie/server/rest/services/Geochemistry/"
        "IE_GSI_Soil_Urban_Geochemistry_SURGE_Dublin_IE26_ITM/MapServer/7"
    )

    def fetch(self) -> list[SourceFileRecord]:
        records = [
            self._write_raw_file(
                "layer_metadata.json",
                f"{self.LAYER_URL}?f=pjson",
                self._download(f"{self.LAYER_URL}?f=pjson"),
            )
        ]
        features = []
        offset = 0
        page_size = 2000
        while True:
            params = {
                "where": "1=1",
                "outFields": "OBJECTID,SAMPLE_ID,X_ITM,Y_ITM,X_ING,Y_ING,CD_MGKG",
                "returnGeometry": "false",
                "f": "json",
                "resultOffset": str(offset),
                "resultRecordCount": str(page_size),
            }
            data = json.loads(self._download(f"{self.LAYER_URL}/query?{urlencode(params)}"))
            batch = data.get("features", [])
            features.extend(batch)
            if len(batch) < page_size or not data.get("exceededTransferLimit"):
                break
            offset += len(batch)
        records.append(
            self._write_raw_file(
                "cadmium_features.json",
                f"{self.LAYER_URL}/query",
                json.dumps({"features": features}, indent=2).encode("utf-8"),
            )
        )
        return records

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        features_path = self.raw_dir / "cadmium_features.json"
        if not features_path.exists():
            return payload
        data = json.loads(features_path.read_text(encoding="utf-8"))
        study = StudyRecord(
            study_id=stable_id(self.source_id, "dublin-surge-2009"),
            source_id=self.source_id,
            study_title="Dublin SURGE urban topsoil geochemistry",
            year_start=2009,
            year_end=2009,
            country="Ireland",
            citation="Geological Survey Ireland Dublin SURGE soil urban geochemistry ArcGIS service",
            notes="Query of the official GSI ArcGIS layer for Cadmium (Cd), mg/kg.",
        )
        payload.studies_or_batches.append(study)
        parsed_rows = []
        for feature in data.get("features", []):
            attrs = {str(key).upper(): value for key, value in feature.get("attributes", {}).items()}
            sample_number = clean_id(attrs.get("SAMPLE_ID")) or clean_id(attrs.get("OBJECTID"))
            value = try_float(attrs.get("CD_MGKG"))
            if sample_number is None or value is None:
                continue
            sample_id = stable_id(self.source_id, sample_number)
            payload.samples.append(
                SampleRecord(
                    sample_id=sample_id,
                    source_id=self.source_id,
                    study_id=study.study_id,
                    matrix_group="soil",
                    matrix_subtype="urban_topsoil",
                    sample_name=f"Dublin SURGE sample {sample_number}",
                    specimen_or_part="topsoil",
                    dry_wet_basis="dry_weight",
                    location_name="Dublin",
                    country="Ireland",
                    collection_year=2009,
                    year_for_plotting=2009,
                    year_for_plotting_source="study_year",
                    comments=(
                        f"OBJECTID={clean_id(attrs.get('OBJECTID'))}; X_ITM={attrs.get('X_ITM')}; "
                        f"Y_ITM={attrs.get('Y_ITM')}; X_ING={attrs.get('X_ING')}; Y_ING={attrs.get('Y_ING')}"
                    ),
                )
            )
            payload.measurements_raw.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(self.source_id, sample_number, "CD_MGKG"),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=value,
                    raw_value_text=str(attrs.get("CD_MGKG")),
                    raw_unit="mg/kg",
                    raw_basis_text="dry_weight",
                    page_or_sheet="cadmium_features.json",
                    table_or_figure="ArcGIS feature layer 7",
                    row_label=sample_number,
                    column_label="CD_MGKG",
                    extraction_method="arcgis_feature_query",
                    confidence_score=0.97,
                )
            )
            parsed_rows.append({"sample_id": sample_id, "sample_number": sample_number, "cd_mgkg": value})
        self._write_staging_json("cadmium_rows.json", parsed_rows)
        return payload


def try_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def clean_id(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    return text
