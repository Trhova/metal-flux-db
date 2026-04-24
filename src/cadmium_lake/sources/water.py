from __future__ import annotations

from io import BytesIO
import json
import os
import time
from urllib.parse import urlencode

import httpx
import pandas as pd

from cadmium_lake.models import RawMeasurementRecord, SampleRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class UsgsWqpWaterAdapter(BaseAdapter):
    source_id = "usgs_wqp_water"

    RESULT_ENDPOINT = "https://www.waterqualitydata.us/data/Result/search"
    DEFAULT_QUERY_WINDOWS = [
        {
            "name": "california_groundwater_cadmium_filtered",
            "params": {
                "siteid": "USGS-383000121313601",
                "pCode": "01025",
                "mimeType": "csv",
                "zip": "no",
                "dataProfile": "resultPhysChem",
            },
        },
        *[
            {
            "name": f"{slug}_cadmium_filtered_{start_year}_{end_year}",
                "params": {
                    "statecode": statecode,
                    "pCode": "01025",
                    "mimeType": "csv",
                    "zip": "no",
                    "dataProfile": "resultPhysChem",
                    "count": "no",
                    "startDateLo": f"01-01-{start_year}",
                    "startDateHi": f"12-31-{end_year}",
                },
            }
            for slug, statecode, start_year, end_year in [
                ("california", "US:06", 1990, 1999),
                ("california", "US:06", 2000, 2009),
                ("california", "US:06", 2010, 2019),
                ("california", "US:06", 2020, 2026),
                ("arizona", "US:04", 2000, 2009),
                ("arizona", "US:04", 2010, 2019),
                ("oregon", "US:41", 2000, 2009),
                ("oregon", "US:41", 2010, 2019),
                ("washington", "US:53", 2000, 2009),
                ("washington", "US:53", 2010, 2019),
                ("idaho", "US:16", 2000, 2009),
                ("idaho", "US:16", 2010, 2019),
                ("texas", "US:48", 2000, 2009),
                ("texas", "US:48", 2010, 2019),
            ]
        ],
        *[
            {
                "name": f"{slug}_wqx_cadmium_water_{start_year}_{end_year}",
                "params": {
                    "statecode": statecode,
                    "characteristicName": "Cadmium",
                    "sampleMedia": "Water",
                    "mimeType": "csv",
                    "zip": "no",
                    "dataProfile": "resultPhysChem",
                    "count": "no",
                    "startDateLo": f"01-01-{start_year}",
                    "startDateHi": f"12-31-{end_year}",
                },
            }
            for slug, statecode in [
                ("nevada", "US:32"),
                ("utah", "US:49"),
                ("colorado", "US:08"),
                ("new_mexico", "US:35"),
                ("montana", "US:30"),
                ("wyoming", "US:56"),
                ("oklahoma", "US:40"),
                ("kansas", "US:20"),
                ("florida", "US:12"),
                ("new_york", "US:36"),
                ("pennsylvania", "US:42"),
                ("illinois", "US:17"),
                ("michigan", "US:26"),
                ("wisconsin", "US:55"),
                ("minnesota", "US:27"),
                ("georgia", "US:13"),
                ("north_carolina", "US:37"),
            ]
            for start_year, end_year in [
                (2000, 2004),
                (2005, 2009),
                (2010, 2014),
                (2015, 2019),
                (2020, 2026),
            ]
        ],
    ]

    def fetch(self) -> list[SourceFileRecord]:
        records: list[SourceFileRecord] = []
        for window in self.DEFAULT_QUERY_WINDOWS:
            params = dict(window["params"])
            url = f"{self.RESULT_ENDPOINT}?{urlencode(params)}"
            filename = f"{window['name']}.csv"
            existing = self.raw_dir / filename
            if existing.exists():
                print(f"{self.source_id}: using existing {filename}")
                records.append(self._source_file_for_existing(existing, url))
                continue
            try:
                print(f"{self.source_id}: fetching {filename}")
                content = self._download_wqp(url)
            except Exception as exc:
                print(f"{self.source_id}: skipped {window['name']} after fetch error: {exc}")
                continue
            records.append(self._write_raw_file(filename, url, content))
        return records

    def _download_wqp(self, url: str) -> bytes:
        timeout = httpx.Timeout(45.0, connect=15.0, read=45.0)
        max_elapsed_seconds = 90.0
        max_bytes = 80_000_000
        started = time.monotonic()
        chunks: list[bytes] = []
        size = 0
        with httpx.Client(follow_redirects=True, timeout=timeout) as client:
            with client.stream("GET", url, headers={"User-Agent": "cadmium-lake/0.1.0"}) as response:
                response.raise_for_status()
                for chunk in response.iter_bytes():
                    chunks.append(chunk)
                    size += len(chunk)
                    if time.monotonic() - started > max_elapsed_seconds:
                        raise TimeoutError(f"WQP request exceeded {max_elapsed_seconds:.0f}s")
                    if size > max_bytes:
                        raise ValueError(f"WQP response exceeded {max_bytes} bytes")
            response.raise_for_status()
            return b"".join(chunks)

    def _source_file_for_existing(self, path, url: str) -> SourceFileRecord:
        return SourceFileRecord(
            file_id=stable_id(self.source_id, url, path.name),
            source_id=self.source_id,
            original_url=url,
            local_path=str(path),
            mime_type="text/csv",
            sha256=self._write_and_hash_existing(path),
            retrieved_at=self._timestamp(),
            parser_version=self.parser_version,
        )

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        files = [path for path in self._records_from_raw_dir() if path.suffix.lower() == ".csv"]
        if not files:
            return payload

        study = StudyRecord(
            study_id=stable_id(self.source_id, "wqp", "cadmium-water"),
            source_id=self.source_id,
            study_title="Water Quality Portal cadmium water measurements",
            country="US",
            citation="Water Quality Portal, USGS/EPA/National Water Quality Monitoring Council",
            notes=(
                "Direct water cadmium sample results from the WQP physical/chemical result profile. "
                "WQP aggregates USGS NWIS, EPA WQX, and other public water-quality contributors."
            ),
        )
        payload.studies_or_batches.append(study)

        parsed_rows = []
        for file_path in files:
            frame = load_wqp_csv(file_path.read_bytes())
            for row in frame.to_dict(orient="records"):
                lowered = {str(key).lower(): value for key, value in row.items()}
                characteristic = str(lowered.get("characteristicname") or "").strip().lower()
                pcode = str(lowered.get("usgspcode") or "").strip()
                if "cadmium" not in characteristic and pcode not in {"01025", "01027", "82398"}:
                    continue

                raw_text = clean_text(lowered.get("resultmeasurevalue"))
                detection_condition = clean_text(lowered.get("resultdetectionconditiontext"))
                qualifier = clean_text(lowered.get("measurequalifiercode")) or detection_condition
                value = try_float(raw_text)
                nondetect = bool(detection_condition) or raw_text.startswith("<")
                unit = normalize_liquid_unit(clean_text(lowered.get("resultmeasure/measureunitcode")))
                activity_id = clean_text(lowered.get("activityidentifier")) or stable_id(self.source_id, row)
                result_id = clean_text(lowered.get("resultidentifier")) or stable_id(activity_id, characteristic, raw_text)
                location_id = clean_text(lowered.get("monitoringlocationidentifier"))
                location_name = clean_text(lowered.get("monitoringlocationname"))
                collection_date = clean_text(lowered.get("activitystartdate"))
                collection_year = infer_year(collection_date)
                sample_fraction = clean_text(lowered.get("resultsamplefractiontext"))
                sample_id = stable_id(self.source_id, activity_id, location_id)
                matrix_subtype = classify_water_subtype(row)

                payload.samples.append(
                    SampleRecord(
                        sample_id=sample_id,
                        source_id=self.source_id,
                        study_id=study.study_id,
                        matrix_group="water",
                        matrix_subtype=matrix_subtype,
                        sample_name=activity_id,
                        specimen_or_part=sample_fraction or "water",
                        location_name=location_name or location_id,
                        latitude=try_float(lowered.get("activitylocation/latitudemeasure")),
                        longitude=try_float(lowered.get("activitylocation/longitudemeasure")),
                        country="US",
                        collection_date=collection_date or None,
                        collection_year=collection_year,
                        year_for_plotting=collection_year,
                        year_for_plotting_source="collection_year" if collection_year else None,
                        analyte_method=clean_text(lowered.get("resultanalyticalmethod/methodname"))
                        or clean_text(lowered.get("resultanalyticalmethod/methodidentifier")),
                        comments=(
                            f"WQP provider={clean_text(lowered.get('organizationidentifier'))}; "
                            f"activity_media={clean_text(lowered.get('activitymedianame'))}; "
                            f"location_type={clean_text(lowered.get('monitoringlocationtypename'))}"
                        ),
                    )
                )
                payload.measurements_raw.append(
                    RawMeasurementRecord(
                        measurement_id=stable_id(self.source_id, result_id),
                        sample_id=sample_id,
                        analyte_name="cadmium",
                        raw_value=value,
                        raw_value_text=raw_text or detection_condition or "",
                        raw_unit=unit,
                        nondetect_flag=nondetect,
                        detection_qualifier=qualifier or None,
                        raw_basis_text=sample_fraction or None,
                        page_or_sheet=file_path.name,
                        table_or_figure="WQP resultPhysChem",
                        row_label=result_id,
                        column_label="ResultMeasureValue",
                        extraction_method="wqp_result_physchem_csv",
                        confidence_score=0.95,
                    )
                )
                parsed_rows.append(
                    {
                        "sample_id": sample_id,
                        "result_id": result_id,
                        "matrix_subtype": matrix_subtype,
                        "raw_value": value,
                        "raw_unit": unit,
                        "location_id": location_id,
                    }
                )

        self._write_staging_json("parsed_rows.json", parsed_rows)
        payload.samples = list({sample.sample_id: sample for sample in payload.samples}.values())
        payload.measurements_raw = list({item.measurement_id: item for item in payload.measurements_raw}.values())
        return payload


def load_wqp_csv(content: bytes) -> pd.DataFrame:
    frame = pd.read_csv(BytesIO(content), dtype=str)
    frame.columns = [str(column).strip() for column in frame.columns]
    return frame


def clean_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def normalize_liquid_unit(value: str) -> str | None:
    text = value.replace("µ", "u").strip()
    if text.lower() in {"ug/l", "ug/l as cd", "mcg/l", "micrograms per liter"}:
        return "ug/L"
    return text or None


def try_float(value) -> float | None:
    text = clean_text(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text.lstrip("<").strip())
    except ValueError:
        return None


def infer_year(value: str) -> int | None:
    text = clean_text(value)
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


def classify_water_subtype(row: dict) -> str:
    text = " ".join(clean_text(value).lower() for value in row.values())
    if any(term in text for term in ["drinking", "finished water", "tap water", "public water supply"]):
        return "drinking_water"
    if any(term in text for term in ["irrigation", "canal", "ditch", "agricultural drain"]):
        return "irrigation_water"
    if any(term in text for term in ["well", "spring", "groundwater", "ground water", "aquifer"]):
        return "groundwater"
    if any(term in text for term in ["stream", "river", "lake", "reservoir", "surface water", "wetland"]):
        return "surface_water"
    return "water_unspecified"


class EeaWaterbaseWaterAdapter(BaseAdapter):
    source_id = "eea_waterbase_water"

    LANDING_URL = "https://www.eea.europa.eu/data-and-maps/data/waterbase-water-quality-icm-2"
    DISCODATA_SQL_URL = "https://discodata.eea.europa.eu/sql"
    CADMIUM_CODE = "CAS_7440-43-9"
    COUNTRIES = [
        "AT",
        "BE",
        "BG",
        "CY",
        "CZ",
        "DE",
        "DK",
        "EE",
        "ES",
        "FI",
        "FR",
        "GR",
        "HR",
        "HU",
        "IE",
        "IS",
        "IT",
        "LT",
        "LU",
        "LV",
        "MT",
        "NL",
        "NO",
        "PL",
        "PT",
        "RO",
        "SE",
        "SI",
        "SK",
        "UK",
    ]
    YEAR_WINDOWS = [(1990, 1999), (2000, 2009), (2010, 2014), (2015, 2019), (2020, 2024)]
    FIELDS = [
        "countryCode",
        "monitoringSiteIdentifier",
        "monitoringSiteIdentifierScheme",
        "parameterWaterBodyCategory",
        "observedPropertyDeterminandCode",
        "observedPropertyDeterminandLabel",
        "procedureAnalysedMatrix",
        "resultUom",
        "phenomenonTimeSamplingDate",
        "phenomenonTimeReferenceYear",
        "sampleIdentifier",
        "resultObservedValue",
        "resultQualityObservedValueBelowLOQ",
        "procedureLOQValue",
        "parameterSampleDepth",
        "resultObservationStatus",
        "Remarks",
        "metadata_versionId",
        "metadata_beginLifeSpanVersion",
        "metadata_statusCode",
        "UID",
    ]

    def fetch(self) -> list[SourceFileRecord]:
        records = [self._write_raw_file("landing.html", self.LANDING_URL, self._download(self.LANDING_URL))]
        chunk_limit = int(os.getenv("CADMIUM_LAKE_EEA_WATER_CHUNK_LIMIT", "0") or "0")
        chunk_count = 0
        for country in self.COUNTRIES:
            for start_year, end_year in self.YEAR_WINDOWS:
                if chunk_limit and chunk_count >= chunk_limit:
                    return records
                filename = f"cadmium_{country}_{start_year}_{end_year}.json"
                path = self.raw_dir / filename
                query = self._query(country, start_year, end_year)
                if path.exists():
                    print(f"{self.source_id}: using existing {filename}")
                    records.append(self._source_file_for_existing(path, self.DISCODATA_SQL_URL))
                    chunk_count += 1
                    continue
                try:
                    rows = self._query_discodata(query)
                except Exception as exc:
                    print(f"{self.source_id}: skipped {country} {start_year}-{end_year}: {exc}")
                    continue
                payload = {
                    "query": query,
                    "country": country,
                    "start_year": start_year,
                    "end_year": end_year,
                    "rows": rows,
                }
                records.append(
                    self._write_raw_file(
                        filename,
                        self.DISCODATA_SQL_URL,
                        json.dumps(payload, indent=2, default=str).encode("utf-8"),
                    )
                )
                print(f"{self.source_id}: fetched {filename} rows={len(rows)}")
                chunk_count += 1
        return records

    def _source_file_for_existing(self, path, url: str) -> SourceFileRecord:
        return SourceFileRecord(
            file_id=stable_id(self.source_id, url, path.name),
            source_id=self.source_id,
            original_url=url,
            local_path=str(path),
            mime_type="text/html" if path.suffix.lower() == ".html" else "application/json",
            sha256=self._write_and_hash_existing(path),
            retrieved_at=self._timestamp(),
            parser_version=self.parser_version,
        )

    def _query(self, country: str, start_year: int, end_year: int) -> str:
        fields = ", ".join(self.FIELDS)
        return (
            f"select top 5000 {fields} "
            "from [WISE_SOE].[latest].[Waterbase_T_WISE6_DisaggregatedData] "
            f"where observedPropertyDeterminandCode='{self.CADMIUM_CODE}' "
            "and procedureAnalysedMatrix in ('W','W-DIS') "
            f"and countryCode='{country}' "
            f"and phenomenonTimeReferenceYear>={start_year} "
            f"and phenomenonTimeReferenceYear<={end_year}"
        )

    def _query_discodata(self, query: str) -> list[dict]:
        with httpx.Client(follow_redirects=True, timeout=90.0, headers={"User-Agent": "cadmium-lake/0.1.0"}) as client:
            response = client.post(
                self.DISCODATA_SQL_URL,
                data={"query": query, "p": "1", "nrOfHits": "5000", "mail": "", "schema": ""},
            )
            response.raise_for_status()
            payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(payload["errors"][0].get("error", payload["errors"]))
        return payload.get("results", [])

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        files = sorted(path for path in self._records_from_raw_dir() if path.name.startswith("cadmium_") and path.suffix == ".json")
        if not files:
            return payload
        for file_path in files:
            payload.source_files.append(self._source_file_for_existing(file_path, self.DISCODATA_SQL_URL))
        landing_path = self.raw_dir / "landing.html"
        if landing_path.exists():
            payload.source_files.append(self._source_file_for_existing(landing_path, self.LANDING_URL))

        study = StudyRecord(
            study_id=stable_id(self.source_id, "wise-soe-waterbase-cadmium"),
            source_id=self.source_id,
            study_title="EEA Waterbase Water Quality ICM cadmium water measurements",
            year_start=1990,
            year_end=2024,
            country="Europe",
            citation="European Environment Agency Waterbase - Water Quality ICM, WISE SoE",
            notes=(
                "Direct disaggregated cadmium water sample rows queried from EEA Discodata "
                "WISE_SOE.latest.Waterbase_T_WISE6_DisaggregatedData."
            ),
        )
        payload.studies_or_batches.append(study)

        parsed_rows = []
        for file_path in files:
            file_payload = json.loads(file_path.read_text(encoding="utf-8"))
            for row in file_payload.get("rows", []):
                if row.get("observedPropertyDeterminandCode") != self.CADMIUM_CODE:
                    continue
                unit = normalize_liquid_unit(clean_text(row.get("resultUom")))
                sample_id = stable_id(
                    self.source_id,
                    row.get("monitoringSiteIdentifier"),
                    row.get("sampleIdentifier"),
                    row.get("phenomenonTimeSamplingDate"),
                    row.get("UID"),
                )
                raw_value = try_float(row.get("resultObservedValue"))
                below_loq = bool(row.get("resultQualityObservedValueBelowLOQ"))
                loq = try_float(row.get("procedureLOQValue"))
                collection_date = clean_text(row.get("phenomenonTimeSamplingDate"))
                collection_year = infer_year(collection_date) or try_int(row.get("phenomenonTimeReferenceYear"))
                matrix_subtype = classify_eea_water_subtype(row)
                payload.samples.append(
                    SampleRecord(
                        sample_id=sample_id,
                        source_id=self.source_id,
                        study_id=study.study_id,
                        matrix_group="water",
                        matrix_subtype=matrix_subtype,
                        sample_name=clean_text(row.get("sampleIdentifier")) or str(row.get("UID")),
                        specimen_or_part=clean_text(row.get("procedureAnalysedMatrix")) or "water",
                        location_name=clean_text(row.get("monitoringSiteIdentifier")),
                        country=clean_text(row.get("countryCode")) or None,
                        collection_date=collection_date or None,
                        collection_year=collection_year,
                        year_for_plotting=collection_year,
                        year_for_plotting_source="phenomenonTimeReferenceYear" if collection_year else None,
                        loq=loq,
                        comments=(
                            f"water_body_category={clean_text(row.get('parameterWaterBodyCategory'))}; "
                            f"site_scheme={clean_text(row.get('monitoringSiteIdentifierScheme'))}; "
                            f"metadata_status={clean_text(row.get('metadata_statusCode'))}; "
                            f"metadata_version={clean_text(row.get('metadata_versionId'))}"
                        ),
                    )
                )
                result_id = clean_text(row.get("UID")) or stable_id(sample_id, raw_value, unit)
                payload.measurements_raw.append(
                    RawMeasurementRecord(
                        measurement_id=stable_id(self.source_id, result_id),
                        sample_id=sample_id,
                        analyte_name="cadmium",
                        raw_value=raw_value,
                        raw_value_text=clean_text(row.get("resultObservedValue")) or clean_text(row.get("procedureLOQValue")) or "",
                        raw_unit=unit,
                        nondetect_flag=below_loq,
                        detection_qualifier="below_LOQ" if below_loq else clean_text(row.get("resultObservationStatus")) or None,
                        raw_basis_text=clean_text(row.get("procedureAnalysedMatrix")) or None,
                        page_or_sheet=file_path.name,
                        table_or_figure="WISE6_DisaggregatedData",
                        row_label=str(result_id),
                        column_label="resultObservedValue",
                        extraction_method="eea_discodata_sql",
                        confidence_score=0.95,
                    )
                )
                parsed_rows.append(
                    {
                        "measurement_id": stable_id(self.source_id, result_id),
                        "country": row.get("countryCode"),
                        "matrix_subtype": matrix_subtype,
                        "raw_value": raw_value,
                        "raw_unit": unit,
                        "below_loq": below_loq,
                    }
                )

        self._write_staging_json("parsed_rows.json", parsed_rows)
        payload.samples = list({sample.sample_id: sample for sample in payload.samples}.values())
        payload.measurements_raw = list({item.measurement_id: item for item in payload.measurements_raw}.values())
        return payload


def try_int(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(str(value)))
    except ValueError:
        return None


def classify_eea_water_subtype(row: dict) -> str:
    category = clean_text(row.get("parameterWaterBodyCategory")).upper()
    if category == "GW":
        return "groundwater"
    if category in {"RW", "LW", "TW", "CW"}:
        return "surface_water"
    return classify_water_subtype(row)
