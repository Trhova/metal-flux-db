from __future__ import annotations

import json
import re
import zipfile
from urllib.parse import urljoin

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from cadmium_lake.models import (
    RawMeasurementRecord,
    ReviewQueueRecord,
    SampleRecord,
    SourceFileRecord,
    StudyRecord,
    SummaryMeasurementRecord,
)
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class EsdacLucasSoilAdapter(BaseAdapter):
    source_id = "esdac_lucas_soil"

    LANDING_URL = "https://esdac.jrc.ec.europa.eu/content/lucas-heavy-metals"
    METADATA_URL = "https://esdac.jrc.ec.europa.eu/public_path//shared_folder/dataset/121/Metadata_HeavyMetals.rtf"
    PUBLIC_TOPSOIL_URL = "https://esdac.jrc.ec.europa.eu/public_path//shared_folder/dataset/75-LUCAS-SOIL-2018/LUCAS_Text_All_10032025.zip"

    def fetch(self) -> list[SourceFileRecord]:
        html = self._download(self.LANDING_URL)
        records = [self._write_raw_file("landing.html", self.LANDING_URL, html)]
        records.append(self._write_raw_file("Metadata_HeavyMetals.rtf", self.METADATA_URL, self._download(self.METADATA_URL)))
        records.append(
            self._write_raw_file("LUCAS_Text_All_10032025.zip", self.PUBLIC_TOPSOIL_URL, self._download(self.PUBLIC_TOPSOIL_URL))
        )
        return records

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        study = StudyRecord(
            study_id=stable_id(self.source_id, "lucas-heavy-metals"),
            source_id=self.source_id,
            study_title="LUCAS heavy metals official metadata record",
            year_start=2018,
            year_end=2018,
            country="EU",
            citation="ESDAC / JRC",
            notes=(
                "Official LUCAS heavy metals metadata captured. Public page states that download requests are "
                "processed manually after approval; row-level cadmium data is therefore tracked as gated."
            ),
        )
        payload.studies_or_batches.append(study)
        payload.review_queue.append(
            ReviewQueueRecord(
                review_id=stable_id(self.source_id, "gated-heavy-metals"),
                source_id=self.source_id,
                study_id=study.study_id,
                local_path=str(self.raw_dir / "Metadata_HeavyMetals.rtf"),
                issue_type="gated_official_dataset",
                issue_summary="LUCAS heavy metals cadmium dataset requires manual approval before download.",
                parsing_feasibility="awaiting_access",
                status="open",
                notes=f"landing_page={self.LANDING_URL}",
            )
        )
        public_zip = self.raw_dir / "LUCAS_Text_All_10032025.zip"
        public_columns = inspect_lucas_public_zip(public_zip) if public_zip.exists() else []
        payload.review_queue.append(
            ReviewQueueRecord(
                review_id=stable_id(self.source_id, "public-topsoil-inspection"),
                source_id=self.source_id,
                study_id=study.study_id,
                local_path=str(public_zip),
                issue_type="public_dataset_without_cadmium_field",
                issue_summary="Public LUCAS 2018 topsoil zip is accessible, but inspected tabular columns do not expose cadmium values.",
                parsing_feasibility="public_supporting_dataset_only",
                status="open",
                notes=json.dumps({"public_columns": public_columns[:30]}),
            )
        )
        self._write_staging_json(
            "source_inventory.json",
            [
                {
                    "source_id": self.source_id,
                    "landing_url": self.LANDING_URL,
                    "metadata_path": str(self.raw_dir / "Metadata_HeavyMetals.rtf"),
                    "public_topsoil_zip": str(public_zip),
                    "public_topsoil_columns": public_columns,
                    "access_mode": "manual_approval",
                }
            ],
        )
        return payload


class GemasSoilAdapter(BaseAdapter):
    source_id = "gemas_soil"

    LINKS_URL = "https://gemas.eurogeosurveys.org/Links_GEMAS.htm"
    DOWNLOADS_URL = "https://gemas.eurogeosurveys.org/Download_GEMAS.htm"

    def fetch(self) -> list[SourceFileRecord]:
        links_html = self._download(self.LINKS_URL)
        downloads_html = self._download(self.DOWNLOADS_URL)
        return [
            self._write_raw_file("links.html", self.LINKS_URL, links_html),
            self._write_raw_file("downloads.html", self.DOWNLOADS_URL, downloads_html),
        ]

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        study = StudyRecord(
            study_id=stable_id(self.source_id, "gemas"),
            source_id=self.source_id,
            study_title="GEMAS official access inventory",
            country="Europe",
            citation="EuroGeoSurveys GEMAS",
            notes="Official GEMAS pages captured. Primary machine-usable access appears to be via BGR geoviewer/product centre rather than a flat public cadmium table.",
        )
        payload.studies_or_batches.append(study)
        html = (self.raw_dir / "links.html").read_text(encoding="utf-8") if (self.raw_dir / "links.html").exists() else ""
        soup = BeautifulSoup(html, "lxml")
        urls = [urljoin(self.LINKS_URL, a["href"]) for a in soup.select("a[href]")]
        public_services = list_public_gemas_services()
        public_service_layers = inspect_gemas_public_services(public_services)
        payload.review_queue.append(
            ReviewQueueRecord(
                review_id=stable_id(self.source_id, "portal-export"),
                source_id=self.source_id,
                study_id=study.study_id,
                local_path=str(self.raw_dir / "downloads.html"),
                issue_type="portal_export_required",
                issue_summary="GEMAS cadmium data requires reproducible export path from BGR product centre/geoviewer.",
                parsing_feasibility="public_arcgis_services_but_no_public_cadmium_layer",
                status="open",
                notes=json.dumps(
                    {
                        "links": urls[:10],
                        "public_services": public_services,
                        "public_service_layers": public_service_layers,
                    }
                ),
            )
        )
        self._write_staging_json(
            "source_inventory.json",
            [
                {
                    "source_id": self.source_id,
                    "access_points": urls[:20],
                    "public_arcgis_services": public_services,
                    "public_arcgis_service_layers": public_service_layers,
                    "status": "inventory_only",
                }
            ],
        )
        return payload


class ForegsGeochemicalAtlasSoilAdapter(BaseAdapter):
    source_id = "foregs_geochemical_atlas_soil"

    LANDING_URL = "http://weppi.gtk.fi/publ/foregsatlas/ForegsData.php"
    TOPSOIL_URL = "http://weppi.gtk.fi/publ/foregsatlas/Topsoil.zip"
    SUBSOIL_URL = "http://weppi.gtk.fi/publ/foregsatlas/Subsoil.zip"
    FILES = [
        {
            "zip_name": "Topsoil.zip",
            "url": TOPSOIL_URL,
            "member": "Topsoil/T_icpadd_data_2v3_8Feb06.csv",
            "matrix_subtype": "topsoil",
            "sample_suffix": "T",
        },
        {
            "zip_name": "Subsoil.zip",
            "url": SUBSOIL_URL,
            "member": "Subsoil/C_icpadd_data_2v5_9Feb06.csv",
            "matrix_subtype": "subsoil",
            "sample_suffix": "C",
        },
    ]

    def fetch(self) -> list[SourceFileRecord]:
        records = [self._write_raw_file("ForegsData.html", self.LANDING_URL, self._download(self.LANDING_URL))]
        for item in self.FILES:
            records.append(self._write_raw_file(item["zip_name"], item["url"], self._download(item["url"])))
        return records

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        studies = StudyRecord(
            study_id=stable_id(self.source_id, "foregs-geochemical-atlas-soil-cadmium"),
            source_id=self.source_id,
            study_title="FOREGS-EuroGeoSurveys Geochemical Atlas of Europe topsoil and subsoil cadmium",
            year_start=1998,
            year_end=2005,
            publication_year=2005,
            country="Europe",
            citation="Salminen R. (ed.) 2005. Geochemical Atlas of Europe. Part 1. Geological Survey of Finland / EuroGeoSurveys.",
            notes=(
                "Official GTK/EuroGeoSurveys FOREGS raw analytical data. Parser imports CD_MS from topsoil "
                "and subsoil ICP-MS additional-elements CSV files. Source documentation states values below "
                "detection limit were converted to half the detection-limit value in the published files."
            ),
        )
        payload.studies_or_batches.append(studies)

        parsed_rows = []
        for item in self.FILES:
            zip_path = self.raw_dir / item["zip_name"]
            if not zip_path.exists():
                continue
            payload.source_files.append(
                SourceFileRecord(
                    file_id=stable_id(self.source_id, item["url"], zip_path.name),
                    source_id=self.source_id,
                    original_url=item["url"],
                    local_path=str(zip_path),
                    mime_type="application/zip",
                    sha256=self._write_and_hash_existing(zip_path),
                    retrieved_at=self._timestamp(),
                    parser_version=self.parser_version,
                )
            )
            frame, unit, detection_limit = load_foregs_cadmium_csv(zip_path, item["member"])
            for row in frame.to_dict(orient="records"):
                gtn = clean_text(row.get("GTN"))
                if not gtn:
                    continue
                raw_value = try_float(row.get("CD_MS"))
                if raw_value is None or raw_value < 0:
                    continue
                country = clean_text(row.get("COUNTRY")) or clean_text(row.get("CNTRY")) or clean_text(row.get("COUNTR"))
                sample_id = stable_id(self.source_id, gtn, item["matrix_subtype"])
                payload.samples.append(
                    SampleRecord(
                        sample_id=sample_id,
                        source_id=self.source_id,
                        study_id=studies.study_id,
                        matrix_group="soil",
                        matrix_subtype=item["matrix_subtype"],
                        sample_name=gtn,
                        specimen_or_part=item["matrix_subtype"],
                        location_name=gtn,
                        latitude=try_float(row.get("LAT")),
                        longitude=try_float(row.get("LONG")),
                        country=country or None,
                        publication_year=2005,
                        year_for_plotting=2005,
                        year_for_plotting_source="publication_year",
                        analyte_method="ICP-MS additional-elements file",
                        lod=detection_limit,
                        comments=(
                            "FOREGS GTN sample identifier; coordinates transformed to continental-scale "
                            "geographical coordinates by source. Published values below detection limit were "
                            "converted by source to DL/2, so exact censored rows cannot be recovered from the "
                            "public file."
                        ),
                    )
                )
                payload.measurements_raw.append(
                    RawMeasurementRecord(
                        measurement_id=stable_id(self.source_id, gtn, item["matrix_subtype"], "CD_MS"),
                        sample_id=sample_id,
                        analyte_name="cadmium",
                        raw_value=raw_value,
                        raw_value_text=clean_text(row.get("CD_MS")),
                        raw_unit=unit,
                        nondetect_flag=False,
                        detection_qualifier="source_below_dl_values_are_dl_over_2",
                        raw_basis_text="published_foregs_solid_soil",
                        page_or_sheet=item["member"],
                        table_or_figure="CD_MS",
                        row_label=gtn,
                        column_label="CD_MS",
                        extraction_method="foregs_raw_csv",
                        confidence_score=0.96,
                    )
                )
                parsed_rows.append(
                    {
                        "sample_id": sample_id,
                        "gtn": gtn,
                        "matrix_subtype": item["matrix_subtype"],
                        "country": country,
                        "raw_value": raw_value,
                        "raw_unit": unit,
                    }
                )

        self._write_staging_json("parsed_rows.json", parsed_rows)
        payload.samples = list({sample.sample_id: sample for sample in payload.samples}.values())
        payload.measurements_raw = list({measurement.measurement_id: measurement for measurement in payload.measurements_raw}.values())
        return payload


class UkFsaTotalDietAdapter(BaseAdapter):
    source_id = "uk_fsa_total_diet"

    LANDING_URL = "https://www.food.gov.uk/research/chemical-hazards-in-food-and-feed/total-diet-study-metals-and-other-elements"
    XLSX_URL = "https://www.food.gov.uk/sites/default/files/media/document/metals-exposure-data.xlsx"

    def fetch(self) -> list[SourceFileRecord]:
        html = self._download(self.LANDING_URL)
        xlsx = self._download(self.XLSX_URL)
        return [
            self._write_raw_file("landing.html", self.LANDING_URL, html),
            self._write_raw_file("metals-exposure-data.xlsx", self.XLSX_URL, xlsx),
        ]

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        study = StudyRecord(
            study_id=stable_id(self.source_id, "uk-fsa-tds-metals"),
            source_id=self.source_id,
            study_title="UK FSA Total Diet Study metals exposure summary",
            country="UK",
            citation="UK Food Standards Agency",
            notes="Official Excel contains food-group cadmium exposure summaries by age class in ug/kg bw/day.",
        )
        payload.studies_or_batches.append(study)
        xlsx_path = self.raw_dir / "metals-exposure-data.xlsx"
        if not xlsx_path.exists():
            return payload
        rows = extract_uk_fsa_cadmium_rows(xlsx_path)
        self._write_staging_json("cadmium_exposure_summary.json", rows)
        for row in rows:
            parsed = parse_numeric_text(row["raw_value_text"])
            payload.summary_measurements.append(
                SummaryMeasurementRecord(
                    summary_measurement_id=stable_id(
                        self.source_id, study.study_id, row["age_class"], row["food_group"], row["statistic_name"]
                    ),
                    source_id=self.source_id,
                    study_id=study.study_id,
                    matrix_group="food",
                    matrix_subtype="dietary_exposure_summary",
                    analyte_name="cadmium",
                    statistic_name=row["statistic_name"],
                    subgroup=row["age_class"],
                    item_label=row["food_group"],
                    raw_value_text=row["raw_value_text"],
                    summary_value=parsed["value"],
                    lower_value=parsed["lower"],
                    upper_value=parsed["upper"],
                    summary_unit="ug/kg_bw/day",
                    summary_dimension="intake_mass_per_mass_per_day",
                    raw_basis_text="as_consumed",
                    page_or_sheet=row["age_class"],
                    table_or_figure="cadmium_exposure_summary",
                    extraction_method="xlsx_summary_table",
                    confidence_score=0.98,
                    notes="UK FSA Total Diet Study metals exposure summary",
                )
            )
        for sheet_name in sorted({row["age_class"] for row in rows}):
            payload.review_queue.append(
                ReviewQueueRecord(
                    review_id=stable_id(self.source_id, study.study_id, sheet_name),
                    source_id=self.source_id,
                    study_id=study.study_id,
                    local_path=str(self.staging_dir / "cadmium_exposure_summary.json"),
                    page_or_sheet=sheet_name,
                    issue_type="summary_stat_source",
                    issue_summary="UK FSA cadmium exposure data staged as summary statistics and kept separate from individual-row tables.",
                    parsing_feasibility="summary_stats_ready",
                    status="open",
                    notes=f"sheet={sheet_name}",
                )
            )
        return payload


class Hbm4euParcCadmiumAdapter(BaseAdapter):
    source_id = "hbm4eu_parc_cadmium"

    DASHBOARD_URL = "https://hbm.vito.be/eu-hbm-dashboard"
    LANDING_URL = "https://www.hbm4eu.eu/hbm4eu-substances/cadmium/"
    POLICY_URL = "https://www.hbm4eu.eu/wp-content/uploads/2022/07/HBM4EU_Policy-Brief-Cadmium.pdf"

    def fetch(self) -> list[SourceFileRecord]:
        landing = self._download(self.LANDING_URL)
        dashboard = self._download(self.DASHBOARD_URL)
        policy = self._download(self.POLICY_URL)
        return [
            self._write_raw_file("landing.html", self.LANDING_URL, landing),
            self._write_raw_file("dashboard.html", self.DASHBOARD_URL, dashboard),
            self._write_raw_file("HBM4EU_Policy-Brief-Cadmium.pdf", self.POLICY_URL, policy),
        ]

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        study = StudyRecord(
            study_id=stable_id(self.source_id, "dashboard-summary"),
            source_id=self.source_id,
            study_title="HBM4EU/PARC cadmium biomonitoring dashboard summary",
            country="EU",
            citation="HBM4EU / PARC / VITO",
            notes="Dashboard exposes public summary statistics via Tableau; page notes that access to the underlying summary-statistics file is available on request.",
        )
        payload.studies_or_batches.append(study)
        dashboard_path = self.raw_dir / "dashboard.html"
        workbook = extract_tableau_metadata(dashboard_path.read_text(encoding="utf-8")) if dashboard_path.exists() else {}
        self._write_staging_json("dashboard_metadata.json", [workbook] if workbook else [])
        landing_path = self.raw_dir / "landing.html"
        landing_text = BeautifulSoup(landing_path.read_text(encoding="utf-8"), "lxml").get_text(" ", strip=True) if landing_path.exists() else ""
        payload.summary_measurements.extend(extract_hbm4eu_cadmium_summaries(self.source_id, study.study_id, landing_text))
        payload.review_queue.append(
            ReviewQueueRecord(
                review_id=stable_id(self.source_id, "dashboard-summary"),
                source_id=self.source_id,
                study_id=study.study_id,
                local_path=str(self.staging_dir / "dashboard_metadata.json"),
                issue_type="summary_stat_source",
                issue_summary="HBM4EU/PARC cadmium data is publicly exposed as dashboard summary statistics, not row-level biomonitoring records.",
                parsing_feasibility="dashboard_or_requested_summary_file",
                status="open",
                notes=json.dumps(workbook),
            )
        )
        return payload


def extract_uk_fsa_cadmium_rows(path) -> list[dict]:
    excel = pd.ExcelFile(path)
    rows: list[dict] = []
    for sheet_name in excel.sheet_names:
        frame = pd.read_excel(path, sheet_name=sheet_name, header=None)
        if frame.empty:
            continue
        block_starts = frame.index[frame.iloc[:, 0].astype(str).str.strip() == "Food Group"].tolist()
        for start in block_starts:
            statistic_label = str(frame.iloc[start, 1]).strip()
            analyte_row = frame.iloc[start + 1].tolist()
            cadmium_idx = next((idx for idx, value in enumerate(analyte_row) if str(value).strip() == "Cd"), None)
            if cadmium_idx is None:
                continue
            statistic_name = normalize_uk_fsa_statistic_name(statistic_label)
            data_start = start + 2
            for row_idx in range(data_start, len(frame)):
                food_group = frame.iloc[row_idx, 0]
                if pd.isna(food_group):
                    break
                raw_value = frame.iloc[row_idx, cadmium_idx]
                if pd.isna(raw_value):
                    continue
                rows.append(
                    {
                        "age_class": sheet_name,
                        "food_group": str(food_group).strip(),
                        "statistic_name": statistic_name,
                        "raw_value_text": str(raw_value).strip(),
                        "raw_unit": "ug/kg_bw/day",
                        "analyte_name": "cadmium",
                    }
                )
    return rows


def extract_tableau_metadata(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    params = {}
    for param in soup.select("object.tableauViz param"):
        name = param.get("name")
        value = param.get("value")
        if name and value:
            params[name] = value
    return params


def parse_numeric_text(raw_text: str) -> dict[str, float | None]:
    cleaned = raw_text.strip()
    match = re.fullmatch(r"(?P<lower>-?\d+(?:\.\d+)?)\s*-\s*(?P<upper>-?\d+(?:\.\d+)?)", cleaned)
    if match:
        return {"value": None, "lower": float(match.group("lower")), "upper": float(match.group("upper"))}
    numeric = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if numeric:
        return {"value": float(numeric.group(0)), "lower": None, "upper": None}
    return {"value": None, "lower": None, "upper": None}


def normalize_uk_fsa_statistic_name(label: str) -> str:
    lower = label.lower()
    if "97.5th" in lower:
        return "exposure_p97_5"
    if "mean" in lower:
        return "mean_exposure"
    return re.sub(r"[^a-z0-9]+", "_", lower).strip("_")


def extract_hbm4eu_cadmium_summaries(source_id: str, study_id: str, text: str) -> list[SummaryMeasurementRecord]:
    records: list[SummaryMeasurementRecord] = []
    patterns = [
        (
            "blood_non_smokers",
            "blood",
            "whole_blood_summary",
            "average_concentration",
            "Adults non-smokers",
            "Blood cadmium",
            r"blood concentrations of (\d+(?:\.\d+)?-\d+(?:\.\d+)?) µgCd/L for non-smokers",
            "ug/L",
            "mass_per_volume",
            False,
        ),
        (
            "blood_reference_adults",
            "blood",
            "whole_blood_reference",
            "reference_value_upper_bound",
            "Adults",
            "Blood reference value",
            r"reference value is below (\d+(?:\.\d+)?) μg/L for adults",
            "ug/L",
            "mass_per_volume",
            False,
        ),
    ]
    for key, matrix_group, matrix_subtype, statistic_name, subgroup, item_label, pattern, unit, dimension, derived in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        raw_value_text = match.group(1)
        parsed = parse_numeric_text(raw_value_text)
        summary_value = parsed["value"]
        lower_value = parsed["lower"]
        upper_value = parsed["upper"]
        if statistic_name == "reference_value_upper_bound" and summary_value is not None:
            upper_value = summary_value
            summary_value = None
        records.append(
            SummaryMeasurementRecord(
                summary_measurement_id=stable_id(source_id, study_id, key),
                source_id=source_id,
                study_id=study_id,
                matrix_group=matrix_group,
                matrix_subtype=matrix_subtype,
                analyte_name="cadmium",
                statistic_name=statistic_name,
                subgroup=subgroup,
                item_label=item_label,
                raw_value_text=raw_value_text,
                summary_value=summary_value,
                lower_value=lower_value,
                upper_value=upper_value,
                summary_unit=unit,
                summary_dimension=dimension,
                raw_basis_text=None,
                page_or_sheet="landing.html",
                table_or_figure="cadmium_substance_page",
                extraction_method="html_text_pattern",
                confidence_score=0.92,
                derived_flag=derived,
                notes="Parsed from HBM4EU cadmium substance page",
            )
        )
    blood_range = next(
        (record for record in records if record.item_label == "Blood cadmium" and record.subgroup == "Adults non-smokers"),
        None,
    )
    if blood_range and blood_range.lower_value is not None and blood_range.upper_value is not None:
        records.append(
            SummaryMeasurementRecord(
                summary_measurement_id=stable_id(source_id, study_id, "blood_smokers_derived"),
                source_id=source_id,
                study_id=study_id,
                matrix_group="blood",
                matrix_subtype="whole_blood_summary",
                analyte_name="cadmium",
                statistic_name="average_concentration",
                subgroup="Adults smokers",
                item_label="Blood cadmium",
                raw_value_text="twice as high in smokers",
                summary_value=None,
                lower_value=blood_range.lower_value * 2.0,
                upper_value=blood_range.upper_value * 2.0,
                summary_unit="ug/L",
                summary_dimension="mass_per_volume",
                raw_basis_text=None,
                page_or_sheet="landing.html",
                table_or_figure="cadmium_substance_page",
                extraction_method="html_text_pattern_derived",
                confidence_score=0.8,
                derived_flag=True,
                notes="Derived from non-smoker range using phrase 'twice as high in smokers'.",
            )
        )
    return records


def inspect_lucas_public_zip(path) -> list[str]:
    import zipfile

    with zipfile.ZipFile(path) as zf:
        member = next((name for name in zf.namelist() if name.lower().endswith(".csv")), None)
        if member is None:
            return []
        with zf.open(member) as handle:
            frame = pd.read_csv(handle, nrows=5)
        return [str(column) for column in frame.columns]


def list_public_gemas_services() -> list[str]:
    data = httpx.get(
        "https://services.bgr.de/arcgis/rest/services/geochemie?f=pjson",
        headers={"User-Agent": "cadmium-lake/0.1.0"},
        timeout=60,
    ).json()
    return [entry["name"] for entry in data.get("services", [])]


def inspect_gemas_public_services(services: list[str]) -> dict[str, list[str]]:
    details: dict[str, list[str]] = {}
    for service_name in services:
        if "gemas" not in service_name:
            continue
        url = f"https://services.bgr.de/arcgis/rest/services/{service_name}/MapServer/layers?f=pjson"
        data = httpx.get(url, headers={"User-Agent": "cadmium-lake/0.1.0"}, timeout=60).json()
        details[service_name] = [str(layer.get("name")) for layer in data.get("layers", [])[:50]]
    return details


def load_foregs_cadmium_csv(zip_path, member: str) -> tuple[pd.DataFrame, str, float | None]:
    with zipfile.ZipFile(zip_path) as archive:
        with archive.open(member) as handle:
            frame = pd.read_csv(handle, encoding="latin-1", dtype=str)
    unit_row = frame[frame["GTN"].astype(str).str.lower() == "unit"]
    dl_row = frame[frame["GTN"].astype(str).str.upper() == "DL"]
    unit = clean_text(unit_row["CD_MS"].iloc[0]) if not unit_row.empty else "mg/kg"
    detection_limit = try_float(dl_row["CD_MS"].iloc[0]) if not dl_row.empty else None
    data = frame[~frame["GTN"].astype(str).isin(["unit", "DL"])].copy()
    return data, unit, detection_limit


class EfsaSeaweedOccurrenceAdapter(BaseAdapter):
    source_id = "efsa_seaweed_occurrence"

    RECORD_API_URL = "https://zenodo.org/api/records/7576613"
    RAW_OCCURRENCE_URL = "https://zenodo.org/api/records/7576613/files/Annex_C_Raw_Ocurrence_data.xls/content"
    CADMIUM_PARAM_CODE = "RF-00000150-CHE"

    def fetch(self) -> list[SourceFileRecord]:
        return [
            self._write_raw_file("zenodo_record.json", self.RECORD_API_URL, self._download(self.RECORD_API_URL)),
            self._write_raw_file(
                "Annex_C_Raw_Ocurrence_data.xls",
                self.RAW_OCCURRENCE_URL,
                self._download(self.RAW_OCCURRENCE_URL),
            ),
        ]

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        xls_path = self.raw_dir / "Annex_C_Raw_Ocurrence_data.xls"
        if not xls_path.exists():
            return payload

        study = StudyRecord(
            study_id=stable_id(self.source_id, "efsa-seaweed-halophyte-occurrence"),
            source_id=self.source_id,
            study_title="EFSA seaweed and halophyte heavy-metal food occurrence data",
            year_start=2011,
            year_end=2021,
            country="EU",
            citation="EFSA Knowledge Junction / Zenodo record 7576613",
            repository_doi="10.5281/zenodo.7576613",
            notes=(
                "Official EFSA raw occurrence dataset for food and feed samples; parser imports only food rows "
                "with cadmium parameter code RF-00000150-CHE and excludes rows marked EXCLUSION."
            ),
        )
        payload.studies_or_batches.append(study)

        frame = pd.read_excel(xls_path, sheet_name="Raw Food occurrence data")
        cadmium = frame[frame["paramCode.base.param"].astype(str) == self.CADMIUM_PARAM_CODE].copy()
        outcome = cadmium["outcome"].astype(str)
        cadmium = cadmium[~outcome.str.contains("EXCLUSION|Exclusion", na=False)]
        parsed_rows = []
        for row in cadmium.to_dict(orient="records"):
            res_id = clean_text(row.get("resId_A")) or stable_id(self.source_id, row)
            sample_key = clean_text(row.get("sampId_A")) or clean_text(row.get("sampEventId_A")) or res_id
            sample_id = stable_id(self.source_id, sample_key, clean_text(row.get("sampAnId_A")))
            sample_year = try_int(row.get("sampY")) or try_int(row.get("analysisY"))
            sample_date = build_partial_date(row.get("sampY"), row.get("sampD"))
            food_description = clean_text(row.get("sampMatCode_desc")) or clean_text(row.get("anMatCode_desc"))
            food_name = food_description.split("#", 1)[0] if food_description else clean_text(row.get("sampMatCode_last"))
            raw_value = try_float(row.get("resVal"))
            res_type = clean_text(row.get("resType"))
            raw_unit = normalize_efsa_unit(clean_text(row.get("resUnit")))
            lod = try_float(row.get("resLOD"))
            loq = try_float(row.get("resLOQ"))
            payload.samples.append(
                SampleRecord(
                    sample_id=sample_id,
                    source_id=self.source_id,
                    study_id=study.study_id,
                    matrix_group="food",
                    matrix_subtype="seaweed_halophyte_food",
                    sample_name=food_name or sample_key,
                    specimen_or_part=food_name or "food",
                    edible_portion_flag=True,
                    dry_wet_basis=clean_text(row.get("exprResType")) or None,
                    location_name=clean_text(row.get("sampPoint")) or None,
                    country=clean_text(row.get("sampCountry")) or None,
                    collection_date=sample_date,
                    collection_year=sample_year,
                    year_for_plotting=sample_year,
                    year_for_plotting_source="sampY" if sample_year else None,
                    analyte_method=clean_text(row.get("anMethCode.base.meth")) or None,
                    lod=lod,
                    loq=loq,
                    comments=(
                        f"sample_material={food_description}; analysis_year={clean_text(row.get('analysisY'))}; "
                        f"origin_country={clean_text(row.get('origCountry'))}; "
                        f"process_country={clean_text(row.get('procCountry'))}; "
                        f"lab_country={clean_text(row.get('labCountry'))}; "
                        f"outcome={clean_text(row.get('outcome'))}; issue={clean_text(row.get('issue'))}"
                    ),
                )
            )
            payload.measurements_raw.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(self.source_id, res_id),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=raw_value,
                    raw_value_text=clean_text(row.get("resVal"))
                    or clean_text(row.get("resLOQ"))
                    or clean_text(row.get("resLOD"))
                    or "",
                    raw_unit=raw_unit,
                    nondetect_flag=res_type in {"LOD", "LOQ"} and raw_value is None,
                    detection_qualifier=res_type or None,
                    raw_basis_text=clean_text(row.get("exprResType")) or None,
                    page_or_sheet="Raw Food occurrence data",
                    table_or_figure="Annex_C_Raw_Ocurrence_data.xls",
                    row_label=res_id,
                    column_label="resVal",
                    extraction_method="efsa_raw_occurrence_xls",
                    confidence_score=0.95,
                )
            )
            parsed_rows.append(
                {
                    "measurement_id": stable_id(self.source_id, res_id),
                    "sample_id": sample_id,
                    "country": clean_text(row.get("sampCountry")),
                    "food_name": food_name,
                    "raw_value": raw_value,
                    "raw_unit": raw_unit,
                    "res_type": res_type,
                }
            )

        self._write_staging_json("parsed_rows.json", parsed_rows)
        payload.samples = list({sample.sample_id: sample for sample in payload.samples}.values())
        payload.measurements_raw = list({item.measurement_id: item for item in payload.measurements_raw}.values())
        return payload


def normalize_efsa_unit(value: str) -> str | None:
    if value == "G050A":
        return "ug/kg"
    return value or None


def clean_text(value) -> str:
    if value is None or pd.isna(value):
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


def try_int(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(str(value)))
    except ValueError:
        return None


def build_partial_date(year, day) -> str | None:
    parsed_year = try_int(year)
    parsed_day = try_int(day)
    if parsed_year and parsed_day and 1 <= parsed_day <= 31:
        return f"{parsed_year:04d}-01-{parsed_day:02d}"
    if parsed_year:
        return f"{parsed_year:04d}"
    return None
