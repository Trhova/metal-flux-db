from __future__ import annotations

import io
import json
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from cadmium_lake.models import ReviewQueueRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


class EsdacLucasSoilAdapter(BaseAdapter):
    source_id = "esdac_lucas_soil"

    LANDING_URL = "https://esdac.jrc.ec.europa.eu/content/lucas-heavy-metals"
    METADATA_URL = "https://esdac.jrc.ec.europa.eu/public_path//shared_folder/dataset/121/Metadata_HeavyMetals.rtf"

    def fetch(self) -> list[SourceFileRecord]:
        html = self._download(self.LANDING_URL)
        records = [self._write_raw_file("landing.html", self.LANDING_URL, html)]
        records.append(self._write_raw_file("Metadata_HeavyMetals.rtf", self.METADATA_URL, self._download(self.METADATA_URL)))
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
        self._write_staging_json(
            "source_inventory.json",
            [
                {
                    "source_id": self.source_id,
                    "landing_url": self.LANDING_URL,
                    "metadata_path": str(self.raw_dir / "Metadata_HeavyMetals.rtf"),
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
        payload.review_queue.append(
            ReviewQueueRecord(
                review_id=stable_id(self.source_id, "portal-export"),
                source_id=self.source_id,
                study_id=study.study_id,
                local_path=str(self.raw_dir / "downloads.html"),
                issue_type="portal_export_required",
                issue_summary="GEMAS cadmium data requires reproducible export path from BGR product centre/geoviewer.",
                parsing_feasibility="portal_or_manual_export",
                status="open",
                notes=json.dumps(urls[:10]),
            )
        )
        self._write_staging_json(
            "source_inventory.json",
            [{"source_id": self.source_id, "access_points": urls[:20], "status": "inventory_only"}],
        )
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
        frame = pd.read_excel(path, sheet_name=sheet_name)
        if frame.empty:
            continue
        analytes = frame.iloc[0].tolist()
        cadmium_idx = None
        for idx, value in enumerate(analytes):
            if str(value).strip() == "Cd":
                cadmium_idx = idx
                break
        if cadmium_idx is None:
            continue
        for _, row in frame.iloc[1:].iterrows():
            food_group = row.iloc[0]
            raw_value = row.iloc[cadmium_idx]
            if pd.isna(food_group) or pd.isna(raw_value):
                continue
            rows.append(
                {
                    "age_class": sheet_name,
                    "food_group": str(food_group).strip(),
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
