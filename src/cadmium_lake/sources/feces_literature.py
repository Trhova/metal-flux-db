from __future__ import annotations

from dataclasses import dataclass

from cadmium_lake.models import RawMeasurementRecord, ReviewQueueRecord, SampleRecord, SourceFileRecord, StudyRecord
from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.utils import stable_id


@dataclass(frozen=True)
class FecesStat:
    township: str
    n: int
    statistic: str
    value: float
    raw_value_text: str


@dataclass(frozen=True)
class FecesConcentrationSummary:
    key: str
    title: str
    citation: str
    doi: str | None
    pmid: str | None
    publication_year: int
    country: str | None
    location_name: str | None
    matrix_subtype: str
    dry_wet_basis: str
    value_mg_kg: float
    raw_value_text: str
    statistic: str
    primary_reference: str
    source_table: str
    source_file: str
    notes: str


class CuratedFecesLiteratureAdapter(BaseAdapter):
    source_id = "feces_literature"

    YABE_PDF_URL = "https://eprints.lib.hokudai.ac.jp/repo/huscap/all/78798/Chemosphere202_48-55.pdf"
    TSUCHIYA_PMC_URL = "https://pmc.ncbi.nlm.nih.gov/articles/PMC1637200/"
    ROSE_PMC_XML_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=4500995&retmode=xml"
    OBESITY_PMC_XML_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=12540913&retmode=xml"

    YABE_CD_F_STATS = [
        FecesStat("Chowa", 8, "mean", 0.18, "0.18"),
        FecesStat("Chowa", 8, "geometric_mean", 0.15, "0.15"),
        FecesStat("Chowa", 8, "median", 0.16, "0.16"),
        FecesStat("Chowa", 8, "minimum", 0.07, "0.07"),
        FecesStat("Chowa", 8, "maximum", 0.43, "0.43"),
        FecesStat("Chowa", 8, "iqr_upper", 0.23, "0.23"),
        FecesStat("Chowa", 8, "iqr_lower", 0.09, "0.09"),
        FecesStat("Kasanda", 88, "mean", 0.54, "0.54"),
        FecesStat("Kasanda", 88, "geometric_mean", 0.31, "0.31"),
        FecesStat("Kasanda", 88, "median", 0.28, "0.28"),
        FecesStat("Kasanda", 88, "minimum", 0.04, "0.04"),
        FecesStat("Kasanda", 88, "maximum", 4.49, "4.49"),
        FecesStat("Kasanda", 88, "iqr_upper", 0.57, "0.57"),
        FecesStat("Kasanda", 88, "iqr_lower", 0.15, "0.15"),
        FecesStat("Makululu", 94, "mean", 0.26, "0.26"),
        FecesStat("Makululu", 94, "geometric_mean", 0.18, "0.18"),
        FecesStat("Makululu", 94, "median", 0.17, "0.17"),
        FecesStat("Makululu", 94, "minimum", 0.04, "0.04"),
        FecesStat("Makululu", 94, "maximum", 1.58, "1.58"),
        FecesStat("Makululu", 94, "iqr_upper", 0.29, "0.29"),
        FecesStat("Makululu", 94, "iqr_lower", 0.10, "0.10"),
    ]

    HUMAN_EXCRETA_REVIEW_STATS = [
        FecesConcentrationSummary(
            key="schouw_2002_southern_thailand",
            title="Composition of human excreta--A case study from Southern Thailand",
            citation="Schouw et al. 2002, Science of the Total Environment 286:155-166",
            doi="10.1016/S0048-9697(01)00973-1",
            pmid="11886091",
            publication_year=2002,
            country="Thailand",
            location_name="Southern Thailand",
            matrix_subtype="human_feces_literature_review_summary",
            dry_wet_basis="wet_weight",
            value_mg_kg=0.27,
            raw_value_text="0.27",
            statistic="reported_concentration",
            primary_reference="Schouw et al. (2002)",
            source_table="Rose et al. 2015 Table 5",
            source_file="pmc4500995_rose_feces_urine.xml",
            notes=(
                "Concentration column in Rose et al. 2015 Table 5 for Cd in feces, wet weight. "
                "Traceable to primary human-excreta study Schouw et al. 2002; daily loading was not imported."
            ),
        ),
        FecesConcentrationSummary(
            key="vinneras_2006_sweden",
            title="The characteristics of household wastewater and biodegradable solid waste--A proposal for new Swedish design values",
            citation="Vinneras et al. 2006, Urban Water Journal 3:3-11",
            doi="10.1080/15730620600578629",
            pmid=None,
            publication_year=2006,
            country="Sweden",
            location_name="Sweden",
            matrix_subtype="human_feces_literature_review_summary",
            dry_wet_basis="wet_weight",
            value_mg_kg=6.39,
            raw_value_text="6.39",
            statistic="reported_concentration",
            primary_reference="Vinneras et al. (2006)",
            source_table="Rose et al. 2015 Table 5",
            source_file="pmc4500995_rose_feces_urine.xml",
            notes=(
                "Concentration column in Rose et al. 2015 Table 5 for Cd in feces, wet weight. "
                "Traceable to primary Swedish household wastewater/faeces design-value study; daily loading was not imported."
            ),
        ),
    ]

    REVIEW_CANDIDATES = [
        {
            "key": "wang_2012_chinese_males",
            "title": "Correlations of Trace Element Levels in the Diet, Blood, Urine, and Feces in the Chinese Male",
            "doi": "10.1007/s12011-011-9177-8",
            "pmid": "21870153",
            "notes": (
                "Human feces concentration study; abstract confirms 120 healthy men and feces Cd measurement by ICP-MS. "
                "Full feces concentration table is not open in local automated access and needs manual extraction."
            ),
        },
        {
            "key": "tsuchiya_iwao_1978",
            "title": "Interrelationships among zinc, copper, lead, and cadmium in food, feces, and organs of humans",
            "doi": "10.1289/ehp.7825119",
            "pmid": "720297",
            "notes": (
                "Open PMC article confirms 221 human feces samples, but concentration values are not present as HTML tables. "
                "PDF/OCR table extraction needs manual review before ingesting Cd concentration rows."
            ),
        },
        {
            "key": "iwao_1977",
            "title": "Cadmium, lead, copper and zinc in food, feces and organs of humans",
            "doi": "10.2302/kjm.26.63",
            "pmid": "609185",
            "notes": "Likely companion/original table source for human feces concentrations; needs manual table extraction.",
        },
        {
            "key": "obesity_gut_microbiota_2025",
            "title": "Heavy Metals, Gut Microbiota, and Biochemical Markers: Unraveling the Complexities of Obesity",
            "doi": "10.1002/mbo3.70071",
            "pmid": "41121649",
            "notes": (
                "Human stool heavy metals measured by ICP-MS. Open PMC XML confirms stool Cd was measured, but numeric Cd "
                "values are currently figure/supplement-only in local automated access; supplement download was not available "
                "through PMC/Wiley in this run."
            ),
        },
        {
            "key": "bergback_1994_swedish_women_high_fiber",
            "title": "Intestinal absorption of dietary cadmium in women depends on body iron stores and fiber intake",
            "doi": "10.1289/ehp.941021058",
            "pmid": "7713018",
            "notes": (
                "Open PMC article reports Cd in feces as ug/day in Table 5; excluded from direct concentration import because "
                "the atlas is restricted to actual feces concentrations."
            ),
        },
        {
            "key": "kjellstrom_1978_swedish_feces",
            "title": "Cadmium in feces as an estimator of daily cadmium intake in Sweden",
            "doi": "10.1016/0013-9351(78)90101-9",
            "pmid": None,
            "notes": (
                "Abstract reports daily fecal Cd amount and daily fecal wet weight, but not a direct concentration table in "
                "local automated access; excluded rather than deriving concentration from daily output."
            ),
        },
        {
            "key": "kikuchi_2003_japanese_women",
            "title": "Uptake of Cadmium in Meals from the Digestive Tract of Young Non-smoking Japanese Female Volunteers",
            "doi": "10.1539/joh.45.43",
            "pmid": "14605432",
            "notes": (
                "J-STAGE PDF is open and reports Cd-F as ug/day with fecal wet/dry mass metadata, but no direct feces "
                "concentration row. Excluded from direct concentration import per atlas rule."
            ),
        },
    ]

    def fetch(self) -> list[SourceFileRecord]:
        return [
            self._write_raw_file("yabe_2018_kabwe_feces_urine.pdf", self.YABE_PDF_URL, self._download(self.YABE_PDF_URL)),
            self._write_raw_file("tsuchiya_iwao_1978_pmc.html", self.TSUCHIYA_PMC_URL, self._download(self.TSUCHIYA_PMC_URL)),
            self._write_raw_file("pmc4500995_rose_feces_urine.xml", self.ROSE_PMC_XML_URL, self._download(self.ROSE_PMC_XML_URL)),
            self._write_raw_file("pmc12540913_obesity_gut_microbiota.xml", self.OBESITY_PMC_XML_URL, self._download(self.OBESITY_PMC_XML_URL)),
        ]

    def parse(self) -> ParsedPayload:
        payload = ParsedPayload()
        yabe_pdf = self.raw_dir / "yabe_2018_kabwe_feces_urine.pdf"
        if yabe_pdf.exists():
            payload.studies_or_batches.append(self._yabe_study())
            payload.samples.extend(self._yabe_samples())
            payload.measurements_raw.extend(self._yabe_measurements())

        payload.studies_or_batches.extend(self._human_excreta_review_studies())
        payload.samples.extend(self._human_excreta_review_samples())
        payload.measurements_raw.extend(self._human_excreta_review_measurements())

        for candidate in self.REVIEW_CANDIDATES:
            study = StudyRecord(
                study_id=stable_id(self.source_id, candidate["key"]),
                source_id=self.source_id,
                study_title=candidate["title"],
                country=None,
                doi=candidate.get("doi"),
                pmid=candidate.get("pmid"),
                citation="Curated feces cadmium literature candidate",
                notes=candidate["notes"],
            )
            payload.studies_or_batches.append(study)
            payload.review_queue.append(
                ReviewQueueRecord(
                    review_id=stable_id(self.source_id, "review", candidate["key"]),
                    source_id=self.source_id,
                    study_id=study.study_id,
                    issue_type="literature_table_extraction_needed",
                    issue_summary=f"Manual feces concentration extraction needed for {candidate['title']}",
                    parsing_feasibility="manual_table_or_supplement_review",
                    status="open",
                    notes=f"literature_only=true; not_api_database=true; {candidate['notes']}",
                )
            )
        return payload

    def _human_excreta_review_studies(self) -> list[StudyRecord]:
        return [
            StudyRecord(
                study_id=stable_id(self.source_id, stat.key),
                source_id=self.source_id,
                study_title=stat.title,
                publication_year=stat.publication_year,
                country=stat.country,
                citation=stat.citation,
                doi=stat.doi,
                pmid=stat.pmid,
                notes=(
                    "Curated from Rose et al. 2015 open PMC review Table 5, which reports Cd concentration in feces "
                    f"and cites the primary source {stat.primary_reference}. literature_only=true; "
                    f"not_api_database=true; secondary_literature_review_table=true; {stat.notes}"
                ),
            )
            for stat in self.HUMAN_EXCRETA_REVIEW_STATS
        ]

    def _human_excreta_review_samples(self) -> list[SampleRecord]:
        records = []
        for stat in self.HUMAN_EXCRETA_REVIEW_STATS:
            records.append(
                SampleRecord(
                    sample_id=stable_id(self.source_id, stat.key, "cd_feces", stat.statistic),
                    source_id=self.source_id,
                    study_id=stable_id(self.source_id, stat.key),
                    matrix_group="feces",
                    matrix_subtype=stat.matrix_subtype,
                    sample_name=f"{stat.primary_reference} fecal cadmium {stat.statistic}",
                    specimen_or_part="feces",
                    dry_wet_basis=stat.dry_wet_basis,
                    location_name=stat.location_name,
                    country=stat.country,
                    publication_year=stat.publication_year,
                    year_for_plotting=stat.publication_year,
                    year_for_plotting_source="publication_year",
                    comments=(
                        "literature_only=true; not_api_database=true; secondary_literature_review_table=true; "
                        f"source_table={stat.source_table}; primary_reference={stat.primary_reference}; {stat.notes}"
                    ),
                )
            )
        return records

    def _human_excreta_review_measurements(self) -> list[RawMeasurementRecord]:
        records = []
        for stat in self.HUMAN_EXCRETA_REVIEW_STATS:
            sample_id = stable_id(self.source_id, stat.key, "cd_feces", stat.statistic)
            records.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(sample_id, "cadmium", stat.statistic),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=stat.value_mg_kg,
                    raw_value_text=stat.raw_value_text,
                    raw_unit="mg/kg",
                    nondetect_flag=False,
                    raw_basis_text=stat.dry_wet_basis.replace("_", " "),
                    page_or_sheet=stat.source_file,
                    table_or_figure=stat.source_table,
                    row_label=f"Cd, {stat.primary_reference}",
                    column_label="Value (mg/kg)",
                    extraction_method="literature_review_table_summary_stat",
                    confidence_score=0.82,
                )
            )
        return records

    def _yabe_study(self) -> StudyRecord:
        return StudyRecord(
            study_id=stable_id(self.source_id, "yabe_2018_kabwe"),
            source_id=self.source_id,
            study_title=(
                "Lead and cadmium excretion in feces and urine of children from polluted townships near "
                "a lead-zinc mine in Kabwe, Zambia"
            ),
            year_start=2012,
            year_end=2012,
            publication_year=2018,
            country="Zambia",
            citation="Yabe et al. 2018, Chemosphere 202:48-55",
            doi="10.1016/j.chemosphere.2018.03.079",
            pmid="29554507",
            notes=(
                "Curated from paper Table 3. Fecal cadmium concentrations are reported in mg/kg dry weight. "
                "This is literature extraction, not an API/database record."
            ),
        )

    def _yabe_samples(self) -> list[SampleRecord]:
        records = []
        study_id = stable_id(self.source_id, "yabe_2018_kabwe")
        for stat in self.YABE_CD_F_STATS:
            sample_id = stable_id(self.source_id, "yabe_2018_kabwe", stat.township, stat.statistic)
            records.append(
                SampleRecord(
                    sample_id=sample_id,
                    source_id=self.source_id,
                    study_id=study_id,
                    matrix_group="feces",
                    matrix_subtype="child_stool_literature_summary",
                    sample_name=f"Yabe 2018 {stat.township} fecal cadmium {stat.statistic}",
                    specimen_or_part="feces",
                    dry_wet_basis="dry_weight",
                    location_name=f"{stat.township}, Kabwe",
                    country="Zambia",
                    collection_year=2012,
                    publication_year=2018,
                    year_for_plotting=2012,
                    year_for_plotting_source="collection_year",
                    analyte_method="ICP-MS after microwave digestion",
                    comments=(
                        f"literature_only=true; not_api_database=true; source_table=Table 3; "
                        f"township_n={stat.n}; statistic={stat.statistic}"
                    ),
                )
            )
        return records

    def _yabe_measurements(self) -> list[RawMeasurementRecord]:
        records = []
        for stat in self.YABE_CD_F_STATS:
            sample_id = stable_id(self.source_id, "yabe_2018_kabwe", stat.township, stat.statistic)
            records.append(
                RawMeasurementRecord(
                    measurement_id=stable_id(sample_id, "cadmium", stat.statistic),
                    sample_id=sample_id,
                    analyte_name="cadmium",
                    raw_value=stat.value,
                    raw_value_text=stat.raw_value_text,
                    raw_unit="mg/kg",
                    nondetect_flag=False,
                    raw_basis_text="dry weight",
                    page_or_sheet="yabe_2018_kabwe_feces_urine.pdf",
                    table_or_figure="Table 3",
                    row_label=f"Cd-F (mg/kg), {stat.township}",
                    column_label=stat.statistic,
                    extraction_method="literature_pdf_table_summary_stat",
                    confidence_score=0.98,
                )
            )
        return records
