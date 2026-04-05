from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class SourceRecord(BaseModel):
    source_id: str
    source_name: str
    layer: str
    organization: str | None = None
    country_or_region: str | None = None
    access_type: str
    source_url: str
    license_or_terms: str | None = None
    retrieval_method: str
    priority: str
    notes: str | None = None
    parser_class: str | None = None
    status: str | None = None
    maturity: str | None = None


class SourceFileRecord(BaseModel):
    file_id: str
    source_id: str
    original_url: str
    local_path: str
    mime_type: str | None = None
    sha256: str
    retrieved_at: datetime
    parser_version: str


class StudyRecord(BaseModel):
    study_id: str
    source_id: str
    study_title: str | None = None
    year_start: int | None = None
    year_end: int | None = None
    publication_year: int | None = None
    country: str | None = None
    citation: str | None = None
    doi: str | None = None
    pmid: str | None = None
    repository_doi: str | None = None
    notes: str | None = None


class SampleRecord(BaseModel):
    sample_id: str
    source_id: str
    study_id: str | None = None
    matrix_group: str
    matrix_subtype: str | None = None
    sample_name: str | None = None
    specimen_or_part: str | None = None
    edible_portion_flag: bool | None = None
    dry_wet_basis: str | None = None
    as_sold_prepared_flag: str | None = None
    location_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    country: str | None = None
    collection_date: str | None = None
    collection_year: int | None = None
    publication_year: int | None = None
    year_for_plotting: int | None = None
    year_for_plotting_source: str | None = None
    analyte_method: str | None = None
    lod: float | None = None
    loq: float | None = None
    comments: str | None = None


class RawMeasurementRecord(BaseModel):
    measurement_id: str
    sample_id: str
    analyte_name: str
    raw_value: float | None = None
    raw_value_text: str
    raw_unit: str | None = None
    nondetect_flag: bool = False
    detection_qualifier: str | None = None
    raw_basis_text: str | None = None
    page_or_sheet: str | None = None
    table_or_figure: str | None = None
    row_label: str | None = None
    column_label: str | None = None
    extraction_method: str
    confidence_score: float = Field(default=1.0, ge=0.0, le=1.0)


class NormalizedMeasurementRecord(BaseModel):
    measurement_id: str
    canonical_value: float | None = None
    canonical_unit: str | None = None
    canonical_dimension: str | None = None
    conversion_rule: str | None = None
    converted_from_unit: str | None = None
    normalized_basis: str | None = None
    moisture_adjustment_applied: bool = False
    body_weight_adjustment_applied: bool = False
    uncertainty_flag: bool = False
    derived_flag: bool = False
    derivation_notes: str | None = None


class SummaryMeasurementRecord(BaseModel):
    summary_measurement_id: str
    source_id: str
    study_id: str | None = None
    matrix_group: str
    matrix_subtype: str | None = None
    analyte_name: str
    statistic_name: str
    subgroup: str | None = None
    item_label: str | None = None
    raw_value_text: str
    summary_value: float | None = None
    lower_value: float | None = None
    upper_value: float | None = None
    summary_unit: str | None = None
    summary_dimension: str | None = None
    raw_basis_text: str | None = None
    page_or_sheet: str | None = None
    table_or_figure: str | None = None
    extraction_method: str
    confidence_score: float = Field(default=1.0, ge=0.0, le=1.0)
    derived_flag: bool = False
    notes: str | None = None


class LinkageEdgeRecord(BaseModel):
    edge_id: str
    from_sample_id: str
    to_sample_id: str
    relationship_type: Literal[
        "same_field",
        "same_study",
        "same_food_item",
        "same_subject",
        "modeled_from",
        "paired_measurement",
    ]
    linkage_confidence: float = Field(ge=0.0, le=1.0)
    notes: str | None = None


class ReviewQueueRecord(BaseModel):
    review_id: str
    source_id: str
    study_id: str | None = None
    local_path: str | None = None
    page_or_sheet: str | None = None
    table_or_figure: str | None = None
    issue_type: str
    issue_summary: str
    parsing_feasibility: str | None = None
    status: Literal["open", "reviewed", "resolved"] = "open"
    notes: str | None = None


class PipelineRunRecord(BaseModel):
    run_id: str
    command: str
    started_at: datetime
    completed_at: datetime | None = None
    status: Literal["running", "completed", "failed"]


class PipelineRunStepRecord(BaseModel):
    run_step_id: str
    run_id: str
    step_name: str
    started_at: datetime
    completed_at: datetime | None = None
    status: Literal["running", "completed", "failed"]
    details: str | None = None
