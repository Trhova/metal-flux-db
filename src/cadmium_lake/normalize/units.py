from __future__ import annotations

import math
from dataclasses import dataclass

from pint import UnitRegistry


ureg = UnitRegistry(autoconvert_offset_to_baseunit=True)
ureg.define("ug = microgram")
ureg.define("mcg = microgram")


BASIS_MAP = {
    "dry weight": "dry_weight",
    "dw": "dry_weight",
    "dry wt": "dry_weight",
    "dry basis": "dry_weight",
    "wet weight": "wet_weight",
    "ww": "wet_weight",
    "wet basis": "wet_weight",
    "fresh weight": "fresh_weight",
    "fw": "fresh_weight",
    "as sold": "as_sold",
    "as prepared": "as_prepared",
    "edible portion": "edible_portion",
    "whole sample": "whole_sample",
}


@dataclass
class NormalizationResult:
    canonical_value: float | None
    canonical_unit: str | None
    canonical_dimension: str | None
    conversion_rule: str | None
    converted_from_unit: str | None
    normalized_basis: str | None
    uncertainty_flag: bool


def normalize_basis(raw_basis_text: str | None) -> str | None:
    if not raw_basis_text:
        return None
    key = raw_basis_text.strip().lower()
    return BASIS_MAP.get(key, key.replace(" ", "_"))


def _safe_float(value: float | None) -> float | None:
    if value is None or math.isnan(value):
        return None
    return float(value)


def normalize_measurement(
    *,
    matrix_group: str,
    raw_value: float | None,
    raw_unit: str | None,
    raw_basis_text: str | None,
) -> NormalizationResult:
    normalized_basis = normalize_basis(raw_basis_text)
    if raw_value is None or raw_unit is None:
        return NormalizationResult(
            canonical_value=None,
            canonical_unit=None,
            canonical_dimension=None,
            conversion_rule=None,
            converted_from_unit=raw_unit,
            normalized_basis=normalized_basis,
            uncertainty_flag=True,
        )

    raw_unit = raw_unit.replace("µ", "u").strip()
    if matrix_group in {"fertilizer", "soil", "plant", "food", "feces"}:
        if raw_unit == "ppm":
            canonical_value = _safe_float(raw_value)
            return NormalizationResult(
                canonical_value=canonical_value,
                canonical_unit="mg/kg",
                canonical_dimension="mass_per_mass",
                conversion_rule="ppm_as_mg_per_kg_for_solids",
                converted_from_unit=raw_unit,
                normalized_basis=normalized_basis,
                uncertainty_flag=False,
            )
        if raw_unit in {"mg/kg", "ug/kg", "ug/g", "ng/g"}:
            if raw_unit == "mg/kg":
                canonical_value = raw_value
                rule = "identity"
            elif raw_unit == "ug/kg":
                canonical_value = raw_value / 1000.0
                rule = "ug_per_kg_to_mg_per_kg"
            elif raw_unit == "ug/g":
                canonical_value = raw_value
                rule = "ug_per_g_to_mg_per_kg"
            else:
                canonical_value = raw_value / 1000.0
                rule = "ng_per_g_to_mg_per_kg"
            return NormalizationResult(
                canonical_value=_safe_float(canonical_value),
                canonical_unit="mg/kg",
                canonical_dimension="mass_per_mass",
                conversion_rule=rule,
                converted_from_unit=raw_unit,
                normalized_basis=normalized_basis,
                uncertainty_flag=False,
            )

    if matrix_group == "blood" and raw_unit in {"ug/L", "mcg/L"}:
        return NormalizationResult(
            canonical_value=_safe_float(raw_value),
            canonical_unit="ug/L",
            canonical_dimension="mass_per_volume",
            conversion_rule="identity",
            converted_from_unit=raw_unit,
            normalized_basis=normalized_basis,
            uncertainty_flag=False,
        )

    return NormalizationResult(
        canonical_value=None,
        canonical_unit=None,
        canonical_dimension=None,
        conversion_rule=None,
        converted_from_unit=raw_unit,
        normalized_basis=normalized_basis,
        uncertainty_flag=True,
    )
