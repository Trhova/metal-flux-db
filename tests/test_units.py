from cadmium_lake.normalize.units import normalize_basis, normalize_measurement


def test_mgkg_identity():
    result = normalize_measurement(matrix_group="soil", raw_value=1.5, raw_unit="mg/kg", raw_basis_text="dry weight")
    assert result.canonical_value == 1.5
    assert result.canonical_unit == "mg/kg"
    assert result.normalized_basis == "dry_weight"


def test_ugkg_to_mgkg():
    result = normalize_measurement(matrix_group="food", raw_value=500.0, raw_unit="ug/kg", raw_basis_text="as sold")
    assert result.canonical_value == 0.5
    assert result.canonical_unit == "mg/kg"
    assert result.conversion_rule == "ug_per_kg_to_mg_per_kg"


def test_ug_per_g_to_mg_per_kg():
    result = normalize_measurement(matrix_group="plant", raw_value=0.7, raw_unit="ug/g", raw_basis_text="fresh weight")
    assert result.canonical_value == 0.7
    assert result.canonical_unit == "mg/kg"


def test_ng_per_g_to_ug_per_kg_equivalent_path():
    result = normalize_measurement(matrix_group="fertilizer", raw_value=800.0, raw_unit="ng/g", raw_basis_text=None)
    assert result.canonical_value == 0.8
    assert result.canonical_unit == "mg/kg"


def test_ppm_only_for_solids():
    solids = normalize_measurement(matrix_group="soil", raw_value=2.0, raw_unit="ppm", raw_basis_text=None)
    blood = normalize_measurement(matrix_group="blood", raw_value=2.0, raw_unit="ppm", raw_basis_text=None)
    assert solids.canonical_unit == "mg/kg"
    assert blood.canonical_unit is None


def test_blood_unit_identity():
    result = normalize_measurement(matrix_group="blood", raw_value=1.2, raw_unit="ug/L", raw_basis_text=None)
    assert result.canonical_value == 1.2
    assert result.canonical_unit == "ug/L"


def test_gut_unit_identity():
    result = normalize_measurement(matrix_group="gut", raw_value=15.0, raw_unit="ug/day", raw_basis_text=None)
    assert result.canonical_dimension == "intake_mass_per_day"


def test_basis_normalization():
    assert normalize_basis("dry weight") == "dry_weight"
