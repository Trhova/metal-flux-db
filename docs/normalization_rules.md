# Normalization Rules

## Core principles

- Never overwrite raw values, units, qualifiers, or basis text.
- Normalize only within scientifically compatible matrix classes.
- Do not fabricate moisture-based or body-weight-based derivations when source data is absent.

## Solids and semi-solids

- Treat `ug/g` as equivalent to `mg/kg`.
- Treat `ng/g` as equivalent to `ug/kg`.
- Treat `ppm` as `mg/kg` only for solids and only when the matrix class supports it.

## Basis handling

Preserve and normalize basis into explicit categories:

- `dry_weight`
- `wet_weight`
- `fresh_weight`
- `as_sold`
- `as_prepared`
- `edible_portion`
- `whole_sample`

Moisture conversions are allowed only when moisture metadata is present and traceable.

## Matrix endpoints

- `fertilizer`, `soil`, `crop`, `food`, and `feces` normalize to canonical `mg/kg`
- `blood` and `water` normalize to canonical `ug/L`

For cross-layer comparison plots, solid matrices use `ppm` as the display unit and liquid matrices (`blood`, `water`) use an approximate `ppm-equivalent` of `ug/L / 1000`.

## Blood layer

Canonical blood endpoint is `ug/L`, with specimen subtype preserved separately.

## Water layer

Canonical water endpoint is `ug/L`, with water subtype preserved as `matrix_subtype`:

- `groundwater`
- `surface_water`
- `drinking_water`
- `irrigation_water`

Water is not mixed into the solid `mg/kg` tables. It is only converted to `ppm-equivalent` for cross-layer visualization by dividing `ug/L` by 1000.

## Nondetects

Raw nondetects stay raw. Analysis policies such as half-LOD or zero are applied only in derived analytical contexts.
