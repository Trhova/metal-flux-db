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

- `fertilizer`, `soil`, `plant`, `food`, and `feces` normalize to canonical `mg/kg`
- `blood` normalizes to canonical `ug/L`

For cross-layer comparison plots, solid matrices use `ppm` as the display unit and blood uses an approximate `ppm-equivalent` of `ug/L / 1000`.

## Blood layer

Canonical blood endpoint is `ug/L`, with specimen subtype preserved separately.

## Nondetects

Raw nondetects stay raw. Analysis policies such as half-LOD or zero are applied only in derived analytical contexts.
