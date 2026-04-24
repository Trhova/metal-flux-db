# Source Adapters 2

> 31 nodes · cohesion 0.11

## Key Concepts

- **BaseAdapter** (16 connections) — `src/cadmium_lake/sources/base.py`
- **._write_raw_file()** (16 connections) — `src/cadmium_lake/sources/base.py`
- **.fetch()** (13 connections) — `/home/trhova/metal-flux-db/src/cadmium_lake/sources/washington.py`
- **_download()** (11 connections) — `src/cadmium_lake/sources/base.py`
- **WashingtonFertilizerAdapter** (10 connections) — `/home/trhova/metal-flux-db/src/cadmium_lake/sources/washington.py`
- **utils.py** (7 connections) — `src/cadmium_lake/utils.py`
- **._client()** (5 connections) — `src/cadmium_lake/sources/base.py`
- **._records_from_raw_dir()** (4 connections) — `src/cadmium_lake/sources/base.py`
- **._timestamp()** (3 connections) — `src/cadmium_lake/sources/base.py`
- **._write_and_hash_existing()** (3 connections) — `src/cadmium_lake/sources/base.py`
- **.fetch()** (3 connections) — `src/cadmium_lake/sources/europe.py`
- **.fetch()** (3 connections) — `src/cadmium_lake/sources/europe.py`
- **.fetch()** (3 connections) — `src/cadmium_lake/sources/europe.py`
- **.fetch()** (3 connections) — `src/cadmium_lake/sources/europe.py`
- **.fetch()** (3 connections) — `src/cadmium_lake/sources/fda.py`
- **.fetch()** (3 connections) — `/home/trhova/metal-flux-db/src/cadmium_lake/sources/nhanes.py`
- **base.py** (3 connections) — `src/cadmium_lake/sources/base.py`
- **.fetch()** (3 connections) — `src/cadmium_lake/sources/usgs.py`
- **now_utc()** (3 connections) — `src/cadmium_lake/utils.py`
- **sha256_file()** (3 connections) — `src/cadmium_lake/utils.py`
- **._fetch_fertilizer_list()** (3 connections) — `/home/trhova/metal-flux-db/src/cadmium_lake/sources/washington.py`
- **.fetch()** (3 connections) — `/home/trhova/metal-flux-db/src/cadmium_lake/sources/water.py`
- **.source_record()** (2 connections) — `src/cadmium_lake/sources/base.py`
- **._staging_path()** (2 connections) — `src/cadmium_lake/sources/base.py`
- **write_json()** (2 connections) — `src/cadmium_lake/utils.py`
- *... and 6 more nodes in this community*

## Relationships

- No strong cross-community connections detected

## Source Files

- `/home/trhova/metal-flux-db/src/cadmium_lake/sources/nhanes.py`
- `/home/trhova/metal-flux-db/src/cadmium_lake/sources/washington.py`
- `/home/trhova/metal-flux-db/src/cadmium_lake/sources/water.py`
- `src/cadmium_lake/sources/base.py`
- `src/cadmium_lake/sources/europe.py`
- `src/cadmium_lake/sources/fda.py`
- `src/cadmium_lake/sources/usgs.py`
- `src/cadmium_lake/utils.py`

## Audit Trail

- EXTRACTED: 65 (47%)
- INFERRED: 74 (53%)
- AMBIGUOUS: 0 (0%)

---

*Part of the graphify knowledge wiki. See [[index]] to navigate.*