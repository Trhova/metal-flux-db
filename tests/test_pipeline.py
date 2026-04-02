from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd
import polars as pl
import pyreadstat

from cadmium_lake.io import read_duckdb_table
from cadmium_lake.normalize.pipeline import run_normalization
from cadmium_lake.pipeline import fetch_sources, initialize_catalog_tables, parse_sources, run_literature_search
from cadmium_lake.qa.checks import run_qa_checks
from cadmium_lake.sources.europe import extract_tableau_metadata, extract_uk_fsa_cadmium_rows
from cadmium_lake.sources import SOURCE_REGISTRY
from cadmium_lake.viz.views import build_views


def test_fetch_manifest_hash_stability(monkeypatch):
    initialize_catalog_tables()

    def fake_download(self, url):
        return b"fixture-content"

    def fake_list(self, letter):
        return [{"AutoKey": 1, "ProductNumber": "0001-0003", "ProductName": "Fixture Fertilizer"}] if letter == "A" else []

    def fake_detail(self, auto_key):
        return {
            "Product": {
                "prod_number": "0001-0003",
                "prod_name": "Fixture Fertilizer",
                "cd_met_type": "=",
                "pc_cd_metals": "12.5",
            }
        }

    monkeypatch.setattr(SOURCE_REGISTRY["washington_fertilizer"], "_download", fake_download)
    monkeypatch.setattr(SOURCE_REGISTRY["washington_fertilizer"], "_fetch_fertilizer_list", fake_list)
    monkeypatch.setattr(SOURCE_REGISTRY["washington_fertilizer"], "_fetch_fertilizer_detail", fake_detail)
    results = fetch_sources(source="washington_fertilizer")
    assert results["washington_fertilizer"] >= 1
    source_files = read_duckdb_table("source_files")
    assert source_files.height >= 1
    assert source_files["sha256"][0]


def test_parse_smoke_for_all_sources(tmp_path):
    initialize_catalog_tables()
    seed_washington_fixture()
    seed_usgs_fixture()
    seed_fda_fixture()
    seed_nhanes_fixture()

    parse_sources(source="washington_fertilizer")
    parse_sources(source="usgs_soil")
    parse_sources(source="fda_tds")
    parse_sources(source="nhanes_blood_cadmium")

    samples = read_duckdb_table("samples")
    raw = read_duckdb_table("measurements_raw")
    assert samples.height >= 4
    assert raw.height >= 4


def test_literature_search_capture(monkeypatch):
    initialize_catalog_tables()

    adapter_cls = SOURCE_REGISTRY["literature_search"]

    def fake_json(self, url):
        if "europepmc" in url:
            return {"resultList": {"result": [{"title": "Cadmium in rice", "doi": "10.1/abc", "pmid": "123", "pubYear": "2022"}]}}
        if "openalex" in url:
            return {"results": [{"display_name": "Cadmium bioaccessibility", "doi": "10.1/def", "publication_year": 2023, "ids": {}, "locations": []}]}
        return {"data": [{"attributes": {"titles": [{"title": "Cadmium supplement"}], "doi": "10.1/ghi", "publicationYear": 2021, "url": "https://example.org"}}]}

    monkeypatch.setattr(adapter_cls, "_json", fake_json)
    results = run_literature_search()
    assert results["studies_or_batches"] >= 3
    review = read_duckdb_table("review_queue")
    assert review.height >= 1


def test_literature_curated_extractors(monkeypatch):
    initialize_catalog_tables()

    adapter_cls = SOURCE_REGISTRY["literature_search"]

    def fake_json(self, url):
        if "PMCID:PMC12733840" in url or "PMCID%3APMC12733840" in url:
            return {
                "resultList": {
                    "result": [
                        {
                            "title": "Rice cadmium paper",
                            "doi": "10.1000/rice",
                            "pmid": "111",
                            "pmcid": "PMC12733840",
                            "pubYear": "2024",
                        }
                    ]
                }
            }
        if "PMCID:PMC12846066" in url or "PMCID%3APMC12846066" in url:
            return {
                "resultList": {
                    "result": [
                        {
                            "title": "Gut cadmium paper",
                            "doi": "10.1000/gut",
                            "pmid": "222",
                            "pmcid": "PMC12846066",
                            "pubYear": "2024",
                        }
                    ]
                }
            }
        return {"resultList": {"result": []}, "esearchresult": {"idlist": []}, "result": {"uids": []}, "data": []}

    def fake_download_text(self, url):
        if "PMC12733840" in url:
            return """
            <html><body>
            <table>
              <tr><th>Site</th><th colspan="3">Cd Concentrations</th></tr>
              <tr><th>Site</th><th>TCd-S</th><th>ACd-S</th><th>TCd-G (mg/kg)</th></tr>
              <tr><td>MC</td><td>2.00 ± 0.10 c</td><td>1.26 ± 0.08 c</td><td>0.13 ± 0.03 b</td></tr>
              <tr><td>SC1</td><td>3.29 ± 0.13 a</td><td>2.12 ± 0.09 b</td><td>0.16 ± 0.04 b</td></tr>
            </table>
            </body></html>
            """
        return """
        <html><body>
        <table><tr><th>ignore</th></tr></table>
        <table>
          <tr><th>Mushroom Fruiting Bodies</th><th>Biological Accessibility of Cadmium (%)</th></tr>
          <tr><th>Body</th><th>Accessibility</th></tr>
          <tr><td><em>A. blazei</em></td><td>5.73 ± 0.04</td></tr>
          <tr><td><em>L. edodes</em></td><td>N</td></tr>
        </table>
        </body></html>
        """

    monkeypatch.setattr(adapter_cls, "_json", fake_json)
    monkeypatch.setattr(adapter_cls, "_download_text", fake_download_text)
    monkeypatch.setattr(adapter_cls, "_download", lambda self, url: b"pdf-bytes")

    results = run_literature_search(layer="plant")
    assert results["samples"] >= 2
    samples = read_duckdb_table("samples").filter(pl.col("source_id") == "literature_search")
    raw = read_duckdb_table("measurements_raw").join(samples.select("sample_id"), on="sample_id", how="inner")
    assert samples.height >= 2
    assert raw.height >= 2


def test_uk_fsa_summary_extractor(tmp_path):
    path = tmp_path / "uk_fsa.xlsx"
    frame = pd.DataFrame(
        [
            [None, "Al", "Cd", "Zn"],
            ["Bread", "1.0", "0.024", "12"],
            ["Fish", "0.8", "0-0.0062", "3.1"],
        ],
        columns=["Food Group", "Mean Exposure (µg/kg bw/d)", "Unnamed: 2", "Unnamed: 3"],
    )
    with pd.ExcelWriter(path) as writer:
        frame.to_excel(writer, sheet_name="Age class 19 to adult", index=False)
    rows = extract_uk_fsa_cadmium_rows(path)
    assert len(rows) == 2
    assert rows[0]["raw_unit"] == "ug/kg_bw/day"
    assert rows[0]["analyte_name"] == "cadmium"


def test_tableau_metadata_extractor():
    html = """
    <html><body>
    <object class="tableauViz">
      <param name="host_url" value="https://report.vito.be/">
      <param name="site_root" value="/t/EU-HBM">
      <param name="name" value="EuropeanHumanBioMonitoringData/HBM4EU">
    </object>
    </body></html>
    """
    metadata = extract_tableau_metadata(html)
    assert metadata["host_url"] == "https://report.vito.be/"
    assert metadata["name"] == "EuropeanHumanBioMonitoringData/HBM4EU"


def test_normalize_qa_and_views():
    initialize_catalog_tables()
    seed_washington_fixture()
    seed_usgs_fixture()
    seed_fda_fixture()
    seed_nhanes_fixture()
    parse_sources()
    normalized = run_normalization()
    assert normalized.height >= 4
    qa_outputs = run_qa_checks()
    assert "provenance_completeness_report" in qa_outputs
    build_views()
    chain = read_duckdb_table("measurements_normalized")
    assert chain.height >= 4


def seed_washington_fixture():
    raw_dir = Path("data/raw/washington_fertilizer")
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath("landing.html").write_text(
        "<html><body>fixture</body></html>",
        encoding="utf-8",
    )
    raw_dir.joinpath("fertilizer_detail.json").write_text(
        """
        [
          {
            "Product": {
              "prod_number": "0001-0003",
              "prod_name": "Fertilizer A",
              "cd_met_type": "=",
              "pc_cd_metals": "12.5"
            }
          }
        ]
        """,
        encoding="utf-8",
    )


def seed_usgs_fixture():
    raw_dir = Path("data/raw/usgs_soil")
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "soil.csv"
    csv_path.write_text("sample_id,latitude,longitude,cadmium_mgkg\nS1,47.1,-122.3,0.8\n", encoding="utf-8")
    with zipfile.ZipFile(raw_dir / "usgs_soildata.zip", "w") as zf:
        zf.write(csv_path, arcname="soil.csv")


def seed_fda_fixture():
    raw_dir = Path("data/raw/fda_tds")
    raw_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(raw_dir / "https:__www.fda.gov_media_151752_download", "w") as zf:
        zf.writestr(
            "Elements 2003.txt",
            "MB\tFood No.\tFood Name\tAnal Type\tSample Qualifier\tReplicate No.\tElement\tConc\tUnit\tTrace\tLOD\tLOQ\tResult Qualifier and Remarks\tMethod\tInstrument\tBatch ID\n"
            '200301\t1\t"Spinach"\tO\t\t\tCadmium\t0.04\tmg/kg\t\t0.002\t0.005\t\tMethod\tInst\t316\n',
        )


def seed_nhanes_fixture():
    raw_dir = Path("data/raw/nhanes_blood_cadmium")
    raw_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([{"SEQN": 1001, "LBXBCD": 0.31, "SDDSRVYR": 10}])
    pyreadstat.write_xport(frame, raw_dir / "cadmium_fixture.xpt")
