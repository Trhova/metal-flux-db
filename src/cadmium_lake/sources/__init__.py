from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.sources.canada import HealthCanadaTdsTraceElementsAdapter
from cadmium_lake.sources.europe import (
    EfsaSeaweedOccurrenceAdapter,
    EsdacLucasSoilAdapter,
    ForegsGeochemicalAtlasSoilAdapter,
    GemasSoilAdapter,
    Hbm4euParcCadmiumAdapter,
    UkFsaTotalDietAdapter,
)
from cadmium_lake.sources.fda import FdaTdsAdapter
from cadmium_lake.sources.feces_literature import CuratedFecesLiteratureAdapter
from cadmium_lake.sources.ireland import GsiDublinSoilAdapter
from cadmium_lake.sources.literature import LiteratureSearchAdapter
from cadmium_lake.sources.nhanes import NhanesBloodCadmiumAdapter
from cadmium_lake.sources.usgs import UsgsSoilAdapter
from cadmium_lake.sources.washington import WashingtonFertilizerAdapter
from cadmium_lake.sources.water import EeaWaterbaseWaterAdapter, UsgsWqpWaterAdapter

SOURCE_REGISTRY = {
    "washington_fertilizer": WashingtonFertilizerAdapter,
    "usgs_soil": UsgsSoilAdapter,
    "fda_tds": FdaTdsAdapter,
    "feces_literature": CuratedFecesLiteratureAdapter,
    "health_canada_tds_trace_elements": HealthCanadaTdsTraceElementsAdapter,
    "gsi_dublin_soil": GsiDublinSoilAdapter,
    "nhanes_blood_cadmium": NhanesBloodCadmiumAdapter,
    "literature_search": LiteratureSearchAdapter,
    "esdac_lucas_soil": EsdacLucasSoilAdapter,
    "gemas_soil": GemasSoilAdapter,
    "foregs_geochemical_atlas_soil": ForegsGeochemicalAtlasSoilAdapter,
    "uk_fsa_total_diet": UkFsaTotalDietAdapter,
    "hbm4eu_parc_cadmium": Hbm4euParcCadmiumAdapter,
    "usgs_wqp_water": UsgsWqpWaterAdapter,
    "eea_waterbase_water": EeaWaterbaseWaterAdapter,
    "efsa_seaweed_occurrence": EfsaSeaweedOccurrenceAdapter,
}

__all__ = [
    "BaseAdapter",
    "EeaWaterbaseWaterAdapter",
    "EfsaSeaweedOccurrenceAdapter",
    "EsdacLucasSoilAdapter",
    "ForegsGeochemicalAtlasSoilAdapter",
    "GemasSoilAdapter",
    "Hbm4euParcCadmiumAdapter",
    "ParsedPayload",
    "FdaTdsAdapter",
    "CuratedFecesLiteratureAdapter",
    "HealthCanadaTdsTraceElementsAdapter",
    "GsiDublinSoilAdapter",
    "LiteratureSearchAdapter",
    "NhanesBloodCadmiumAdapter",
    "SOURCE_REGISTRY",
    "UkFsaTotalDietAdapter",
    "UsgsSoilAdapter",
    "UsgsWqpWaterAdapter",
    "WashingtonFertilizerAdapter",
]
