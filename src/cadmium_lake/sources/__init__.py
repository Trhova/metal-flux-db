from cadmium_lake.sources.base import BaseAdapter, ParsedPayload
from cadmium_lake.sources.europe import (
    EsdacLucasSoilAdapter,
    GemasSoilAdapter,
    Hbm4euParcCadmiumAdapter,
    UkFsaTotalDietAdapter,
)
from cadmium_lake.sources.fda import FdaTdsAdapter
from cadmium_lake.sources.literature import LiteratureSearchAdapter
from cadmium_lake.sources.nhanes import NhanesBloodCadmiumAdapter
from cadmium_lake.sources.usgs import UsgsSoilAdapter
from cadmium_lake.sources.washington import WashingtonFertilizerAdapter

SOURCE_REGISTRY = {
    "washington_fertilizer": WashingtonFertilizerAdapter,
    "usgs_soil": UsgsSoilAdapter,
    "fda_tds": FdaTdsAdapter,
    "nhanes_blood_cadmium": NhanesBloodCadmiumAdapter,
    "literature_search": LiteratureSearchAdapter,
    "esdac_lucas_soil": EsdacLucasSoilAdapter,
    "gemas_soil": GemasSoilAdapter,
    "uk_fsa_total_diet": UkFsaTotalDietAdapter,
    "hbm4eu_parc_cadmium": Hbm4euParcCadmiumAdapter,
}

__all__ = [
    "BaseAdapter",
    "EsdacLucasSoilAdapter",
    "GemasSoilAdapter",
    "Hbm4euParcCadmiumAdapter",
    "ParsedPayload",
    "FdaTdsAdapter",
    "LiteratureSearchAdapter",
    "NhanesBloodCadmiumAdapter",
    "SOURCE_REGISTRY",
    "UkFsaTotalDietAdapter",
    "UsgsSoilAdapter",
    "WashingtonFertilizerAdapter",
]
