"""
API에 필요한것들
"""
import sys
import configparser
from pathlib import Path


path = Path(__file__).parent.parent
parser = configparser.ConfigParser()
parser.read(f"{path}/config/setting.conf")



# AGE TOPIC
DEVMKT_AGE: str = parser.get("AGETOPIC", "dev_market_AGE")
PALCULT_AGE: str = parser.get("AGETOPIC", "palace_culture_AGE")
PARK_AGE: str = parser.get("AGETOPIC", "park_AGE")
POPAREA_AGE: str = parser.get("AGETOPIC", "pop_area_AGE")
TOURZONE_AGE: str = parser.get("AGETOPIC", "tourist_zone_AGE")

DEVMKT_NOF_AGE: str = parser.get("AGETOPIC", "dev_market_noFCST_AGE")
PALCULT_NOF_AGE: str = parser.get("AGETOPIC", "palace_culture_noFCST_AGE")
PARK_NOF_AGE: str = parser.get("AGETOPIC", "park_noFCST_AGE")
POPAREA_NOF_AGE: str = parser.get("AGETOPIC", "pop_area_noFCST_AGE")
TOURZONE_NOF_AGE: str = parser.get("AGETOPIC", "tourist_zone_noFCST_AGE")

# ------------------------------------------------------------------------------

# GENDER TOPIC
DEVMKT_GENDER: str = parser.get("GENDERTOPIC", "dev_market_GENDER")
PALCULT_GENDER: str = parser.get("GENDERTOPIC", "palace_culture_GENDER")
PARK_GENDER: str = parser.get("GENDERTOPIC", "park_GENDER")
POPAREA_GENDER: str = parser.get("GENDERTOPIC", "pop_area_GENDER")
TOURZONE_GENDER: str = parser.get("GENDERTOPIC", "tourist_zone_GENDER")

DEVMKT_NOF_GENDER: str = parser.get("GENDERTOPIC", "dev_market_noFCST_GENDER")
PALCULT_NOF_GENDER: str = parser.get("GENDERTOPIC", "palace_culture_noFCST_GENDER")
PARK_NOF_GENDER: str = parser.get("GENDERTOPIC", "park_noFCST_GENDER")
POPAREA_NOF_GENDER: str = parser.get("GENDERTOPIC", "pop_area_noFCST_GENDER")
TOURZONE_NOF_GENDER: str = parser.get("GENDERTOPIC", "tourist_zone_noFCST_GENDER")


# ------------------------------------------------------------------------------


# AGE TOPIC
AVG_DEVMKT_AGE: str = parser.get("AVEAGETOPIC", "avg_devMkt_AGE")
AVG_PALCULT_AGE: str = parser.get("AVEAGETOPIC", "avg_palCult_AGE")
AVG_PARK_AGE: str = parser.get("AVEAGETOPIC", "avg_park_AGE")
AVG_POPAREA_AGE: str = parser.get("AVEAGETOPIC", "avg_popArea_AGE")
AVG_TOURZONE_AGE: str = parser.get("AVEAGETOPIC", "avg_tourZone_AGE")

AVG_DEVMKT_NOF_AGE: str = parser.get("AVEAGETOPIC", "avg_devMkt_noF_AGE")
AVG_PALCULT_NOF_AGE: str = parser.get("AVEAGETOPIC", "avg_palCult_noF_AGE")
AVG_PARK_NOF_AGE: str = parser.get("AVEAGETOPIC", "avg_park_noF_AGE")
AVG_POPAREA_NOF_AGE: str = parser.get("AVEAGETOPIC", "avg_popArea_noF_AGE")
AVG_TOURZONE_NOF_AGE: str = parser.get("AVEAGETOPIC", "avg_tourZone_noF_AGE")

# ------------------------------------------------------------------------------

# GENDER TOPIC
AVG_DEVMKT_GEN: str = parser.get("AVEGENDERTOPIC", "avg_devMkt_GEN")
AVG_PALCULT_GEN: str = parser.get("AVEGENDERTOPIC", "avg_palCult_GEN")
AVG_PARK_GEN: str = parser.get("AVEGENDERTOPIC", "avg_park_GEN")
AVG_POPAREA_GEN: str = parser.get("AVEGENDERTOPIC", "avg_popArea_GEN")
AVG_TOURZONE_GEN: str = parser.get("AVEGENDERTOPIC", "avg_tourZone_GEN")

AVG_DEVMKT_NOF_GEN: str = parser.get("AVEGENDERTOPIC", "avg_devMkt_noF_GEN")
AVG_PALCULT_NOF_GEN: str = parser.get("AVEGENDERTOPIC", "avg_palCult_noF_GEN")
AVG_PARK_NOF_GEN: str = parser.get("AVEGENDERTOPIC", "avg_park_noF_GEN")
AVG_POPAREA_NOF_GEN: str = parser.get("AVEGENDERTOPIC", "avg_popArea_noF_GEN")
AVG_TOURZONE_NOF_GEN: str = parser.get("AVEGENDERTOPIC", "avg_tourZone_noF_GEN")

# ------------------------------------------------------------------------------
