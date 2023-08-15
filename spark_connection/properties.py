"""
API에 필요한것들
"""
import sys
import configparser
from pathlib import Path


path = Path(__file__).parent.parent
parser = configparser.ConfigParser()
parser.read(f"{path}/config/setting.conf")

# # TOPIC
# PALACE_AND_CULTURAL_HERITAGE: str = parser.get("TOPIC", "palace_and_cultural_heritage")
# TOURIST_SPECIAL_ZONE: str = parser.get("TOPIC", "tourist_special_zone")
# DEVELOPED_MARKET: str = parser.get("TOPIC", "developed_market")
# POPULATED_AREA: str = parser.get("TOPIC", "populated_area")
# PARK: str = parser.get("TOPIC", "park")

# # TOPIC
# DEVELOPED_MARKET_NOT_FCST: str = parser.get("TOPIC", "developed_market_not_FCST")
# POPULATED_AREA_NOT_FCST: str = parser.get("TOPIC", "populated_area_not_FCST")
# PARK_NOT_FCST: str = parser.get("TOPIC", "park_not_FCST")
# PALACE_AND_CULTURAL_HERITAGE_NOT_FCST: str = parser.get(
#     "TOPIC", "palace_and_cultural_heritage_not_FCST"
# )
# TOURIST_SPECIAL_ZONE_NOT_FCST: str = parser.get(
#     "TOPIC", "tourist_special_zone_not_FCST"
# )

# AGE TOPIC
PALACE_AND_CULTURAL_HERITAGE_AGE: str = parser.get(
    "AGETOPIC", "palace_and_cultural_heritage_AGE"
)
TOURIST_SPECIAL_ZONE_AGE: str = parser.get("AGETOPIC", "tourist_special_zone_AGE")
DEVELOPED_MARKET_AGE: str = parser.get("AGETOPIC", "developed_market_AGE")
POPULATED_AREA_AGE: str = parser.get("AGETOPIC", "populated_area_AGE")
PARK_AGE: str = parser.get("AGETOPIC", "park_AGE")

PARK_NOT_FCST_AGE: str = parser.get("AGETOPIC", "park_not_FCST_AGE")
DEVELOPED_MARKET_NOT_FCST_AGE: str = parser.get(
    "AGETOPIC", "developed_market_not_FCST_AGE"
)
POPULATED_AREA_NOT_FCST_AGE: str = parser.get("AGETOPIC", "populated_area_not_FCST_AGE")
PALACE_AND_CULTURAL_HERITAGE_NOT_FCST_AGE: str = parser.get(
    "AGETOPIC", "palace_and_cultural_heritage_not_FCST_AGE"
)
TOURIST_SPECIAL_ZONE_NOT_FCST_AGE: str = parser.get(
    "AGETOPIC", "tourist_special_zone_not_FCST_AGE"
)
# ------------------------------------------------------------------------------

# GENDER TOPIC
PALACE_AND_CULTURAL_HERITAGE_GENDER: str = parser.get(
    "GENDERTOPIC", "palace_and_cultural_heritage_GENDER"
)
TOURIST_SPECIAL_ZONE_GENDER: str = parser.get(
    "GENDERTOPIC", "tourist_special_zone_GENDER"
)
DEVELOPED_MARKET_GENDER: str = parser.get("GENDERTOPIC", "developed_market_GENDER")
POPULATED_AREA_GENDER: str = parser.get("GENDERTOPIC", "populated_area_GENDER")
PARK_GENDER: str = parser.get("GENDERTOPIC", "park_GENDER")

PARK_NOT_FCST_GENDER: str = parser.get("GENDERTOPIC", "park_not_FCST_GENDER")
DEVELOPED_MARKET_NOT_FCST_GENDER: str = parser.get(
    "GENDERTOPIC", "developed_market_not_FCST_GENDER"
)
POPULATED_AREA_NOT_FCST_GENDER: str = parser.get(
    "GENDERTOPIC", "populated_area_not_FCST_GENDER"
)
PALACE_AND_CULTURAL_HERITAGE_NOT_FCST_GENDER: str = parser.get(
    "GENDERTOPIC", "palace_and_cultural_heritage_not_FCST_GENDER"
)
TOURIST_SPECIAL_ZONE_NOT_FCST_GENDER: str = parser.get(
    "GENDERTOPIC", "tourist_special_zone_not_FCST_GENDER"
)
# ------------------------------------------------------------------------------

