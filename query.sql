WITH congestion_data AS (
    SELECT
        congestion.area_name AS area_name,
        congestion.ppltn_time as ppltn_time,
        congestion.area_congestion_lvl AS area_congestion_lvl 
        congestion.area_ppltn_min AS area_ppltn_min,
        congestion.area_ppltn_max AS area_ppltn_max,
        congestion.gender_rate.male_ppltn_rate AS male_ppltn_rate,
        congestion.gender_rate.female_ppltn_rate AS female_ppltn_rate
    FROM 
        congestion
)


SELECT 
    area_name,
    ppltn_time,
    AVG(area_congestion_lvl) as avg_congestion_lvl,
    AVG(area_ppltn_min) as avg_ppltn_min,
    AVG(area_ppltn_max) as avg_ppltn_max,
    AVG(male_ppltn_rate) as avg_male_ppltn_rate,
    AVG(female_ppltn_rate) as avg_female_ppltn_rate,
FROM 
    congestion_data
GROUP BY
    area_name, ppltn_time