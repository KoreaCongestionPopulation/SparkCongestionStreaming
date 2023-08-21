SELECT
    congestion.area_name AS area_name,
    congestion.ppltn_time AS ppltn_time,
    congestion.area_congestion_lvl AS area_congestion_lvl,
    congestion.area_ppltn_min AS area_ppltn_min,
    congestion.area_ppltn_max AS area_ppltn_max,
    congestion.gender_rate.male_ppltn_rate AS male_ppltn_rate,
    congestion.gender_rate.female_ppltn_rate AS female_ppltn_rate
FROM 
    congestion


SELECT 
    cd.area_name,
    cd.ppltn_time,
    AVG(cd.area_congestion_lvl) as avg_congestion_lvl,
    AVG(cd.area_ppltn_min) as avg_ppltn_min,
    AVG(cd.area_ppltn_max) as avg_ppltn_max,
    AVG(cd.male_ppltn_rate) as avg_male_ppltn_rate,
    AVG(cd.female_ppltn_rate) as avg_female_ppltn_rate
FROM 
    congestion_data AS cd
GROUP BY
    cd.area_name, cd.ppltn_time




USE YourDatabaseName;

WITH CombinedData AS(
	SELECT 
		A.area_name as area_name, 
		A.ppltn_time as ppltn_time,
		A.area_congestion_lvl as area_congestion_lvl,
        A.area_congestion_msg as area_congestion_msg,
		A.area_ppltn_min as area_ppltn_min,
		A.area_ppltn_max as area_ppltn_max,
		F.fcst_time as fcst_time,
		F.fcst_congest_lvl as fcst_congest_lvl,
		F.fcst_ppltn_min as fcst_ppltn_min,
		F.fcst_ppltn_max as fcst_ppltn_max,
		R.ppltn_rate_0 as ppltn_rate_0,
		R.ppltn_rate_10 as ppltn_rate_10,
		R.ppltn_rate_20 as ppltn_rate_20,
		R.ppltn_rate_30 as ppltn_rate_30,
		R.ppltn_rate_40 as ppltn_rate_40,
		R.ppltn_rate_50 as ppltn_rate_50,
		R.ppltn_rate_60 as ppltn_rate_60,
		R.ppltn_rate_70 as ppltn_rate_70
	FROM 
		AreaData AS A
	JOIN 
		ForecastData AS F ON A.area_name = F.area_name 
	JOIN 
		AgeRateData AS R ON A.area_name = R.area_name
)

SELECT
    area_name,
    ppltn_time,
    area_congestion_msg, 
    AVG(area_congestion_lvl) AS avg_area_congestion_lvl,
    AVG(area_ppltn_min) AS avg_area_ppltn_min,
    AVG(area_ppltn_max) AS avg_area_ppltn_max,
    AVG(fcst_congest_lvl) as fcst_congest_lvl,
    AVG(ppltn_rate_0) AS avg_ppltn_rate_0,
    AVG(ppltn_rate_10) AS avg_ppltn_rate_10,
    AVG(ppltn_rate_20) AS avg_ppltn_rate_20,
    AVG(ppltn_rate_30) AS avg_ppltn_rate_30,
    AVG(ppltn_rate_40) AS avg_ppltn_rate_40,
    AVG(ppltn_rate_50) AS avg_ppltn_rate_50,
    AVG(ppltn_rate_60) AS avg_ppltn_rate_60,
    AVG(ppltn_rate_70) AS avg_ppltn_rate_70
FROM
    CombinedData
GROUP BY
    area_name, ppltn_time, area_congestion_msg

;



