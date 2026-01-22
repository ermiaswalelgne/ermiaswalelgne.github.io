SELECT
User as User,
Vendor,
Model,
Country as Country,
Tech,
#Host_app,
Month,
MIN(DayOfYear) as First_DayOfYear,
MAX(DayOfYear) as Last_DayOfYear,
MIN(DL_Bytes)/1000000 as First_bytes_MB,
MAX(DL_Bytes)/1000000 as Last_bytes_MB,
count(distinct session_id) as Sessions
FROM (
 SELECT
 a.installation_id as User,
 a.dl_bytes as DL_Bytes,
 session_id,
 #EXTRACT(DAY from a.timestamp at time zone "Europe/Helsinki") as Day,
 EXTRACT(DAYOFYEAR from a.timestamp at time zone "Europe/Helsinki") as DayOfYear,
 EXTRACT(MONTH from a.timestamp at time zone "Europe/Helsinki") as Month,
 a.tech as Tech,
 b.client_brand as Host_app,
 b.device_model as Model,
 b.device_vendor as Vendor,
 b.subscriber_country as Country
 FROM `netradar-prod.Netradar.data_usage`  a JOIN
 `netradar-prod.Netradar.session` b
 ON a.installation_id = b.installation_id and DATE(a.timestamp) = DATE(b.started_wallclock)
 WHERE
 DATE(b.started_wallclock) >= "2018-04-01"
 and DATE(b.started_wallclock) <= "2018-09-30" #current_date()
 and DATE(a.timestamp) >= "2018-06-01"
 and DATE(a.timestamp) < "2018-09-01"
 and a.dl_bytes IS NOT NULL and
 b.subscriber_country IS NOT NULL
 and b.network_mcc = "244" #in ('234','244','262')
)
GROUP BY User, Vendor, Model, Country, Tech, Month #Host_app, Month
HAVING (Last_DayOfYear > First_DayOfYear) and (Last_bytes_MB > First_bytes_MB) and sessions > 200 and country = "Finland"
ORDER BY Country, Tech, month


---This one works ===== check

SELECT
		User as User,
		Vendor,
		Model,
		Country as Country,
		Tech,
		MIN(DL_Bytes)/1000000 as First_bytes_MB,
		MAX(DL_Bytes)/1000000 as Last_bytes_MB,

		MIN(DayOfYear) as First_DayOfYear,
		MAX(DayOfYear) as Last_DayOfYear,
		count(distinct session_id) as Sessions

		FROM  ( SELECT a.tech as Tech,
			   			a.dl_bytes as DL_Bytes,
			   			a.installation_id as User,
			   			EXTRACT(MONTH from a.timestamp at time zone 'Europe/Helsinki') as Month,
			   			EXTRACT(DOY from a.timestamp at time zone 'Europe/Helsinki') as DayOfYear,
			    		session_id,
						 b.client_brand as Host_app,
						 b.device_model as Model,
						 b.device_vendor as Vendor,
						 b.subscriber_country as Country
				FROM public.data_usage  a JOIN public.session b
				ON a.installation_id = b.installation_id
				WHERE
					DATE(b.started_wallclock) >= '2018-04-01'
			   		and b.network_mcc = '244'
			   LIMIT 100
		) as joint
		GROUP BY User, Vendor, Model, Country, Tech, Month --#Host_app, Month
		HAVING (Country ='Finland') and (MAX(DayOfYear) > MIN(DayOfYear)) and
				(MAX(DL_Bytes)/1000000 > MIN(DL_Bytes)/1000000 )

		LIMIT 100;
