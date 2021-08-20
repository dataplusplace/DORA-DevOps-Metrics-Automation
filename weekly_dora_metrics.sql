WITH dates_cte AS
(
SELECT	LAG(week_start, 1) OVER (ORDER BY order_num) AS last_week_start
	,LAG(week_end, 1) OVER (ORDER BY order_num) AS last_week_end
	,LAG(week_start, 2) OVER (ORDER BY order_num) AS prev_week_start
	,LAG(week_end, 2) OVER (ORDER BY order_num) AS prev_week_end
	,to_char(now() AT time zone 'America/New York', 'YYYY-MM') AS mth_num
	,to_char(now() AT time zone 'America/New York', 'YYYY-Q') AS qtr_num
FROM	week_interval
ORDER BY order_num DESC
LIMIT 	1
)
,mth_days_cte AS
(
SELECT	SUM(CASE WHEN dow IN (1,2,3,4,5) THEN 1 ELSE 0 END) AS mth_bus_days
FROM 	(
	SELECT EXTRACT (dow FROM generate_series(to_char(now() AT time zone 'America/New York','YYYY-MM-01')::date, (now() AT time zone 'America/New York')::date - 1, '1 day'::interval)) as dow
	) foo
)
,qtr_days_cte AS
(
SELECT 	SUM(CASE WHEN dow IN (1,2,3,4,5) THEN 1 ELSE 0 END) AS qtr_bus_days
FROM 	(
	SELECT EXTRACT (dow FROM generate_series((to_char(now() AT time zone 'America/New York','YYYY') || lpad(((to_char(now() AT time zone 'America/New York','Q')::int * 3) - 2)::TEXT, 2, '0') || '01')::date, (now() AT time zone 'America/New York')::date - 1, '1 day'::interval)) as dow
	) foo
)
,tk_cte AS 
(
SELECT	issue_key
	,start_time
	,completion_time
	,resolution
	,CASE resolution
		WHEN 'Completed - Success'
		THEN 1
		ELSE 0
		END AS success_ind        
FROM	tickets	
WHERE   status = 'Closed'
AND     resolution IN
		(
		'Completed - Success',
		'Completed - Downtime',
		'Failed - Rollback'
		)
AND     completion_time >= (CURRENT_DATE AT time zone 'America/New York')::date - 100
)
,inc_cte AS 
(
SELECT  issue_key
	,inc_end_time
        ,(EXTRACT(EPOCH FROM (inc_end_time)) - EXTRACT(EPOCH FROM (inc_start_time))) / 60 AS ttr
        ,affected_users
FROM    incident_tickets inc
WHERE   impact = 'Impacted'
AND	component NOT IN ('third_party')
AND 	severity IN ('Med','Hi')
AND     inc_end_time >= (CURRENT_DATE AT time zone 'America/New York')::date - 100
)
,df_lweek_cte AS
(
SELECT  'daily_deployments' AS metric
	,(COUNT(DISTINCT lw.issue_key) / 5.0)::numeric(4,1)::text AS last_week
FROM    dates_cte dt
	LEFT JOIN tk_cte lw
		ON lw.completion_time BETWEEN dt.last_week_start AND dt.last_week_end
)
,df_pweek_cte AS
(
SELECT  (COUNT(DISTINCT pw.issue_key) / 5.0)::numeric(4,1)::text AS prev_week
FROM    dates_cte dt
	LEFT JOIN tk_cte pw
		ON pw.completion_time BETWEEN dt.prev_week_start AND dt.prev_week_end
)
,df_mth_cte AS
(
SELECT  (COUNT(DISTINCT mth.issue_key) / (SELECT * FROM mth_days_cte))::numeric(4,1)::text AS curr_mth
FROM    dates_cte dt
	LEFT JOIN tk_cte mth
		ON TO_CHAR(mth.completion_time, 'YYYY-MM') = dt.mth_num
)
,df_qtr_cte AS
(
SELECT  (COUNT(DISTINCT qtr.issue_key) / (SELECT * FROM qtr_days_cte))::numeric(4,1)::text AS curr_qtr
FROM    dates_cte dt
	LEFT JOIN tk_cte qtr
		ON TO_CHAR(qtr.completion_time, 'YYYY-Q') = dt.qtr_num
)
,success_lweek_cte AS
(
SELECT  'success' AS metric
	,(100.0 * SUM(lw.success_ind) / COUNT(lw.success_ind))::numeric(4,1)::text AS last_week
FROM    dates_cte dt
	LEFT JOIN tk_cte lw
		ON lw.completion_time BETWEEN dt.last_week_start AND dt.last_week_end
)
,success_pweek_cte AS
(
SELECT  (100.0 * SUM(pw.success_ind) / COUNT(pw.success_ind))::numeric(4,1)::text AS prev_week
FROM    dates_cte dt
	LEFT JOIN tk_cte pw
		ON pw.completion_time BETWEEN dt.prev_week_start AND dt.prev_week_end
)
,success_mth_cte AS
(
SELECT  (100.0 * SUM(mth.success_ind) / COUNT(mth.success_ind))::numeric(4,1)::text AS curr_mth
FROM    dates_cte dt
	LEFT JOIN tk_cte mth
		ON TO_CHAR(mth.completion_time, 'YYYY-MM') = dt.mth_num
)
,success_qtr_cte AS
(
SELECT  (100.0 * SUM(qtr.success_ind) / COUNT(qtr.success_ind))::numeric(4,1)::text AS curr_qtr
FROM    dates_cte dt
	LEFT JOIN tk_cte qtr
		ON TO_CHAR(qtr.completion_time, 'YYYY-Q') = dt.qtr_num
)
,lead_time_lweek_cte AS
(
SELECT  'lead_time' AS metric
	,AVG(lw.lead_time)::numeric(5,1)::text AS last_week
FROM    dates_cte dt
	LEFT JOIN tk_cte lw
		ON lw.completion_time BETWEEN dt.last_week_start AND dt.last_week_end
)
,leadtime_pweek_cte AS
(
SELECT  AVG(pw.lead_time)::numeric(5,1)::text AS prev_week
FROM    dates_cte dt
	LEFT JOIN tk_cte pw
		ON pw.completion_time BETWEEN dt.prev_week_start AND dt.prev_week_end
)
,leadtime_mth_cte AS
(
SELECT  AVG(mth.lead_time)::numeric(5,1)::text AS curr_mth
FROM    dates_cte dt
	LEFT JOIN tk_cte mth
		ON TO_CHAR(mth.completion_time, 'YYYY-MM') = dt.mth_num
)
,leadtime_qtr_cte AS
(
SELECT  AVG(qtr.lead_time)::numeric(5,1)::text AS curr_qtr
FROM    dates_cte dt
	LEFT JOIN tk_cte qtr
		ON TO_CHAR(qtr.completion_time, 'YYYY-Q') = dt.qtr_num
)
,inc_lweek_cte AS
(
SELECT  'weekly_impacted_minutes' AS metric
	,COALESCE(SUM(lw.ttr * lw.affected_users) / 1000000.0,0)::numeric(12,2) || 'M' AS last_week
FROM    dates_cte dt
	LEFT JOIN inc_cte lw
		ON lw.inc_end_time BETWEEN dt.last_week_start AND dt.last_week_end
)
,inc_pweek_cte AS
(
SELECT  COALESCE(SUM(pw.ttr * pw.affected_users) / 1000000.0,0)::numeric(12,2) || 'M' AS prev_week
FROM    dates_cte dt
	LEFT JOIN inc_cte pw
		ON pw.inc_end_time BETWEEN dt.prev_week_start AND dt.prev_week_end
)
,inc_mth_cte AS
(
SELECT  COALESCE((SUM(mth.ttr * mth.affected_users)) / (SELECT mth_bus_days / 5.0 FROM mth_days_cte) / 1000000.0,0)::numeric(12,2) || 'M' AS curr_mth
FROM    dates_cte dt
	LEFT JOIN inc_cte mth
		ON TO_CHAR(mth.inc_end_time, 'YYYY-MM') = dt.mth_num
)
,inc_qtr_cte AS
(
SELECT  COALESCE((SUM(qtr.ttr * qtr.affected_users)) / (SELECT qtr_bus_days / 5.0 FROM qtr_days_cte) / 1000000.0,0)::numeric(12,2) || 'M' AS curr_qtr
FROM    dates_cte dt
	LEFT JOIN inc_cte qtr
		ON TO_CHAR(qtr.inc_end_time, 'YYYY-Q') = dt.qtr_num
)
SELECT 	*
FROM	(
	SELECT	*
	FROM	df_lweek_cte, df_pweek_cte, df_mth_cte, df_qtr_cte
	UNION
	SELECT	*
	FROM	success_lweek_cte, success_pweek_cte, success_mth_cte, success_qtr_cte
	UNION
	SELECT	*
	FROM	leadtime_lweek_cte, leadtime_pweek_cte, leadtime_mth_cte, leadtime_qtr_cte
	UNION
	SELECT	*
	FROM	inc_lweek_cte, inc_pweek_cte, inc_mth_cte, inc_qtr_cte
	) foo
ORDER BY
	CASE metric
		WHEN 'daily_deployments' 
		THEN 1
		WHEN 'success'
		THEN 2
		WHEN 'lead_time'
		THEN 3
		WHEN 'weekly_impacted_minutes'
		THEN 4
		END
;
