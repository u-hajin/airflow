WITH today_dag AS(	-- VIEW 생성
	SELECT dag_id, next_dagrun_data_interval_start, next_dagrun_data_interval_end
	FROM dag
	WHERE is_paused = FALSE
	AND is_active = TRUE
	AND schedule_interval NOT IN('null', '"Dataset"')
	AND (date(next_dagrun_data_interval_start) BETWEEN current_date -1 AND current_date -- 어제나 오늘 수행된 대상
	OR date(next_dagrun_data_interval_end) BETWEEN current_date -1 AND current_date)	-- 어제나 오늘이 배치일인 대상
)
, today_dagrun AS (
	SELECT
		dag_id,
		COUNT(1) AS run_count,
		COUNT(CASE WHEN state = 'success' THEN 'success' END) AS success_count,	-- 어제, 오늘 success로 끝난 개수
		COUNT(CASE WHEN state = 'failed' THEN 'failed' END) AS failed_count,	-- 어제, 오늘 fail로 끝난 개수
		COUNT(CASE WHEN state = 'running' THEN 'running' END) AS running_count,	-- 현 시점 running인 것
		MAX(CASE WHEN state = 'failed' THEN data_interval_end END) AS last_failed_date,	-- 가장 마지막으로 fail한 날짜
		MAX(CASE WHEN state = 'success' THEN data_interval_end END) AS last_success_date	-- 가장 마지막으로 success한 날짜
	FROM dag_run
	WHERE date(data_interval_end) BETWEEN current_date -1 AND current_date
	GROUP BY dag_id
)
SELECT
	d.dag_id,
	coalesce(r.run_count, 0),
	coalesce(r.success_count, 0),
    coalesce(r.failed_count, 0),
	coalesce(r.running_count, 0),
	r.last_failed_date,
	r.last_success_date,
	next_dagrun_data_interval_start,
	next_dagrun_data_interval_end
FROM today_dag d
LEFT JOIN today_dagrun r
ON d.dag_id = r.dag_id;
