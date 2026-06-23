-- =========================================
-- OVERALL QUALITY SCORE
-- =========================================

SELECT
ROUND(AVG(quality_score), 2) AS overall_quality_score
FROM quality_metrics;

-- =========================================
-- FAILED ROWS BY DATASET
-- =========================================

SELECT
dataset_name,
SUM(failed_rows) AS total_failed_rows
FROM quality_metrics
GROUP BY dataset_name
ORDER BY total_failed_rows DESC;

-- =========================================
-- HIGH SEVERITY FAILURES
-- =========================================

SELECT *
FROM quality_metrics
WHERE severity = 'HIGH';

-- =========================================
-- QUALITY SCORE BY CHECK TYPE
-- =========================================

SELECT
check_type,
ROUND(AVG(quality_score),2) avg_score
FROM quality_metrics
GROUP BY check_type
ORDER BY avg_score;

-- =========================================
-- DAILY QUALITY TREND
-- =========================================

SELECT
DATE(run_timestamp) AS run_date,
ROUND(AVG(quality_score),2) quality_score
FROM quality_metrics
GROUP BY DATE(run_timestamp)
ORDER BY run_date;
