-- Data check: Silver and the two Gold tables must agree on the number of video-days.
-- For every region the three counts below should be identical.
WITH silver AS (
  SELECT region, COUNT(*) AS silver_rows
  FROM yt_pipeline_silver_dev.trending_videos
  GROUP BY region
),
daily AS (
  SELECT region, SUM(videos) AS gold_daily_rows
  FROM yt_pipeline_gold_dev.trending_analytics
  GROUP BY region
),
history AS (
  SELECT region, SUM(days_on_trending) AS gold_history_rows
  FROM yt_pipeline_gold_dev.video_trending_history
  GROUP BY region
)
SELECT
  silver.region,
  silver.silver_rows,
  daily.gold_daily_rows,
  history.gold_history_rows,
  silver.silver_rows = daily.gold_daily_rows
    AND silver.silver_rows = history.gold_history_rows AS all_match
FROM silver
JOIN daily ON daily.region = silver.region
JOIN history ON history.region = silver.region
ORDER BY silver.region;