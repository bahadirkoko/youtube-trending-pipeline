-- How do the 10 regions compare? One row per region, built from the video history.
SELECT
  region,
  COUNT(*)                            AS videos,
  ROUND(AVG(days_on_trending), 2)     AS avg_days_on_trending,
  MAX(days_on_trending)               AS longest_run_days,
  ROUND(AVG(avg_like_ratio) * 100, 2) AS avg_like_percent,
  MAX(peak_views)                     AS biggest_video_views
FROM yt_pipeline_gold_dev.video_trending_history
GROUP BY region
ORDER BY videos DESC;
