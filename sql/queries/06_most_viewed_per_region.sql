-- The single most-viewed trending video in each region.
SELECT
  region,
  MAX_BY(title, peak_views)         AS title,
  MAX_BY(channel_title, peak_views) AS channel_title,
  MAX(peak_views)                   AS peak_views
FROM yt_pipeline_gold_dev.video_trending_history
GROUP BY region
ORDER BY peak_views DESC;