-- The most recent 14 trending days in the US: volume and the top video of each day.
SELECT
  trending_date,
  videos,
  distinct_channels,
  total_views,
  top_video_title
FROM yt_pipeline_gold_dev.trending_analytics
WHERE region = 'US'
ORDER BY trending_date DESC
LIMIT 14;
