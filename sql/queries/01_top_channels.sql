-- Which US channels have the biggest trending footprint?
-- total_peak_views adds up each video's PEAK views (views are cumulative, so summing
-- daily rows would count the same views many times).
SELECT
  channel_title,
  videos_trended,
  total_trending_days,
  total_peak_views,
  max_peak_views,
  top_video_title
FROM yt_pipeline_gold_dev.channel_analytics
WHERE region = 'US'
ORDER BY total_peak_views DESC
LIMIT 10;