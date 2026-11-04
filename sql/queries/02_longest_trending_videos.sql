-- Which US videos stayed on the trending list the longest?
SELECT
  title,
  channel_title,
  days_on_trending,
  first_trending_date,
  last_trending_date,
  peak_views
FROM yt_pipeline_gold_dev.video_trending_history
WHERE region = 'US'
ORDER BY days_on_trending DESC, peak_views DESC
LIMIT 10;