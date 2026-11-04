-- What kind of content dominates US trending? (share of all video-days)
SELECT
  category_name,
  videos_trended,
  total_trending_days,
  ROUND(share_of_trending_days * 100, 1) AS share_percent,
  ROUND(avg_days_per_video, 1)           AS avg_days_per_video,
  ROUND(avg_like_ratio * 100, 2)         AS avg_like_percent
FROM yt_pipeline_gold_dev.category_analytics
WHERE region = 'US'
ORDER BY share_of_trending_days DESC
LIMIT 10;