CREATE EXTERNAL TABLE IF NOT EXISTS yt_pipeline_gold_dev.channel_analytics (
  channel_title       string,
  videos_trended      int,
  total_trending_days int,
  avg_days_per_video  double,
  total_peak_views    bigint,
  avg_peak_views      double,
  max_peak_views      bigint,
  avg_like_ratio      double,
  first_trending_date date,
  last_trending_date  date,
  top_video_title     string
)
PARTITIONED BY (region string)
STORED AS PARQUET
LOCATION 's3://yt-data-pipeline-gold-743116070631-us-east-1-dev/youtube/channel_analytics/'
TBLPROPERTIES (
  'parquet.compression'         = 'SNAPPY',
  'projection.enabled'          = 'true',
  'projection.region.type'      = 'enum',
  'projection.region.values'    = 'CA,DE,FR,GB,IN,JP,KR,MX,RU,US',
  'storage.location.template'   = 's3://yt-data-pipeline-gold-743116070631-us-east-1-dev/youtube/channel_analytics/region=${region}/'
);