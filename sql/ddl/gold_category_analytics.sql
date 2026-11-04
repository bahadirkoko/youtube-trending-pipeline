CREATE EXTERNAL TABLE IF NOT EXISTS yt_pipeline_gold_dev.category_analytics (
  category_id            int,
  category_name          string,
  videos_trended         int,
  total_trending_days    int,
  share_of_trending_days double,
  avg_days_per_video     double,
  total_peak_views       bigint,
  avg_peak_views         double,
  avg_like_ratio         double,
  avg_engagement_rate    double,
  distinct_channels      int
)
PARTITIONED BY (region string)
STORED AS PARQUET
LOCATION 's3://yt-data-pipeline-gold-743116070631-us-east-1-dev/youtube/category_analytics/'
TBLPROPERTIES (
  'parquet.compression'         = 'SNAPPY',
  'projection.enabled'          = 'true',
  'projection.region.type'      = 'enum',
  'projection.region.values'    = 'CA,DE,FR,GB,IN,JP,KR,MX,RU,US',
  'storage.location.template'   = 's3://yt-data-pipeline-gold-743116070631-us-east-1-dev/youtube/category_analytics/region=${region}/'
);
