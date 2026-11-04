CREATE EXTERNAL TABLE IF NOT EXISTS yt_pipeline_gold_dev.trending_analytics (
  trending_date       date,
  videos              int,
  distinct_channels   int,
  distinct_categories int,
  total_views         bigint,
  avg_views           double,
  max_views           bigint,
  avg_like_ratio      double,
  avg_engagement_rate double,
  top_video_id        string,
  top_video_title     string
)
PARTITIONED BY (region string)
STORED AS PARQUET
LOCATION 's3://yt-data-pipeline-gold-743116070631-us-east-1-dev/youtube/trending_analytics/'
TBLPROPERTIES (
  'parquet.compression'         = 'SNAPPY',
  'projection.enabled'          = 'true',
  'projection.region.type'      = 'enum',
  'projection.region.values'    = 'CA,DE,FR,GB,IN,JP,KR,MX,RU,US',
  'storage.location.template'   = 's3://yt-data-pipeline-gold-743116070631-us-east-1-dev/youtube/trending_analytics/region=${region}/'
);