CREATE EXTERNAL TABLE IF NOT EXISTS yt_pipeline_gold_dev.video_trending_history (
  video_id            string,
  title               string,
  channel_title       string,
  category_id         int,
  first_trending_date date,
  last_trending_date  date,
  days_on_trending    int,
  calendar_span_days  int,
  peak_views          bigint,
  peak_views_date     date,
  first_views         bigint,
  last_views          bigint,
  avg_like_ratio      double,
  avg_engagement_rate double
)
PARTITIONED BY (region string)
STORED AS PARQUET
LOCATION 's3://yt-data-pipeline-gold-743116070631-us-east-1-dev/youtube/video_trending_history/'
TBLPROPERTIES (
  'parquet.compression'         = 'SNAPPY',
  'projection.enabled'          = 'true',
  'projection.region.type'      = 'enum',
  'projection.region.values'    = 'CA,DE,FR,GB,IN,JP,KR,MX,RU,US',
  'storage.location.template'   = 's3://yt-data-pipeline-gold-743116070631-us-east-1-dev/youtube/video_trending_history/region=${region}/'
);