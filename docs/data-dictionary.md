# Data dictionary

Every table you can query in Athena, column by column. For how the layers fit together see
[`architecture.md`](architecture.md).

| Layer  | Athena database          | Tables                                                                                    |
| ------ | ------------------------ | ----------------------------------------------------------------------------------------- |
| Silver | `yt_pipeline_silver_dev` | `trending_videos`, `categories`                                                           |
| Gold   | `yt_pipeline_gold_dev`   | `video_trending_history`, `trending_analytics`, `channel_analytics`, `category_analytics` |

All tables are Snappy Parquet, partitioned by `region`, with partition projection. Filter on
`region` whenever you can, because Athena then only reads that region's folder.

**Conventions that apply everywhere**

- `region` is a two-letter code: `CA`, `DE`, `FR`, `GB`, `IN`, `JP`, `KR`, `MX`, `RU`, `US`. It
  comes from the S3 folder name (`region=US/`) and is not stored inside the Parquet files.
- All dates and timestamps are UTC.
- View, like and comment counts are **cumulative**. A video that trends for 5 days reports a
  bigger number every day. Never `SUM(views)` across days for the same video; use the peak
  (`MAX(views)`), as the Gold tables do.
- Ratios are fractions between 0 and 1, not percentages.

## Data sources

Silver mixes two sources, which you can tell apart by the `source` column:

|                     | `api` (live)                                                | `kaggle` (historical)                    |
| ------------------- | ----------------------------------------------------------- | ---------------------------------------- |
| Period              | October 2026 onward, daily                                  | November 2017 to June 2018               |
| Videos per region/day | about 50                                                  | about 200                                |
| `channel_id`        | present                                                     | always NULL (not in the files)           |
| `duration_seconds`  | present                                                     | always NULL                              |
| `dislikes`          | always NULL (YouTube hid dislikes in 2021)                  | present                                  |
| `comments_disabled`, `ratings_disabled` | inferred: `true` when the count is missing | taken from the file                 |
| `ingested_at`       | when the snapshot was taken                                 | always NULL                              |

Because there is an 8-year gap between the two, daily charts jump from 2018 to 2026.

## Silver

### `trending_videos`

Cleaned, typed and deduplicated trending videos from both sources.

- **Grain:** one row per `video_id`, `region` and `trending_date`. When a day has several API
  snapshots, the latest one wins (ties go to the higher view count).
- **Written by:** the `bronze_to_silver` Glue job ([`bronze_to_silver.py`](../src/glue_jobs/bronze_to_silver.py))
- **Location:** `s3://<silver>/youtube/trending_videos/region=XX/`
- Rows without a `video_id`, `trending_date` or `region` are dropped. Values that cannot be
  parsed (for example a non-numeric view count) become NULL instead of failing the job.

| Column              | Type            | Description                                                                                                                       |
| ------------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `video_id`          | `string`        | YouTube video id, for example `dQw4w9WgXcQ`.                                                                                      |
| `region`            | `string`        | Partition. Country whose trending list the video was on.                                                                          |
| `trending_date`     | `date`          | Day the video was trending. API: the UTC date of the snapshot. Kaggle: parsed from the `yy.dd.MM` format of the files.            |
| `title`             | `string`        | Video title on that day (titles can change while a video trends).                                                                 |
| `channel_id`        | `string`        | YouTube channel id. NULL for Kaggle rows.                                                                                         |
| `channel_title`     | `string`        | Channel display name. Used as the channel key in Gold because Kaggle has no `channel_id`.                                         |
| `category_id`       | `int`           | YouTube category id. Join to `categories` for the name.                                                                           |
| `publish_time`      | `timestamp`     | When the video was published.                                                                                                     |
| `tags`              | `array<string>` | Video tags. Empty array when the video has none.                                                                                  |
| `duration_seconds`  | `int`           | Video length in seconds, from the API's ISO-8601 duration (`PT4M13S` is 253). NULL for Kaggle rows.                               |
| `views`             | `bigint`        | Cumulative view count at snapshot time.                                                                                           |
| `likes`             | `bigint`        | Cumulative like count. NULL when ratings are disabled.                                                                            |
| `dislikes`          | `bigint`        | Cumulative dislike count. NULL for API rows.                                                                                      |
| `comment_count`     | `bigint`        | Cumulative comment count. NULL when comments are disabled.                                                                        |
| `comments_disabled` | `boolean`       | Comments are turned off for the video.                                                                                            |
| `ratings_disabled`  | `boolean`       | Likes are hidden or turned off for the video.                                                                                     |
| `like_ratio`        | `double`        | `likes / views`. NULL when `views` is 0 or NULL, or when `likes` is NULL.                                                         |
| `engagement_rate`   | `double`        | `(likes + comment_count) / views`, with missing comments counted as 0. NULL when `views` is 0 or NULL, or when `likes` is NULL.   |
| `source`            | `string`        | `api` or `kaggle`. See [Data sources](#data-sources).                                                                             |
| `ingested_at`       | `timestamp`     | Date and hour of the API snapshot (from the Bronze `date=`/`hour=` folders). NULL for Kaggle rows.                                |

### `categories`

Lookup from category id to name, per region.

- **Grain:** one row per `category_id` and `region`.
- **Written by:** the `json_to_parquet` Lambda ([`handler.py`](../src/lambdas/json_to_parquet/handler.py)).
  It merges the Kaggle and API category files, and the API wins when both have the same id.
- **Location:** `s3://<silver>/youtube/categories/region=XX/categories.parquet`

| Column          | Type      | Description                                                         |
| --------------- | --------- | ------------------------------------------------------------------- |
| `category_id`   | `int`     | YouTube category id.                                                |
| `category_name` | `string`  | Display name, for example `Music` or `Gaming`.                      |
| `assignable`    | `boolean` | Whether uploaders can pick this category for new videos.            |
| `region`        | `string`  | Partition. Category names and availability can differ per region.  |

## Gold

All four tables are rebuilt in full from Silver on every run by the `silver_to_gold` Glue job
([`silver_to_gold.py`](../src/glue_jobs/silver_to_gold.py)), at
`s3://<gold>/youtube/<table>/region=XX/`. Each has `region` as its partition column, which is
not repeated in the tables below.

Averages of ratios (`avg_like_ratio`, `avg_engagement_rate`) are plain averages of the
per-row values, ignoring NULLs. They are not weighted by views.

### `video_trending_history`

How long and how high each video trended.

- **Grain:** one row per `video_id` and `region`.

| Column                | Type     | Description                                                                                         |
| --------------------- | -------- | --------------------------------------------------------------------------------------------------- |
| `video_id`            | `string` | YouTube video id.                                                                                   |
| `title`               | `string` | Title on the video's last trending day.                                                             |
| `channel_title`       | `string` | Channel name on the last trending day.                                                              |
| `category_id`         | `int`    | Category on the last trending day.                                                                  |
| `first_trending_date` | `date`   | First day on the trending list.                                                                     |
| `last_trending_date`  | `date`   | Last day on the trending list.                                                                      |
| `days_on_trending`    | `int`    | Number of days the video actually appeared on the list.                                             |
| `calendar_span_days`  | `int`    | Days from first to last trending date, inclusive. Larger than `days_on_trending` if it dropped off and came back. |
| `peak_views`          | `bigint` | Highest view count seen. Use this, not a sum, for totals.                                           |
| `peak_views_date`     | `date`   | Day `peak_views` was recorded.                                                                      |
| `first_views`         | `bigint` | Views on the first trending day.                                                                    |
| `last_views`          | `bigint` | Views on the last trending day.                                                                     |
| `avg_like_ratio`      | `double` | Average daily `like_ratio`.                                                                         |
| `avg_engagement_rate` | `double` | Average daily `engagement_rate`.                                                                    |

### `trending_analytics`

What each region's trending list looked like on each day.

- **Grain:** one row per `trending_date` and `region`.

| Column                | Type     | Description                                                                          |
| --------------------- | -------- | ------------------------------------------------------------------------------------ |
| `trending_date`       | `date`   | The day.                                                                             |
| `videos`              | `int`    | Videos on the list that day.                                                         |
| `distinct_channels`   | `int`    | Different channels (by `channel_title`) on the list.                                 |
| `distinct_categories` | `int`    | Different categories on the list.                                                    |
| `total_views`         | `bigint` | Sum of that day's view counts. Safe within one day; do not sum it across days.       |
| `avg_views`           | `double` | Average view count of that day's videos.                                             |
| `max_views`           | `bigint` | Highest view count that day.                                                         |
| `avg_like_ratio`      | `double` | Average `like_ratio` that day.                                                       |
| `avg_engagement_rate` | `double` | Average `engagement_rate` that day.                                                  |
| `top_video_id`        | `string` | Video with the most views that day.                                                  |
| `top_video_title`     | `string` | Its title.                                                                           |

### `channel_analytics`

Which channels trend most. Rows with an empty `channel_title` are left out.

- **Grain:** one row per `channel_title` and `region`.
- **Built from:** `video_trending_history`, so every video counts once.

| Column                | Type     | Description                                                       |
| --------------------- | -------- | ----------------------------------------------------------------- |
| `channel_title`       | `string` | Channel name (the channel key, see `trending_videos`).            |
| `videos_trended`      | `int`    | Distinct videos from this channel that trended.                   |
| `total_trending_days` | `int`    | Sum of `days_on_trending` over the channel's videos.              |
| `avg_days_per_video`  | `double` | Average `days_on_trending` per video.                             |
| `total_peak_views`    | `bigint` | Sum of each video's `peak_views`.                                 |
| `avg_peak_views`      | `double` | Average `peak_views` per video.                                   |
| `max_peak_views`      | `bigint` | `peak_views` of the channel's biggest video.                      |
| `avg_like_ratio`      | `double` | Average of the videos' `avg_like_ratio`.                          |
| `first_trending_date` | `date`   | Earliest day any of the channel's videos trended.                 |
| `last_trending_date`  | `date`   | Latest day any of the channel's videos trended.                   |
| `top_video_title`     | `string` | Title of the video with the highest `peak_views`.                 |

### `category_analytics`

Which categories trend most.

- **Grain:** one row per `category_id` and `region`.
- **Built from:** `video_trending_history` joined to Silver `categories`.

| Column                   | Type     | Description                                                                                       |
| ------------------------ | -------- | ------------------------------------------------------------------------------------------------- |
| `category_id`            | `int`    | YouTube category id.                                                                              |
| `category_name`          | `string` | Name from `categories`, or `Unknown (<id>)` when the region's lookup has no entry for the id.     |
| `videos_trended`         | `int`    | Distinct videos in this category that trended.                                                    |
| `total_trending_days`    | `int`    | Sum of `days_on_trending` over the category's videos.                                             |
| `share_of_trending_days` | `double` | This category's `total_trending_days` divided by the region's total. Adds up to 1 per region.     |
| `avg_days_per_video`     | `double` | Average `days_on_trending` per video.                                                             |
| `total_peak_views`       | `bigint` | Sum of each video's `peak_views`.                                                                 |
| `avg_peak_views`         | `double` | Average `peak_views` per video.                                                                   |
| `avg_like_ratio`         | `double` | Average of the videos' `avg_like_ratio`.                                                          |
| `avg_engagement_rate`    | `double` | Average of the videos' `avg_engagement_rate`.                                                     |
| `distinct_channels`      | `int`    | Different channels (by `channel_title`) with a trending video in this category.                   |

## Keeping this file current

Column lists are defined in several places on purpose, and
[`test_schema_contracts.py`](../tests/unit/test_schema_contracts.py) checks that they match. This
file is **not** checked by that test, so update it by hand whenever a column is added, removed or
changes meaning. See the schema change checklist in [`development.md`](development.md).
