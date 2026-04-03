-- Setup Athena database and table for querying Common Crawl's columnar index.
-- Run this once in the Athena console after creating the AWS account.
--
-- The CC index is stored as Parquet on S3 at:
--   s3://commoncrawl/cc-index/table/cc-main/warc/
-- It's partitioned by crawl (e.g., crawl=CC-MAIN-2024-10) and subset (warc/robotstxt/etc).
--
-- This table definition lets us query it directly — no data copy needed.

CREATE DATABASE IF NOT EXISTS ccindex;

CREATE EXTERNAL TABLE IF NOT EXISTS ccindex.ccindex (
  url_surtkey                   STRING,
  url                           STRING,
  url_host_name                 STRING,
  url_host_tld                  STRING,
  url_host_2nd_last_part        STRING,
  url_host_3rd_last_part        STRING,
  url_host_4th_last_part        STRING,
  url_host_5th_last_part        STRING,
  url_host_registry_suffix      STRING,
  url_host_registered_domain    STRING,
  url_host_private_suffix       STRING,
  url_host_private_domain       STRING,
  url_protocol                  STRING,
  url_port                      INT,
  url_path                      STRING,
  url_query                     STRING,
  fetch_time                    TIMESTAMP,
  fetch_status                  SMALLINT,
  content_digest                STRING,
  content_mime_type              STRING,
  content_mime_detected          STRING,
  content_charset               STRING,
  content_languages             STRING,
  warc_filename                 STRING,
  warc_record_offset            INT,
  warc_record_length            INT,
  warc_segment                  STRING
)
PARTITIONED BY (
  crawl STRING,
  subset STRING
)
STORED AS PARQUET
LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';

-- Load partitions (this discovers all available crawls)
MSCK REPAIR TABLE ccindex.ccindex;
