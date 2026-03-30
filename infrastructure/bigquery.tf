# BigQuery Dataset
resource "google_bigquery_dataset" "kyivnotkiev" {
  dataset_id    = "kyivnotkiev"
  friendly_name = "KyivNotKiev Research Dataset"
  description   = "Multi-source toponymic adoption analysis"
  location      = "US"
  project       = google_project.research.project_id

  labels = {
    project = "kyivnotkiev"
    env     = "research"
  }

  depends_on = [google_project_service.apis]
}

# ── Raw Data Tables ─────────────────────────────────────

resource "google_bigquery_table" "raw_gdelt" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "raw_gdelt"
  project    = google_project.research.project_id

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["pair_id", "variant"]

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "date", type = "DATE", mode = "REQUIRED" },
    { name = "source_url", type = "STRING", mode = "NULLABLE" },
    { name = "source_domain", type = "STRING", mode = "NULLABLE" },
    { name = "source_country", type = "STRING", mode = "NULLABLE" },
    { name = "matched_term", type = "STRING", mode = "REQUIRED" },
    { name = "variant", type = "STRING", mode = "REQUIRED" },
    { name = "count", type = "INT64", mode = "REQUIRED" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "raw_common_crawl" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "raw_common_crawl"
  project    = google_project.research.project_id

  time_partitioning {
    type  = "DAY"
    field = "crawl_date"
  }

  clustering = ["pair_id", "variant", "domain"]

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "url", type = "STRING", mode = "REQUIRED" },
    { name = "domain", type = "STRING", mode = "REQUIRED" },
    { name = "tld", type = "STRING", mode = "NULLABLE" },
    { name = "matched_term", type = "STRING", mode = "REQUIRED" },
    { name = "variant", type = "STRING", mode = "REQUIRED" },
    { name = "context_snippet", type = "STRING", mode = "NULLABLE" },
    { name = "crawl_id", type = "STRING", mode = "REQUIRED" },
    { name = "crawl_date", type = "DATE", mode = "REQUIRED" },
    { name = "content_language", type = "STRING", mode = "NULLABLE" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "raw_reddit" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "raw_reddit"
  project    = google_project.research.project_id

  time_partitioning {
    type  = "MONTH"
    field = "created_utc"
  }

  clustering = ["pair_id", "variant", "subreddit"]

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "subreddit", type = "STRING", mode = "REQUIRED" },
    { name = "post_id", type = "STRING", mode = "REQUIRED" },
    { name = "comment_id", type = "STRING", mode = "NULLABLE" },
    { name = "author", type = "STRING", mode = "NULLABLE" },
    { name = "title", type = "STRING", mode = "NULLABLE" },
    { name = "body", type = "STRING", mode = "NULLABLE" },
    { name = "score", type = "INT64", mode = "NULLABLE" },
    { name = "matched_term", type = "STRING", mode = "REQUIRED" },
    { name = "variant", type = "STRING", mode = "REQUIRED" },
    { name = "created_utc", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "raw_wikipedia" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "raw_wikipedia"
  project    = google_project.research.project_id

  time_partitioning {
    type  = "MONTH"
    field = "date"
  }

  clustering = ["pair_id", "variant"]

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "page_title", type = "STRING", mode = "REQUIRED" },
    { name = "variant", type = "STRING", mode = "REQUIRED" },
    { name = "date", type = "DATE", mode = "REQUIRED" },
    { name = "pageviews", type = "INT64", mode = "REQUIRED" },
    { name = "edits", type = "INT64", mode = "NULLABLE" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "raw_trends" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "raw_trends"
  project    = google_project.research.project_id

  clustering = ["pair_id", "variant"]

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "date", type = "DATE", mode = "REQUIRED" },
    { name = "term", type = "STRING", mode = "REQUIRED" },
    { name = "variant", type = "STRING", mode = "REQUIRED" },
    { name = "interest", type = "INT64", mode = "REQUIRED" },
    { name = "geo", type = "STRING", mode = "NULLABLE" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "raw_ngrams" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "raw_ngrams"
  project    = google_project.research.project_id

  clustering = ["pair_id", "variant"]

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "year", type = "INT64", mode = "REQUIRED" },
    { name = "term", type = "STRING", mode = "REQUIRED" },
    { name = "variant", type = "STRING", mode = "REQUIRED" },
    { name = "frequency", type = "FLOAT64", mode = "REQUIRED" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "raw_youtube" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "raw_youtube"
  project    = google_project.research.project_id

  time_partitioning {
    type  = "MONTH"
    field = "published_at"
  }

  clustering = ["pair_id", "variant"]

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "video_id", type = "STRING", mode = "REQUIRED" },
    { name = "channel_id", type = "STRING", mode = "NULLABLE" },
    { name = "channel_title", type = "STRING", mode = "NULLABLE" },
    { name = "title", type = "STRING", mode = "REQUIRED" },
    { name = "description", type = "STRING", mode = "NULLABLE" },
    { name = "matched_term", type = "STRING", mode = "REQUIRED" },
    { name = "variant", type = "STRING", mode = "REQUIRED" },
    { name = "view_count", type = "INT64", mode = "NULLABLE" },
    { name = "published_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# ── Pipeline State Tables ───────────────────────────────

resource "google_bigquery_table" "watermarks" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "watermarks"
  project    = google_project.research.project_id

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "source", type = "STRING", mode = "REQUIRED" },
    { name = "last_fetched", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "row_count", type = "INT64", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED" },
  ])
}

# ── Analysis Output Tables ──────────────────────────────

resource "google_bigquery_table" "analysis_adoption" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "analysis_adoption"
  project    = google_project.research.project_id

  clustering = ["pair_id", "source"]

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "source", type = "STRING", mode = "REQUIRED" },
    { name = "period", type = "STRING", mode = "REQUIRED" },
    { name = "period_start", type = "DATE", mode = "REQUIRED" },
    { name = "period_end", type = "DATE", mode = "REQUIRED" },
    { name = "russian_count", type = "INT64", mode = "REQUIRED" },
    { name = "ukrainian_count", type = "INT64", mode = "REQUIRED" },
    { name = "total_count", type = "INT64", mode = "REQUIRED" },
    { name = "adoption_ratio", type = "FLOAT64", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "analysis_changepoints" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "analysis_changepoints"
  project    = google_project.research.project_id

  schema = jsonencode([
    { name = "pair_id", type = "INT64", mode = "REQUIRED" },
    { name = "source", type = "STRING", mode = "REQUIRED" },
    { name = "changepoint_date", type = "DATE", mode = "REQUIRED" },
    { name = "ci_lower", type = "DATE", mode = "REQUIRED" },
    { name = "ci_upper", type = "DATE", mode = "REQUIRED" },
    { name = "method", type = "STRING", mode = "REQUIRED" },
    { name = "effect_size", type = "FLOAT64", mode = "NULLABLE" },
  ])
}

# ── Views ───────────────────────────────────────────────

resource "google_bigquery_table" "v_cross_source" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "v_cross_source"
  project    = google_project.research.project_id

  view {
    query          = <<-SQL
      SELECT
        a.pair_id,
        a.source,
        a.period,
        a.adoption_ratio,
        a.total_count,
        a.russian_count,
        a.ukrainian_count
      FROM `${var.project_id}.kyivnotkiev.analysis_adoption` a
      WHERE a.period = 'latest'
      ORDER BY a.pair_id, a.source
    SQL
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "v_latest_adoption" {
  dataset_id = google_bigquery_dataset.kyivnotkiev.dataset_id
  table_id   = "v_latest_adoption"
  project    = google_project.research.project_id

  view {
    query          = <<-SQL
      WITH ranked AS (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY pair_id, source ORDER BY period_end DESC) as rn
        FROM `${var.project_id}.kyivnotkiev.analysis_adoption`
      )
      SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1
    SQL
    use_legacy_sql = false
  }
}
