terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# Storage: raw landing bucket + transformed/output bucket.
# Encrypted, versioned, and blocked from any public access -- this pipeline
# handles per-user shopping activity, so both buckets are treated as holding
# sensitive data by default rather than only when someone remembers to ask.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "transformed" {
  bucket = var.transformed_bucket_name
}

resource "aws_s3_bucket_versioning" "transformed" {
  bucket = aws_s3_bucket.transformed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "transformed" {
  bucket = aws_s3_bucket.transformed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "transformed" {
  bucket                  = aws_s3_bucket.transformed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "transformed" {
  bucket = aws_s3_bucket.transformed.id
  rule {
    id     = "expire-old-object-versions"
    status = "Enabled"
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# ---------------------------------------------------------------------------
# Glue: catalog database, IAM role, crawler, and the transform job itself.
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "project" {
  name = replace(var.project_name, "-", "_")
}

resource "aws_iam_role" "glue" {
  name = "${var.project_name}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Scoped to exactly the two buckets this pipeline touches, rather than
# reaching for AmazonS3FullAccess -- read-only on raw, read/write on
# transformed, since the job never needs to modify its own source data.
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = [aws_s3_bucket.transformed.arn, "${aws_s3_bucket.transformed.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_glue_crawler" "raw" {
  name          = "${var.project_name}-raw-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.project.name

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/"
  }

  # Daily re-crawl in case new export files land; remove this block for a
  # strictly one-off historical load.
  schedule = "cron(0 3 * * ? *)"

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_job" "transform" {
  name              = "${var.project_name}-transform"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 60 # minutes -- fail fast rather than let a stuck job burn DPU-hours all day
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = var.glue_script_s3_uri
    python_version  = "3"
  }

  default_arguments = {
    "--SOURCE_DATABASE"                 = aws_glue_catalog_database.project.name
    "--SOURCE_TABLE"                    = "raw" # confirm against the actual crawler-generated table name after the first crawl run
    "--TARGET_DATABASE"                 = aws_glue_catalog_database.project.name
    "--TARGET_TABLE"                    = "transformed"
    "--TARGET_S3_PATH"                  = "s3://${aws_s3_bucket.transformed.bucket}/cleaned/"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--extra-py-files"                  = "${dirname(var.glue_script_s3_uri)}/transformations.py"
  }
}

# ---------------------------------------------------------------------------
# Monitoring: EventBridge catches a FAILED/TIMEOUT Glue job state and
# publishes to SNS, so a broken pipeline surfaces as an email rather than
# silence -- the original project relied on manually watching CloudWatch.
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "glue_alerts" {
  name = "${var.project_name}-glue-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.glue_alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_notification_email
}

resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn = aws_sns_topic.glue_alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "SNS:Publish"
      Resource  = aws_sns_topic.glue_alerts.arn
    }]
  })
}

resource "aws_cloudwatch_event_rule" "glue_job_failed" {
  name        = "${var.project_name}-glue-job-failed"
  description = "Fires when the transform Glue job fails or times out"
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.transform.name]
      state   = ["FAILED", "TIMEOUT"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_job_failed_to_sns" {
  rule      = aws_cloudwatch_event_rule.glue_job_failed.name
  target_id = "notify-sns"
  arn       = aws_sns_topic.glue_alerts.arn
}

# ---------------------------------------------------------------------------
# Athena: a dedicated workgroup with a per-query scan cap, so a WHERE-less
# query against the 15GB dataset can't silently run up the Athena bill.
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "project" {
  name = "${var.project_name}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 5368709120 # 5 GB

    result_configuration {
      output_location = "s3://${aws_s3_bucket.transformed.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
