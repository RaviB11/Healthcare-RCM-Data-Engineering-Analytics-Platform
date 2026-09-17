# ===========================================================================
# Healthcare RCM platform - core infrastructure
#
# Scope: the data lake, catalog, Glue jobs and orchestration. Redshift
# Serverless lives in warehouse.tf, alarms in monitoring.tf.
#
#   terraform init && terraform plan -var-file=envs/dev.tfvars
# ===========================================================================

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket         = "rcm-terraform-state"
    key            = "platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "rcm-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project     = "healthcare-rcm"
      Environment = var.environment
      ManagedBy   = "terraform"
      DataClass   = "PHI"        # drives access review and retention policy
    }
  }
}

locals {
  name_prefix = "rcm-${var.environment}"
  lake_bucket = "${local.name_prefix}-datalake-${var.region}"
}

# ---------------------------------------------------------------------------
# Encryption. A healthcare lake gets a customer-managed key, not the AWS
# default one, so key access is auditable and revocable independently of S3.
# ---------------------------------------------------------------------------
resource "aws_kms_key" "lake" {
  description             = "CMK for the RCM data lake (contains PHI)"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "lake" {
  name          = "alias/${local.name_prefix}-lake"
  target_key_id = aws_kms_key.lake.key_id
}

# ---------------------------------------------------------------------------
# Data lake. One bucket, prefixes per medallion layer, so lifecycle and
# access policy can differ by layer without cross-bucket IAM sprawl.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.lake.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true    # cuts KMS request cost on high-object-count prefixes
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # Landing is a replay buffer, not an archive.
  rule {
    id     = "landing-expire"
    status = "Enabled"
    filter { prefix = "landing/" }
    expiration { days = 90 }
  }

  # Bronze is the replay source of truth: keep it, but stop paying hot prices.
  rule {
    id     = "bronze-tiering"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 90
      storage_class = "INTELLIGENT_TIERING"
    }
    transition {
      days          = 365
      storage_class = "GLACIER_IR"
    }
  }

  # Quarantined rows are worked within a quarter or they are never worked.
  rule {
    id     = "quarantine-expire"
    status = "Enabled"
    filter { prefix = "quarantine/" }
    expiration { days = 180 }
  }
}

# ---------------------------------------------------------------------------
# Glue Data Catalog
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "rcm" {
  name        = "rcm_${var.environment}"
  description = "Healthcare RCM medallion catalog"
}

resource "aws_glue_crawler" "gold" {
  name          = "${local.name_prefix}-gold-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.rcm.name

  s3_target { path = "s3://${aws_s3_bucket.lake.bucket}/gold/" }

  # Partitions change every run; schemas should not. Log unexpected schema
  # drift rather than silently rewriting the table under the BI layer.
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  configuration = jsonencode({
    Version  = 1.0
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })
}

# ---------------------------------------------------------------------------
# Glue jobs - one per medallion layer, same entrypoint script
# ---------------------------------------------------------------------------
resource "aws_s3_object" "job_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/glue_job_entrypoint.py"
  source = "${path.module}/../glue/glue_job_entrypoint.py"
  etag   = filemd5("${path.module}/../glue/glue_job_entrypoint.py")
}

resource "aws_glue_job" "layer" {
  for_each = var.glue_jobs

  name              = "${local.name_prefix}-${each.key}"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0" # Spark 3.5 / Python 3.11
  worker_type       = each.value.worker_type
  number_of_workers = each.value.workers
  timeout           = each.value.timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.job_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--LAYER"                            = each.key
    "--RCM_ENV"                          = var.environment
    "--CONFIG_S3"                        = "s3://${aws_s3_bucket.lake.bucket}/config/pipeline_config.yaml"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.lake.bucket}/scripts/rcm_src.zip"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--job-language"                     = "python"
    # Glue's own bookmarks are disabled: this pipeline manages incrementality
    # through partition overwrite and batch ids, and having two mechanisms
    # disagree about what has been processed is a bad place to be.
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property {
    max_concurrent_runs = 1 # a second concurrent run would corrupt SCD2 history
  }
}

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name_prefix}-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/../stepfunctions/rcm_pipeline.asl.json", {})

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = false # execution data would carry PHI into logs
    level                  = "ERROR"
  }

  tracing_configuration { enabled = true }
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${local.name_prefix}-pipeline"
  retention_in_days = 90
  kms_key_id        = aws_kms_key.lake.arn
}

resource "aws_dynamodb_table" "arrivals" {
  name         = "${local.name_prefix}-source-arrivals"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "business_date"
  range_key    = "source_name"

  attribute {
    name = "business_date"
    type = "S"
  }
  attribute {
    name = "source_name"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}
