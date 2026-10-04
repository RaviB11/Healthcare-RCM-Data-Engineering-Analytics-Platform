# ===========================================================================
# IAM roles and policies for Glue and Step Functions
# ===========================================================================

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# AWS Glue execution role
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data_access" {
  statement {
    sid = "LakeBucketMetadata"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [aws_s3_bucket.lake.arn]
  }

  statement {
    sid = "LakeObjectAccess"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    sid = "LakeKmsAccess"

    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey",
    ]

    resources = [aws_kms_key.lake.arn]
  }
}

resource "aws_iam_role_policy" "glue_data_access" {
  name   = "${local.name_prefix}-glue-data-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data_access.json
}

# ---------------------------------------------------------------------------
# Step Functions execution role
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "step_functions" {
  name               = "${local.name_prefix}-step-functions-role"
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json
}

data "aws_iam_policy_document" "step_functions" {
  statement {
    sid = "RunGlueJobs"

    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]

    resources = values(aws_glue_job.layer)[*].arn
  }

  statement {
    sid = "RunGlueCrawler"

    actions = [
      "glue:StartCrawler",
      "glue:GetCrawler",
    ]

    resources = [aws_glue_crawler.gold.arn]
  }

  statement {
    sid = "InvokePipelineLambdas"

    actions = [
      "lambda:InvokeFunction",
    ]

    resources = [
      "arn:aws:lambda:${var.region}:${data.aws_caller_identity.current.account_id}:function:rcm-*",
    ]
  }

  statement {
    sid = "PublishPipelineNotifications"

    actions = [
      "sns:Publish",
    ]

    resources = [
      "arn:aws:sns:${var.region}:${data.aws_caller_identity.current.account_id}:rcm-*",
    ]
  }

  statement {
    sid = "UseRedshiftDataApi"

    actions = [
      "redshift-data:BatchExecuteStatement",
      "redshift-data:DescribeStatement",
      "redshift-data:CancelStatement",
      "redshift-data:GetStatementResult",
    ]

    resources = ["*"]
  }

  statement {
    sid = "WriteExecutionLogs"

    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]

    resources = ["*"]
  }

  statement {
    sid = "WriteXRayTraces"

    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
      "xray:GetSamplingRules",
      "xray:GetSamplingTargets",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "step_functions" {
  name   = "${local.name_prefix}-step-functions-execution"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions.json
}
