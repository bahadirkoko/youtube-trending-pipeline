locals {
  prefix = "${var.project_name}-${var.environment}"

  # Which buckets each service may touch (least privilege)
  lambda_buckets = [var.bucket_arns["bronze"], var.bucket_arns["silver"], var.bucket_arns["athena"]]
  glue_buckets   = values(var.bucket_arns)

  lambda_s3_resources = concat(local.lambda_buckets, [for a in local.lambda_buckets : "${a}/*"])
  glue_s3_resources   = concat(local.glue_buckets, [for a in local.glue_buckets : "${a}/*"])
}

# =============================== Lambda ===============================
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.prefix}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_access" {
  statement {
    sid       = "S3ReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = local.lambda_s3_resources
  }

  statement {
    sid       = "GlueCatalogAccess"
    actions   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions", "glue:CreateTable", "glue:UpdateTable", "glue:BatchCreatePartition"]
    resources = ["*"]
  }

  # AWS Wrangler can query through Athena. Tighten later if unused.
  statement {
    sid       = "AthenaQuery"
    actions   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults"]
    resources = ["*"]
  }

  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }
  # Secrets are named <project>/<env>/<name>, so one pattern covers all pipeline secrets.
  statement {
    sid       = "ReadPipelineSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:${var.project_name}/${var.environment}/*"]
  }
}


resource "aws_iam_role_policy" "lambda_access" {
  name   = "${local.prefix}-lambda-access"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_access.json
}

# ================================ Glue ================================
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_access" {
  statement {
    sid       = "S3ReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = local.glue_s3_resources
  }
}

resource "aws_iam_role_policy" "glue_access" {
  name   = "${local.prefix}-glue-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_access.json
}

# ============================ Step Functions ============================
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${local.prefix}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn_access" {
  statement {
    sid       = "InvokePipelineLambdas"
    actions   = ["lambda:InvokeFunction"]
    resources = ["arn:aws:lambda:${var.region}:${var.account_id}:function:${var.project_name}-*-${var.environment}"]
  }

  statement {
    sid       = "RunGlueJobs"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = ["arn:aws:glue:${var.region}:${var.account_id}:job/${var.project_name}-*-${var.environment}"]
  }

  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }
}

resource "aws_iam_role_policy" "sfn_access" {
  name   = "${local.prefix}-sfn-access"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn_access.json
}

# ============================== EventBridge ==============================
data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events" {
  name               = "${local.prefix}-events-role"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

data "aws_iam_policy_document" "events_access" {
  statement {
    actions   = ["states:StartExecution"]
    resources = ["arn:aws:states:${var.region}:${var.account_id}:stateMachine:${var.project_name}-*"]
  }
}

resource "aws_iam_role_policy" "events_access" {
  name   = "${local.prefix}-events-access"
  role   = aws_iam_role.events.id
  policy = data.aws_iam_policy_document.events_access.json
}
