locals {
  name = "${var.project_name}-orchestrator-${var.environment}"
}

# The "/aws/vendedlogs/states/" prefix keeps Step Functions log delivery from
# running into the CloudWatch Logs resource policy size limit.
resource "aws_cloudwatch_log_group" "state_machine" {
  name              = "/aws/vendedlogs/states/${local.name}"
  retention_in_days = var.log_retention_days
}

# The name must match <project>-*: the EventBridge role in the iam module only
# allows starting state machines with that pattern.
resource "aws_sfn_state_machine" "pipeline" {
  name     = local.name
  role_arn = var.sfn_role_arn
  type     = "STANDARD"

  definition = templatefile(var.definition_path, {
    ingestion_function_name       = var.ingestion_function_name
    json_to_parquet_function_name = var.json_to_parquet_function_name
    data_quality_function_name    = var.data_quality_function_name
    bronze_to_silver_job_name     = var.bronze_to_silver_job_name
    silver_to_gold_job_name       = var.silver_to_gold_job_name
    alerts_topic_arn              = var.alerts_topic_arn
    environment                   = var.environment
    region                        = var.region
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machine.arn}:*"
    include_execution_data = true
    level                  = var.log_level
  }
}

# Created DISABLED by default. Flip schedule_enabled to true once a manual run is green.
resource "aws_cloudwatch_event_rule" "schedule" {
  name                = "${local.name}-schedule"
  description         = "Starts the YouTube trending pipeline on a schedule"
  schedule_expression = var.schedule_expression
  state               = var.schedule_enabled ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "start_pipeline" {
  rule      = aws_cloudwatch_event_rule.schedule.name
  target_id = "start-pipeline"
  arn       = aws_sfn_state_machine.pipeline.arn
  role_arn  = var.events_role_arn
  input     = jsonencode({})
}