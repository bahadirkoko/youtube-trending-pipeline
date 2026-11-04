locals {
  prefix = "${var.project_name}-${var.environment}"
}

# ----------------------------------------------------------------------------
# A pipeline execution failed, timed out or was aborted.
# Backstop for the emails the state machine sends itself: it also catches the
# cases where that notification could not be sent.
# ----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "pipeline_failed" {
  alarm_name          = "${local.prefix}-pipeline-failed"
  alarm_description   = "A pipeline execution failed, timed out or was aborted. See docs/runbook.md."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]

  metric_query {
    id          = "failed"
    return_data = false

    metric {
      namespace   = "AWS/States"
      metric_name = "ExecutionsFailed"
      period      = 300
      stat        = "Sum"
      dimensions  = { StateMachineArn = var.state_machine_arn }
    }
  }

  metric_query {
    id          = "timed_out"
    return_data = false

    metric {
      namespace   = "AWS/States"
      metric_name = "ExecutionsTimedOut"
      period      = 300
      stat        = "Sum"
      dimensions  = { StateMachineArn = var.state_machine_arn }
    }
  }

  metric_query {
    id          = "aborted"
    return_data = false

    metric {
      namespace   = "AWS/States"
      metric_name = "ExecutionsAborted"
      period      = 300
      stat        = "Sum"
      dimensions  = { StateMachineArn = var.state_machine_arn }
    }
  }

  metric_query {
    id          = "problems"
    expression  = "FILL(failed, 0) + FILL(timed_out, 0) + FILL(aborted, 0)"
    label       = "Failed, timed out or aborted executions"
    return_data = true
  }
}

# ----------------------------------------------------------------------------
# A Lambda function raised an error (one alarm per function).
# ----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = var.lambda_function_names

  alarm_name          = "${local.prefix}-${each.key}-lambda-errors"
  alarm_description   = "Lambda function ${each.value} reported errors. See docs/runbook.md."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = each.value }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]
}

# ----------------------------------------------------------------------------
# The schedule stopped starting the pipeline: no execution started for N days.
# Missing data counts as "breaching" here, because no data IS the problem.
# Only created while the schedule is enabled.
# ----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "pipeline_not_running" {
  count = var.schedule_enabled ? 1 : 0

  alarm_name          = "${local.prefix}-pipeline-not-running"
  alarm_description   = "No pipeline execution started for ${var.missed_days_before_alarm} days. See docs/runbook.md."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsStarted"
  dimensions          = { StateMachineArn = var.state_machine_arn }
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = var.missed_days_before_alarm
  datapoints_to_alarm = var.missed_days_before_alarm
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [var.alerts_topic_arn]
}

# ----------------------------------------------------------------------------
# Dashboard
# ----------------------------------------------------------------------------
locals {
  alarm_arns = concat(
    [aws_cloudwatch_metric_alarm.pipeline_failed.arn],
    [for a in aws_cloudwatch_metric_alarm.lambda_errors : a.arn],
    aws_cloudwatch_metric_alarm.pipeline_not_running[*].arn,
  )

  lambda_error_metrics = [
    for label, name in var.lambda_function_names :
    ["AWS/Lambda", "Errors", "FunctionName", name, { stat = "Sum", label = label }]
  ]
  lambda_invocation_metrics = [
    for label, name in var.lambda_function_names :
    ["AWS/Lambda", "Invocations", "FunctionName", name, { stat = "Sum", label = label }]
  ]
  lambda_duration_metrics = [
    for label, name in var.lambda_function_names :
    ["AWS/Lambda", "Duration", "FunctionName", name, { stat = "Maximum", label = label }]
  ]
}

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${local.prefix}-pipeline"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "alarm"
        x      = 0
        y      = 0
        width  = 24
        height = 4
        properties = {
          title  = "Alarms"
          alarms = local.alarm_arns
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 4
        width  = 12
        height = 6
        properties = {
          title  = "Pipeline executions per day"
          region = var.region
          view   = "bar"
          period = 86400
          stat   = "Sum"
          metrics = [
            ["AWS/States", "ExecutionsStarted", "StateMachineArn", var.state_machine_arn, { label = "Started" }],
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", var.state_machine_arn, { label = "Succeeded" }],
            ["AWS/States", "ExecutionsFailed", "StateMachineArn", var.state_machine_arn, { label = "Failed" }],
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 4
        width  = 12
        height = 6
        properties = {
          title  = "Execution time (milliseconds)"
          region = var.region
          view   = "timeSeries"
          period = 86400
          stat   = "Average"
          metrics = [
            ["AWS/States", "ExecutionTime", "StateMachineArn", var.state_machine_arn, { label = "Average" }],
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 10
        width  = 12
        height = 6
        properties = {
          title   = "Lambda errors"
          region  = var.region
          view    = "timeSeries"
          period  = 3600
          stat    = "Sum"
          metrics = local.lambda_error_metrics
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 10
        width  = 12
        height = 6
        properties = {
          title   = "Lambda invocations"
          region  = var.region
          view    = "timeSeries"
          period  = 3600
          stat    = "Sum"
          metrics = local.lambda_invocation_metrics
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 16
        width  = 24
        height = 6
        properties = {
          title   = "Lambda duration, slowest run (milliseconds)"
          region  = var.region
          view    = "timeSeries"
          period  = 3600
          stat    = "Maximum"
          metrics = local.lambda_duration_metrics
        }
      },
    ]
  })
}