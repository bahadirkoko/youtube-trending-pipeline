output "dashboard_name" {
  value = aws_cloudwatch_dashboard.pipeline.dashboard_name
}

output "dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${aws_cloudwatch_dashboard.pipeline.dashboard_name}"
}

output "alarm_names" {
  value = concat(
    [aws_cloudwatch_metric_alarm.pipeline_failed.alarm_name],
    [for a in aws_cloudwatch_metric_alarm.lambda_errors : a.alarm_name],
    aws_cloudwatch_metric_alarm.pipeline_not_running[*].alarm_name,
  )
}