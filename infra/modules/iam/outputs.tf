output "role_arns" {
  description = "Map of service => IAM role ARN"
  value = {
    lambda = aws_iam_role.lambda.arn
    glue   = aws_iam_role.glue.arn
    sfn    = aws_iam_role.sfn.arn
    events = aws_iam_role.events.arn
  }
}
