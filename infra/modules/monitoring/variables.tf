variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "region" {
  type = string
}

variable "alerts_topic_arn" {
  description = "SNS topic that alarm notifications are sent to"
  type        = string
}

variable "state_machine_arn" {
  description = "Pipeline state machine to watch"
  type        = string
}

variable "lambda_function_names" {
  description = "Label => function name. One error alarm is created per function."
  type        = map(string)
}

variable "schedule_enabled" {
  description = "Only create the 'schedule did not fire' alarm when the schedule is on"
  type        = bool
}

variable "missed_days_before_alarm" {
  description = "Alarm when this many consecutive days pass with no execution started"
  type        = number
  default     = 2
}
