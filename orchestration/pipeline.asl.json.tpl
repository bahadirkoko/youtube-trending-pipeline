{
  "Comment": "YouTube trending pipeline: ingest, build Silver (categories and trending videos in parallel), data quality gate, build Gold, notify.",
  "StartAt": "Ingest",
  "TimeoutSeconds": 5400,
  "States": {
    "Ingest": {
      "Type": "Task",
      "Comment": "Fetch trending videos from the YouTube API into Bronze.",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${ingestion_function_name}",
        "Payload": {}
      },
      "ResultSelector": {
        "status.$": "$.Payload.status",
        "succeeded.$": "$.Payload.succeeded",
        "failed.$": "$.Payload.failed"
      },
      "ResultPath": "$.ingestion",
      "TimeoutSeconds": 300,
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "NotifyFailure"
        }
      ],
      "Next": "CountIngestedRegions"
    },

    "CountIngestedRegions": {
      "Type": "Pass",
      "Parameters": {
        "count.$": "States.ArrayLength($.ingestion.succeeded)"
      },
      "ResultPath": "$.ingested_regions",
      "Next": "AnyRegionIngested"
    },

    "AnyRegionIngested": {
      "Type": "Choice",
      "Comment": "Some regions may fail (partial success is fine), but zero is a failure.",
      "Choices": [
        {
          "Variable": "$.ingested_regions.count",
          "NumericGreaterThan": 0,
          "Next": "BuildSilver"
        }
      ],
      "Default": "NoRegionIngested"
    },

    "NoRegionIngested": {
      "Type": "Pass",
      "Parameters": {
        "Error": "NoRegionIngested",
        "Cause.$": "States.Format('The ingestion Lambda fetched no region. Failures: {}', States.JsonToString($.ingestion.failed))"
      },
      "ResultPath": "$.error",
      "Next": "NotifyFailure"
    },

    "BuildSilver": {
      "Type": "Parallel",
      "Comment": "Category reference data and the trending videos table do not depend on each other.",
      "Branches": [
        {
          "StartAt": "ConvertCategories",
          "States": {
            "ConvertCategories": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "${json_to_parquet_function_name}",
                "Payload": {}
              },
              "ResultSelector": {
                "result.$": "$.Payload"
              },
              "TimeoutSeconds": 300,
              "Retry": [
                {
                  "ErrorEquals": [
                    "Lambda.ServiceException",
                    "Lambda.AWSLambdaException",
                    "Lambda.SdkClientException",
                    "Lambda.TooManyRequestsException"
                  ],
                  "IntervalSeconds": 2,
                  "MaxAttempts": 3,
                  "BackoffRate": 2.0
                }
              ],
              "End": true
            }
          }
        },
        {
          "StartAt": "BronzeToSilver",
          "States": {
            "BronzeToSilver": {
              "Type": "Task",
              "Comment": "Waits for the Glue job to finish (.sync). A failed run fails this state.",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "${bronze_to_silver_job_name}"
              },
              "ResultSelector": {
                "state.$": "$.JobRunState",
                "run_id.$": "$.Id"
              },
              "TimeoutSeconds": 2400,
              "Retry": [
                {
                  "ErrorEquals": ["Glue.ConcurrentRunsExceededException"],
                  "IntervalSeconds": 60,
                  "MaxAttempts": 3,
                  "BackoffRate": 2.0
                }
              ],
              "End": true
            }
          }
        }
      ],
      "ResultSelector": {
        "categories.$": "$[0].result",
        "silver_job.$": "$[1]"
      },
      "ResultPath": "$.silver",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "NotifyFailure"
        }
      ],
      "Next": "RunDataQuality"
    },

    "RunDataQuality": {
      "Type": "Task",
      "Comment": "The gate emails its own details when it fails, then returns passed=false.",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${data_quality_function_name}",
        "Payload": {}
      },
      "ResultSelector": {
        "passed.$": "$.Payload.passed",
        "rows_checked.$": "$.Payload.rows_checked",
        "failed_checks.$": "$.Payload.failed_checks",
        "alert_sent.$": "$.Payload.alert_sent"
      },
      "ResultPath": "$.dq",
      "TimeoutSeconds": 300,
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "NotifyFailure"
        }
      ],
      "Next": "DataQualityGate"
    },

    "DataQualityGate": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.dq.passed",
          "BooleanEquals": true,
          "Next": "BuildGold"
        }
      ],
      "Default": "DataQualityFailed"
    },

    "DataQualityFailed": {
      "Type": "Fail",
      "Error": "DataQualityFailed",
      "Cause": "The data quality gate failed, so Gold was not rebuilt. The gate emailed the failed checks; the same details are in this execution under RunDataQuality."
    },

    "BuildGold": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${silver_to_gold_job_name}"
      },
      "ResultSelector": {
        "state.$": "$.JobRunState",
        "run_id.$": "$.Id"
      },
      "ResultPath": "$.gold",
      "TimeoutSeconds": 2400,
      "Retry": [
        {
          "ErrorEquals": ["Glue.ConcurrentRunsExceededException"],
          "IntervalSeconds": 60,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "NotifyFailure"
        }
      ],
      "Next": "NotifySuccess"
    },

    "NotifySuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${alerts_topic_arn}",
        "Subject": "[${environment}] YouTube pipeline: succeeded",
        "Message.$": "States.Format('The pipeline finished successfully. Regions ingested: {}. Regions that failed at ingestion: {}. Rows checked by the quality gate: {}. Execution: https://console.aws.amazon.com/states/home?region=${region}#/v2/executions/details/{}', States.JsonToString($.ingested_regions.count), States.JsonToString($.ingestion.failed), States.JsonToString($.dq.rows_checked), $$.Execution.Id)"
      },
      "Retry": [
        {
          "ErrorEquals": ["States.ALL"],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "End": true
    },

    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${alerts_topic_arn}",
        "Subject": "[${environment}] YouTube pipeline: FAILED",
        "Message.$": "States.Format('The pipeline failed. Error: {}. Cause: {}. Execution: https://console.aws.amazon.com/states/home?region=${region}#/v2/executions/details/{}', $.error.Error, $.error.Cause, $$.Execution.Id)"
      },
      "Retry": [
        {
          "ErrorEquals": ["States.ALL"],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.notify_error",
          "Next": "PipelineFailed"
        }
      ],
      "Next": "PipelineFailed"
    },

    "PipelineFailed": {
      "Type": "Fail",
      "Error": "PipelineFailed",
      "Cause": "A pipeline step failed. See the failure email and this execution's history for the step and error."
    }
  }
}
