# Database names use underscores because Athena does not allow hyphens.
resource "aws_glue_catalog_database" "this" {
  for_each = toset(var.layers)

  name        = "yt_pipeline_${each.key}_${var.environment}"
  description = "YouTube trending pipeline - ${each.key} layer"
}
