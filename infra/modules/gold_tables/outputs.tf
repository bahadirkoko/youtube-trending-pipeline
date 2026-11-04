output "table_names" {
  value = [for t in aws_glue_catalog_table.gold : t.name]
}
