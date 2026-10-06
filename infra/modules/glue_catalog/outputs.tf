output "database_names" {
  description = "Map of layer => Glue database name"
  value       = { for layer, d in aws_glue_catalog_database.this : layer => d.name }
}
