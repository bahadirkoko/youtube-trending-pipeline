output "categories_table" {
  value = aws_glue_catalog_table.categories.name
}

output "trending_videos_table" {
  value = aws_glue_catalog_table.trending_videos.name
}
