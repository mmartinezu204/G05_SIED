# output "catalog_id" {
#   value = databricks_catalog.catalog.id
# }

output "schema_ids" {
  value = { for k, s in databricks_schema.layers : k => s.id }
}

output "volume_ids" {
  value = { for k, v in databricks_volume.volumes : k => v.id }
}

output "volume_paths" {
  value = { for k, v in databricks_volume.volumes : k => v.volume_path }
}
