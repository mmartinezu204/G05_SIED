# Catalogo creado manualmente vía UI (Default Storage no se puede setear por Terraform en cuenta free).
# resource "databricks_catalog" "catalog" {
#   name    = var.catalog_name
#   comment = var.comment
# }

resource "databricks_schema" "layers" {
  for_each = var.schemas

  catalog_name = var.catalog_name
  name         = each.key
  comment      = each.value
}

resource "databricks_volume" "volumes" {
  for_each = var.volumes

  name         = each.key
  catalog_name = var.catalog_name
  schema_name  = databricks_schema.layers[each.value.schema_name].name
  volume_type  = "MANAGED"
  comment      = each.value.comment

  depends_on = [databricks_schema.layers]
}
