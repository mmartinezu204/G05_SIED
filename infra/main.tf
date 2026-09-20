module "catalog" {
  source       = "./modules/catalog"
  catalog_name = var.catalog_name
  comment      = "Creado por Terraform - Taller 1 Sistemas intensivos en datos"
  schemas      = var.schemas
  volumes      = var.volumes
}

# Antipatrón asumido: Terraform subiendo datos (no solo infra) al volumen.
# Deshabilitado: el provider crashea (plugin6 PlanResourceChange) leyendo
# archivos de ~1GB para hashear/subir. La subida de datos ahora se hace
# con `databricks fs cp` en setup.sh, fuera de Terraform.
# resource "databricks_file" "seed_files" {
#   for_each = local.file_names
#
#   source = each.value.path
#   path   = "${module.catalog.volume_paths[each.value.volume]}/${basename(each.value.path)}"
# }