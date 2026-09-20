variable "databricks_host" {
  description = "Host del workspace de Databricks (sin https://). Ver TF_VAR_databricks_host en setup.sh."
  type        = string
}

variable "databricks_token" {
  description = "PAT de Databricks. Ver TF_VAR_databricks_token en setup.sh."
  type        = string
  sensitive   = true
}

variable "catalog_name" {
  description = "Nombre del catálogo (Unity Catalog) a crear."
  type        = string
  default     = "oceanwatch"
}

variable "schemas" {
  description = "Mapa schema_name => comment. Se crea un schema por cada entrada dentro del catálogo."
  type        = map(string)
  default = {
    bronze = "Datos crudos, tal cual llegan de la fuente, sin transformar."
    silver = "Datos limpios y validados, listos para cruces y análisis."
    gold   = "Datos modelados y agregados, listos para consumo de negocio."
  }
}

variable "volumes" {
  description = "Mapa volume_name => { schema_name, comment }. Se crea un volumen managed en el schema indicado (debe existir en var.schemas)."
  type = map(object({
    schema_name = string
    comment     = string
  }))
  default = {
    landing = {
      schema_name = "bronze"
      comment     = "Volumen para datos crudos"
    }
  }
}

variable "files" {
  description = "Mapa file_name => { volume, path }. Sube cada archivo local al volumen indicado (debe existir en var.volumes)."
  type = object({
    volume = string
    path   = string
  })
  default = {
    volume = "landing"
    path   = "../datos/AIS_2023_06_$().csv"
  }
}
