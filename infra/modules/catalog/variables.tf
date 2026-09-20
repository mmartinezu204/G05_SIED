variable "catalog_name" {
  description = "Nombre del catálogo (Unity Catalog) a crear."
  type        = string
}

variable "comment" {
  description = "Comentario/descripción para el catálogo."
  type        = string
  default     = ""
}

variable "schemas" {
  description = "Mapa schema_name => comment. Se crea un schema por cada entrada dentro del catálogo."
  type        = map(string)
  default     = {}
}

variable "volumes" {
  description = "Mapa volume_name => { schema_name, comment }. Se crea un volumen managed en el schema indicado (debe existir en var.schemas)."
  type = map(object({
    schema_name = string
    comment     = string
  }))
  default = {}
}
