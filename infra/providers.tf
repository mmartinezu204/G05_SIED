terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

# Auth vía PAT: var.databricks_host / var.databricks_token (pobladas desde
# TF_VAR_databricks_host / TF_VAR_databricks_token, ver setup.sh)
provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
