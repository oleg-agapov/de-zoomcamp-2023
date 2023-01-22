locals {
  data_lake_bucket = "de-zoomcamp-2023"
}

variable "gcp_credentials_path" {
  description = "Path to GCP credentials file"
  type        = string
  default     = "/Users/oleg/Downloads/booming-landing-375221-19efec792ab3.json"
}

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = "booming-landing-375221"
}

variable "gcp_region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  type        = string
  default     = "europe-west6"
}

variable "bigquery_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "ny_taxi_trips"
}
