variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "kyivnotkiev-research"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "billing_account_id" {
  description = "GCP billing account ID"
  type        = string
}
