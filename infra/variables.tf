variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Prefix applied to all resource names"
  type        = string
  default     = "customer-shopping-behaviour"
}

variable "raw_bucket_name" {
  description = "S3 bucket holding the raw Kaggle clickstream export (must be globally unique)"
  type        = string
}

variable "transformed_bucket_name" {
  description = "S3 bucket holding the Glue job's cleaned Parquet output (must be globally unique)"
  type        = string
}

variable "glue_script_s3_uri" {
  description = "s3:// URI of the uploaded glue_jobs/CleaningAndTransforming.py"
  type        = string
}

variable "alarm_notification_email" {
  description = "Email subscribed to the Glue-job-failure SNS topic"
  type        = string
}

variable "glue_worker_type" {
  description = "Glue worker type. G.1X comfortably covers the 15GB dataset this project uses; bump to G.2X only if the input grows past ~50GB."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers. 5 x G.1X gives ~5 DPUs of parallelism, enough headroom for the Oct+Nov (15GB) slice without over-provisioning."
  type        = number
  default     = 5
}
