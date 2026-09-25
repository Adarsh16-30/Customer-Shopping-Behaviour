output "glue_database_name" {
  value = aws_glue_catalog_database.project.name
}

output "glue_crawler_name" {
  value = aws_glue_crawler.raw.name
}

output "glue_job_name" {
  value = aws_glue_job.transform.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.project.name
}

output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "transformed_bucket" {
  value = aws_s3_bucket.transformed.bucket
}
