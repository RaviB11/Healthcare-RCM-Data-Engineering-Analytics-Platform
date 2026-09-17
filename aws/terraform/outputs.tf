output "lake_bucket" {
  value       = aws_s3_bucket.lake.bucket
  description = "Data lake bucket name"
}

output "glue_database" {
  value       = aws_glue_catalog_database.rcm.name
  description = "Glue Data Catalog database"
}

output "state_machine_arn" {
  value       = aws_sfn_state_machine.pipeline.arn
  description = "Pipeline state machine ARN"
}

output "kms_key_arn" {
  value       = aws_kms_key.lake.arn
  description = "CMK protecting the lake"
}

output "storage_root" {
  value       = "s3a://${aws_s3_bucket.lake.bucket}"
  description = "Value to set as storage_root in pipeline_config.yaml"
}
