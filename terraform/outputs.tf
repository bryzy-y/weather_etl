# S3 Bucket outputs
output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.dagster_weather_etl.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.dagster_weather_etl.arn
}

output "s3_bucket_region" {
  description = "Region of the S3 bucket"
  value       = aws_s3_bucket.dagster_weather_etl.region
}

# IAM User outputs
output "dagster_service_user_name" {
  description = "IAM user name for Dagster service"
  value       = aws_iam_user.dagster_service.name
}

output "dagster_service_user_arn" {
  description = "ARN of the Dagster service user"
  value       = aws_iam_user.dagster_service.arn
}

output "dagster_service_access_key_id" {
  description = "AWS Access Key ID for Dagster service user"
  value       = aws_iam_access_key.dagster_service.id
  sensitive   = true
}

output "dagster_service_secret_access_key" {
  description = "AWS Secret Access Key for Dagster service user"
  value       = aws_iam_access_key.dagster_service.secret
  sensitive   = true
}
