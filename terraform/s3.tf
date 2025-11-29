# S3 Bucket for Dagster Weather ETL data lake
resource "aws_s3_bucket" "dagster_weather_etl" {
  bucket = "dagster-weather-etl"

  tags = {
    Name        = "dagster-weather-etl"
    Environment = "production"
    ManagedBy   = "terraform"
    Project     = "weather-etl"
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "dagster_weather_etl" {
  bucket = aws_s3_bucket.dagster_weather_etl.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
