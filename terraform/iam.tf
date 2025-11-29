# IAM User for Dagster Weather ETL service
resource "aws_iam_user" "dagster_service" {
  name = "dagster-weather-etl-service"
  path = "/service-accounts/"

  tags = {
    Name        = "dagster-weather-etl-service"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# Access keys for the service user
resource "aws_iam_access_key" "dagster_service" {
  user = aws_iam_user.dagster_service.name
}

# IAM Policy for S3 bucket access
resource "aws_iam_policy" "dagster_s3_access" {
  name        = "dagster-weather-etl-s3-access"
  path        = "/service-accounts/"
  description = "Full access to dagster-weather-etl S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*"
        ]
        Resource = [
          "arn:aws:s3:::dagster-weather-etl",
          "arn:aws:s3:::dagster-weather-etl/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListAllMyBuckets",
          "s3:GetBucketLocation"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach policy to user
resource "aws_iam_user_policy_attachment" "dagster_service_s3" {
  user       = aws_iam_user.dagster_service.name
  policy_arn = aws_iam_policy.dagster_s3_access.arn
}
