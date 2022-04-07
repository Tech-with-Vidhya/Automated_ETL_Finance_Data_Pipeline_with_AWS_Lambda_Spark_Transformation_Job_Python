# Stock Trade Transactions Data - ETL Data Pipeline using Python, AWS (CLI, S3, Glue, Athena, QuickSight)
# Main Program

from security import access_key_id, secret_access_key
from config import bucket_name, bucket_location, s3_bucket_metadata_file_path, source_data_files_path
from s3_bucket_creation import s3_create_bucket
from s3_bucket_metadata_retrieval import s3_response_metadata
from s3_bucket_data_ingestion import s3_bucket_data_ingest

# Creating a Bucket in AWS S3
s3_response = s3_create_bucket(access_key_id, secret_access_key, bucket_name, bucket_location)

# Writing the S3 Bucket Metadata Details to a Text File
s3_response_metadata(s3_response, s3_bucket_metadata_file_path)

# Ingesting Source Data Files into AWS S3 Bucket
s3_bucket_data_ingest(access_key_id, secret_access_key, bucket_name, source_data_files_path)
