# Function to Create a Bucket in AWS S3

# Importing Python's AWS SDK
import boto3

def s3_create_bucket(access_key_id, secret_access_key, bucket_name, bucket_location):
    # Creating an AWS S3 Client Instance
    client = boto3.client('s3',
                           aws_access_key_id=access_key_id,
                           aws_secret_access_key=secret_access_key)

    # Verifying the Existing List of Buckets in AWS S3
    #s3_bucket_list = client.list_buckets()
    #return s3_bucket_list


    # Create a Bucket in AWS S3
    try:
        response = client.create_bucket(
                                        ACL='private',
                                        Bucket=bucket_name,
                                        CreateBucketConfiguration={
                                            'LocationConstraint': bucket_location
                                        }
        )
        return response
    except:
        print("Failed to Create a New Bucket in AWS S3...")


