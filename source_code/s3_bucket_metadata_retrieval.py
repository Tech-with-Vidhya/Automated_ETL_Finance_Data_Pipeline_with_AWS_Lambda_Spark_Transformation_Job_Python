# Function to Retrieve AWS Metadata Details of the S3 Bucket Created

def s3_response_metadata(s3_response, s3_bucket_metadata_file_path):
    file = open(s3_bucket_metadata_file_path, 'w')
    for key, value in s3_response['ResponseMetadata'].items():
        # file.write('%s: %s\n' % (key, value))
        file.write("{}: {}\n".format(key, value))
    # file.write('%s: %s\n' % ("Location", s3_response['Location']))
    file.write("Location: {}\n".format(s3_response['Location']))
    file.close()

