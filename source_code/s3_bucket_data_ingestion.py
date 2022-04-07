# Function to Ingest Source Data Files into AWS S3 Bucket

import os
import boto3

def s3_bucket_data_ingest(access_key_id, secret_access_key, bucket_name, source_data_files_path):
    # Creating an AWS S3 Instance
    client = boto3.client('s3',
                          aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key)

    #response = client.upload_file(r'/Users/vidhyalakshmiparthasarathy/.CMVolumes/Google-Drive-pbvidhya/~~~VP_Data_Science/Big Data Engineering/BDE_Projects/ETL_Data_Pipeline_Python_AWS_CLI_S3_Glue_Athena_QuickSight/data/trade_transactions.csv', 'vp-stock-trades-bucket', 'trade_transactions.csv')
    #return response

    # Ingested File Count
    success_ingest_file_counter = 0
    failed_ingest_file_counter = 0

    # Reading the Source Files from the Directory
    if len(os.listdir(source_data_files_path)) != 0:
        for file in os.listdir(source_data_files_path):
            if ('.csv' in file) or ('.txt' in file):
                source_file_path = source_data_files_path + str(file)
                upload_file_bucket = bucket_name
                if file.endswith('.csv'):
                    #upload_file_path = 'financial_stocks_data_csv/' + str(file)
                    upload_file_path = os.path.join('financial_stocks_data_csv', str(file))
                elif file.endswith('.txt'):
                    # upload_file_path = 'financial_stocks_data_txt/' + str(file)
                    upload_file_path = os.path.join('financial_stocks_data_txt', str(file))
                try:
                    # Ingesting the Source Files into AWS S3 Bucket
                    client.upload_file(source_file_path, upload_file_bucket, upload_file_path)
                    success_ingest_file_counter += 1
                    #print("Data Ingestion to AWS S3 Bucket is Successful...")
                except:
                    print("Failed to Ingest Source Data File into AWS S3 Bucket: {}".format(file))
            else:
                print("Unrecognised Source File Format: {}".format(file))
                failed_ingest_file_counter += 1
        print("Total Source Files Ingested Successfully into AWS S3: {}".format(success_ingest_file_counter))
        print("Total Unrecognised Source Files Not Ingested into AWS S3: {}".format(failed_ingest_file_counter))
    else:
        print("Source Directory is Empty...")

