import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    print("Tech-with-Vidhya vp-stock-trades AWS Lambda Execution...")
    print("Lambda Function Execution Started...")
    client = boto3.client('glue')
    client.start_job_run(
                JobName = 'vp-stock-trades-spark-etl-job',
                Arguments = {}
    )
    print("Lambda Function Execution Completed...")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }