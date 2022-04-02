# Automated_ETL_Finance_Data_Pipeline_with_AWS_Lambda_Spark_Transformation_Job_Python

This project covers the implementation of building an automated ETL data pipeline using Python and AWS Services with Spark transformation job for financial stocks trade transactions.  

The ETL Data Pipeline is automated using AWS Lambda Function with a Trigger defined. Whenever a new file is ingested into the AWS S3 Bucket; then the AWS Lambda Function gets triggered and will implement the further action to execute the AWS Glue Crawler ETL Spark Transformation Job.  

The Spark Transformation Job implemented using Python PySpark transforms the trade transactions data stored in the AWS S3 Bucket; further to filter a sub-set of trade transactions for which the total number of shares transacted are less than or equal to 100.  

Tools & Technologies: 
1. Python
2. Boto3 SDK
3. PySpark
4. AWS CLI
5. AWS Virtual Private Cloud (VPC)
6. AWS VPC Endpoint
7. AWS S3
8. AWS Glue
9. AWS Glue Crawler
10. AWS Glue Jobs
11. AWS Athena
12. AWS Lambda (with Trigger)
13. Spark 
