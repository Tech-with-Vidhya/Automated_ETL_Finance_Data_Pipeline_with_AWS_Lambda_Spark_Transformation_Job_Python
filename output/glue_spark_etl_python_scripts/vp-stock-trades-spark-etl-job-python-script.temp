import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## New Library Imported by Vidhyalakshmi Parthasarathy for AWS Glue ETL Spark Transformation Script
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "vp-stock-trades-bucket-database", table_name = "vp_stock_trades_bucketfinancial_stocks_data_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "vp-stock-trades-bucket-database", table_name = "vp_stock_trades_bucketfinancial_stocks_data_csv", transformation_ctx = "datasource0")

## Vidhyalakshmi Parthasarathy's Custom Transformation Logic added using Python Pandas DataFrame
## Start of the Custom Logic

## Converting the GlueContext Dynamic Frame into Python Pandas DataFrame
trade_transactions_df = datasource0.toDF()

## Transforming the Actual Trade Transactions Data to Filter a Sub-set of Data
## Filter Condition to Extract the Data where the Total Number of Shares are less than or equal to 100
trade_transactions_transformed_df = trade_transactions_df.filter(trade_transactions_df['# Shares'] <= 100)

## Converting the Python Pandas Transformed DataFrame back into GlueContext Dynamic Frame
dynamic_transformed_frame = DynamicFrame.fromDF(trade_transactions_transformed_df, glueContext, "dynamic_df")

## End of the Custom Logic

## @type: ApplyMapping
## @args: [mapping = [("symbol", "string", "symbol", "string"), ("owner", "string", "owner", "string"), ("relationship", "string", "relationship", "string"), ("date", "string", "date", "string"), ("cost", "double", "cost", "double"), ("# shares", "long", "# shares", "long"), ("value($)", "string", "value($)", "string"), ("total shares", "long", "total shares", "long"), ("filing", "string", "filing", "string"), ("type", "string", "type", "string"), ("currentprice", "string", "currentprice", "string"), ("movingaverage", "string", "movingaverage", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = dynamic_transformed_frame, mappings = [("symbol", "string", "symbol", "string"), ("owner", "string", "owner", "string"), ("relationship", "string", "relationship", "string"), ("date", "string", "date", "string"), ("cost", "double", "cost", "double"), ("# shares", "long", "# shares", "long"), ("value($)", "string", "value($)", "string"), ("total shares", "long", "total shares", "long"), ("filing", "string", "filing", "string"), ("type", "string", "type", "string"), ("currentprice", "string", "currentprice", "string"), ("movingaverage", "string", "movingaverage", "string")], transformation_ctx = "applymapping1")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://vp-stock-trades-transformed-bucket"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://vp-stock-trades-transformed-bucket"}, format = "csv", transformation_ctx = "datasink2")

job.commit()