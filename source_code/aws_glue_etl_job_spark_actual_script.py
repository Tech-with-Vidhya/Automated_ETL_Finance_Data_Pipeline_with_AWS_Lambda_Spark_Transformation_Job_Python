import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

## @type: ApplyMapping
## @args: [mapping = [("symbol", "string", "symbol", "string"), ("owner", "string", "owner", "string"), ("relationship", "string", "relationship", "string"), ("date", "string", "date", "string"), ("cost", "double", "cost", "double"), ("# shares", "long", "# shares", "long"), ("value($)", "string", "value($)", "string"), ("total shares", "long", "total shares", "long"), ("filing", "string", "filing", "string"), ("type", "string", "type", "string"), ("currentprice", "string", "currentprice", "string"), ("movingaverage", "string", "movingaverage", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("symbol", "string", "symbol", "string"), ("owner", "string", "owner", "string"), ("relationship", "string", "relationship", "string"), ("date", "string", "date", "string"), ("cost", "double", "cost", "double"), ("# shares", "long", "# shares", "long"), ("value($)", "string", "value($)", "string"), ("total shares", "long", "total shares", "long"), ("filing", "string", "filing", "string"), ("type", "string", "type", "string"), ("currentprice", "string", "currentprice", "string"), ("movingaverage", "string", "movingaverage", "string")], transformation_ctx = "applymapping1")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://vp-stock-trades-transformed-bucket"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://vp-stock-trades-transformed-bucket"}, format = "csv", transformation_ctx = "datasink2")

job.commit()