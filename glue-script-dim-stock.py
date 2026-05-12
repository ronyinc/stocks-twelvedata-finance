import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_path = "s3://rony-finance-stocks-lake-dev-2026/raw/twelvedata/stocks_dim/"
curated_path = "s3://rony-finance-stocks-lake-dev-2026/curated/stocks_dim/"

# Read raw CSV files

df = (
    spark.read
    .option("header","true")
    .csv(raw_path)
)

df.printSchema()
df.show(5, truncate=False)

df_typed = (
    df
    .withColumn("extracted_at_utc", to_timestamp(col("extracted_at_utc")))
)

w = Window.partitionBy("symbol").orderBy(desc("extracted_at_utc"))

df_clean = (
    df_typed
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# write the o/p into a parquet format.


(
    df_clean.write
    .mode("overwrite")
    .partitionBy("symbol")
    .parquet(curated_path)
)

job.commit()