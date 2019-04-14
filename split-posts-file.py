from pyspark import sql
from pyspark.sql.functions import col


spark = sql.SparkSession.builder.getOrCreate()
posts_df = spark.read.format('com.databricks.spark.xml').options(rowTag='row').load('gs://<your-bucket>/Posts.xml')

print('Col names and types ',posts_df.dtypes)

posts_df = posts_df.where(col("_Id").between(0,1000))

posts_df.write.json('gs://<your-bucket>/Posts_json',mode='error')
posts_df.write.parquet('gs://<your-bucket>/Posts_parquet',mode='error')
posts_df.write.orc('gs://<your-bucket>/Posts_orc',mode='error')