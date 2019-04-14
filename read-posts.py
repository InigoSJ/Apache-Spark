import pyspark

sc = pyspark.SparkContext()
sc.setLogLevel("DEBUG")
from pyspark import sql
spark = sql.SparkSession.builder.getOrCreate()
df = spark.read.format('com.databricks.spark.xml').options(rowTag='row').load('gs://<your-bucket>/Posts.xml')
rows = df.count()
print(rows)