from pyspark import sql
spark = sql.SparkSession.builder.getOrCreate()
posts_df = spark.read.format('com.databricks.spark.xml').options(rowTag='row').load('gs://<your-bucket>/Posts.xml')

from pyspark.sql.functions import col,lit,concat
from pyspark.sql import functions as F

print('Col names and types ',posts_df.dtypes)

posts_df = posts_df.select(col("_AnswerCount").alias("AnswerCount"), col("_Id").alias("question_id"), col("_CreationDate").alias("creation_date"), col("_Title").alias("title"), col("_Tags").alias("tags"))

posts_df = posts_df.where((col("tags").isNotNull()) & (col("title").isNotNull()))
posts_df = posts_df.withColumn("is_answered", F.when(col("AnswerCount").isNull(), False).otherwise(True))
posts_df = posts_df.withColumn("link", concat(lit("https://stackoverflow.com/questions/"),col("question_id").cast("STRING"),lit("/"), col("title").cast("STRING")))
posts_df = posts_df.drop("AnswerCount")
posts_df = posts_df.withColumn("tags", F.translate(col("tags"),"<","")).withColumn("tags",F.split(col("tags"),">"))
posts_df.show()

def contains_udf(row,value="google-cloud-dataflow"):
    contains = False
    for element in row:
            if element==value:
                    contains=True
    return contains
udf_two = F.udf(contains_udf)
posts_df = posts_df.withColumn("containsvalue", udf_two(posts_df.tags,lit("google-cloud-dataflow"))).where(col("containsvalue") == True).drop("containsvalue")
posts_df = posts_df.withColumn("tags", F.concat_ws(',',col('tags')))
posts_df.write.json("gs://<your-bucket>/Posts_filtered_onlydataflowtag.json")
